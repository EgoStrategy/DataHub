use chrono::prelude::*;
use chrono::{Local};
use log::{error, info};

use crate::models::stock::StockData;
use crate::errors::{Result, DataHubError};
use crate::util::arrow_utils;
use std::collections::HashMap;
use std::path::Path;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

/// 股票数据提供者，用于访问嵌入的股票数据
pub struct StockDataProvider {
    data: Vec<StockData>,
    // 索引用于快速查找
    symbol_index: HashMap<String, usize>,
    exchange_index: HashMap<String, Vec<usize>>,
}

impl StockDataProvider {
    /// 创建新的数据提供者实例
    pub fn new() -> Result<Self> {
        // 使用包内docs/data/stock.arrow路径文件
        let data_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/docs/data");
        let package_arrow_file = concat!(env!("CARGO_MANIFEST_DIR"), "/docs/data/stock.arrow");
        
        // 确保数据目录存在
        if !std::path::Path::new(data_dir).exists() {
            std::fs::create_dir_all(data_dir)?;
        }
        
        // 检查本地文件是否存在，如果不存在则创建空文件
        if !std::path::Path::new(&package_arrow_file).exists() {
            info!("Local stock.arrow file not found. Creating empty file.");
            
            // 创建空的Arrow文件
            let empty_data: Vec<StockData> = Vec::new();
            arrow_utils::save_stock_data_to_arrow(&empty_data, &package_arrow_file)?;
        }
        
        // 从文件加载数据（更新前）
        let data_before_update = arrow_utils::read_stock_data_from_arrow(&package_arrow_file)?;
        let latest_date_before = Self::get_latest_date_from_data(&data_before_update);
        if let Some(date) = latest_date_before {
            info!("更新前最新交易日期: {}", date);
            let tz_offset: FixedOffset = "+08:00".parse()?;
            let dt_now = Local::now().with_timezone(&tz_offset);
            let now_int = dt_now.format("%Y%m%d").to_string().parse::<i32>()?;
            
            if date < now_int {
                // 同步检查更新
                // 尝试多个国内镜像站点，按优先级排序
                let mirror_sites = [
                    "raw.githubusercontent.com",
                    "raw.bgithub.xyz",
                    "raw.staticdn.net"
                ];
                
                let mut success = false;
                for mirror in mirror_sites {
                    if let Ok(_) = Self::check_for_updates_sync(package_arrow_file, &format!("https://{}/EgoStrategy/DataHub/main/docs/data/stock.arrow", mirror)) {
                        success = true;
                        break;
                    }
                }
                
                if !success {
                    error!("Failed to check for updates from all mirror sites");
                }
            }
        }
            
        // 从文件加载数据（更新后）
        let data = arrow_utils::read_stock_data_from_arrow(&package_arrow_file)?;
        let latest_date_after = Self::get_latest_date_from_data(&data);
        if let Some(date) = latest_date_after {
            info!("更新后最新交易日期: {}", date);
        } else {
            info!("更新后无交易数据");
        }
        
        // 创建索引
        let mut provider = Self { 
            data, 
            symbol_index: HashMap::new(),
            exchange_index: HashMap::new(),
        };
        
        provider.rebuild_indices();
        
        Ok(provider)
    }
    
    /// 使用提供的数据创建新的数据提供者实例
    pub fn new_with_data(data: Vec<StockData>) -> Result<Self> {
        let mut provider = Self {
            data,
            symbol_index: HashMap::new(),
            exchange_index: HashMap::new(),
        };
        
        provider.rebuild_indices();
        
        Ok(provider)
    }
    
    /// 从文件加载数据
    pub fn load_from_file(path: &str) -> Result<Self> {
        let data = if Path::new(path).exists() {
            arrow_utils::read_stock_data_from_arrow(path)?
        } else {
            Vec::new()
        };
        
        let mut provider = Self {
            data,
            symbol_index: HashMap::new(),
            exchange_index: HashMap::new(),
        };
        
        provider.rebuild_indices();
        
        Ok(provider)
    }
    
    /// 保存数据到文件
    pub fn save_to_file(&self, path: &str) -> Result<()> {
        // 确保目录存在
        if let Some(parent) = Path::new(path).parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }
        
        arrow_utils::save_stock_data_to_arrow(&self.data, path)
    }
    
    /// 获取所有股票列表
    pub fn get_all_stocks(&self) -> &[StockData] {
        &self.data
    }
    
    /// 获取指定股票
    pub fn get_stock_by_symbol(&self, symbol: &str) -> Option<&StockData> {
        self.symbol_index.get(symbol).map(|&idx| &self.data[idx])
    }
    
    /// 获取指定交易所的股票
    pub fn get_stocks_by_exchange(&self, exchange: &str) -> Vec<&StockData> {
        self.exchange_index.get(exchange)
            .map(|indices| indices.iter().map(|&idx| &self.data[idx]).collect())
            .unwrap_or_default()
    }
    
    /// 重建索引
    fn rebuild_indices(&mut self) {
        self.symbol_index.clear();
        self.exchange_index.clear();
        
        for (i, stock) in self.data.iter().enumerate() {
            self.symbol_index.insert(stock.symbol.clone(), i);
            
            self.exchange_index
                .entry(stock.exchange.clone())
                .or_insert_with(Vec::new)
                .push(i);
        }
    }
    
    /// 获取最新日期
    pub fn get_latest_trading_date(&self) -> Option<i32> {
        Self::get_latest_date_from_data(&self.data)
    }
    
    /// 从数据中获取最新日期（辅助函数）
    fn get_latest_date_from_data(data: &[StockData]) -> Option<i32> {
        let mut latest_date = None;
        
        for stock in data {
            if let Some(daily) = stock.daily.first() {
                if let Some(current_latest) = latest_date {
                    if daily.date > current_latest {
                        latest_date = Some(daily.date);
                    }
                } else {
                    latest_date = Some(daily.date);
                }
            }
        }
        
        latest_date
    }
    
    // 同步检查远程文件是否有更新
    fn check_for_updates_sync(arrow_file: &str, from_url: &str) -> Result<()> {
        let remote_url = from_url;
        
        // 获取本地文件信息
        let local_metadata = match fs::metadata(arrow_file) {
            Ok(meta) => meta,
            Err(_) => {
                // 本地文件不存在，直接下载
                return Self::download_file_sync(remote_url, arrow_file);
            }
        };
        
        // 获取本地文件修改时间
        let local_modified = local_metadata.modified()
            .unwrap_or_else(|_| SystemTime::now())
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        // 发送HEAD请求获取远程文件信息
        let client = reqwest::blocking::Client::new();
        let resp = client.head(remote_url).send()?;
        
        if !resp.status().is_success() {
            error!("Remote file check failed: HTTP status {}", resp.status());
            return Err(DataHubError::DataError(format!("Remote file check failed: HTTP status {}", resp.status())));
        }
        
        // 获取远程文件大小
        let remote_size = if let Some(content_length) = resp.headers().get("content-length") {
            content_length.to_str().unwrap_or("0").parse::<u64>().unwrap_or(0)
        } else {
            0
        };
        
        let local_size = local_metadata.len();
        
        // 如果远程文件大小不同且不为0，下载新文件
        if remote_size != local_size && remote_size > 0 {
            info!("Remote stock.arrow file size differs. Downloading updates...");
            return Self::download_file_sync(remote_url, arrow_file);
        }
        
        // 获取远程文件修改时间
        if let Some(last_modified) = resp.headers().get("last-modified") {
            if let Ok(last_modified_str) = last_modified.to_str() {
                if let Ok(time) = httpdate::parse_http_date(last_modified_str) {
                    // 将SystemTime转换为秒
                    let remote_time = time.duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    
                    // 比较修改时间
                    if remote_time > local_modified {
                        info!("Remote stock.arrow file is newer. Downloading updates...");
                        return Self::download_file_sync(remote_url, arrow_file);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    // 同步下载文件
    fn download_file_sync(url: &str, arrow_file: &str) -> Result<()> {
        info!("Downloading stock data from: {}", url);
        
        // 下载文件
        let client = reqwest::blocking::Client::new();
        let resp = client.get(url).send()?;
        if !resp.status().is_success() {
            return Err(DataHubError::DataError(format!(
                "Failed to download data file: HTTP status {}", resp.status()
            )));
        }
        
        let bytes = resp.bytes()?;
        
        std::fs::write(&arrow_file, &bytes)?;
        
        info!("Successfully downloaded stock data file");
        Ok(())
    }
}
