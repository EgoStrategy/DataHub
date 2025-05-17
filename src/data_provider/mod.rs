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
        // 检查本地文件是否存在，如果不存在则创建空文件
        let data_dir = "data";
        let arrow_file = format!("{}/stock.arrow", data_dir);
        
        if !std::path::Path::new(&arrow_file).exists() {
            println!("Local stock.arrow file not found. Creating empty file.");
            std::fs::create_dir_all(data_dir)?;
            
            // 创建空的Arrow文件
            let empty_data: Vec<StockData> = Vec::new();
            arrow_utils::save_stock_data_to_arrow(&empty_data, &arrow_file)?;
        }
        
        // 从文件加载数据
        let data = arrow_utils::read_stock_data_from_arrow(&arrow_file)?;
        
        // 创建索引
        let mut provider = Self { 
            data, 
            symbol_index: HashMap::new(),
            exchange_index: HashMap::new(),
        };
        
        provider.rebuild_indices();
        
        // 异步检查更新（不阻塞初始化）
        let arrow_file_clone = arrow_file.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::check_for_updates(&arrow_file_clone).await {
                eprintln!("Failed to check for updates: {}", e);
            }
        });
        
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
        let mut latest_date = None;
        
        for stock in &self.data {
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
    
    // 检查远程文件是否有更新
    async fn check_for_updates(local_path: &str) -> Result<()> {
        let remote_url = "https://egostrategy.github.io/DataHub/data/stock.arrow";
        
        // 获取本地文件信息
        let local_metadata = match fs::metadata(local_path) {
            Ok(meta) => meta,
            Err(_) => {
                // 本地文件不存在，直接下载
                return Self::download_file(remote_url, local_path).await;
            }
        };
        
        // 获取本地文件修改时间
        let local_modified = local_metadata.modified()
            .unwrap_or_else(|_| SystemTime::now())
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        // 发送HEAD请求获取远程文件信息
        let client = reqwest::Client::new();
        let resp = match client.head(remote_url).send().await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Failed to send HEAD request: {}", e);
                return Ok(());  // 忽略错误，使用本地文件
            }
        };
        
        if !resp.status().is_success() {
            eprintln!("Remote file check failed: HTTP status {}", resp.status());
            return Ok(());  // 忽略错误，使用本地文件
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
            println!("Remote stock.arrow file size differs. Downloading updates...");
            return Self::download_file(remote_url, local_path).await;
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
                        println!("Remote stock.arrow file is newer. Downloading updates...");
                        return Self::download_file(remote_url, local_path).await;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    // 下载文件
    async fn download_file(url: &str, path: &str) -> Result<()> {
        println!("Downloading stock data from: {}", url);
        
        // 下载文件
        let resp = reqwest::get(url).await?;
        if !resp.status().is_success() {
            return Err(DataHubError::DataError(format!(
                "Failed to download data file: HTTP status {}", resp.status()
            )));
        }
        
        let bytes = resp.bytes().await?;
        
        // 先写入临时文件
        let temp_path = format!("{}.tmp", path);
        std::fs::write(&temp_path, &bytes)?;
        
        // 然后重命名，确保原子操作
        std::fs::rename(&temp_path, path)?;
        
        println!("Successfully downloaded stock data file");
        Ok(())
    }
}
