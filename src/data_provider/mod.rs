use crate::models::stock::StockData;
use crate::errors::{Result};
use crate::util::arrow_utils;
use std::collections::HashMap;
use std::path::Path;
use std::fs;

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
        // 从内嵌的Arrow文件加载数据
        let data = Self::load_embedded_data()?;
        
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
    
    /// 从内嵌的Arrow文件加载数据
    fn load_embedded_data() -> Result<Vec<StockData>> {
        // 检查文件是否存在
        let file_path = concat!(env!("CARGO_MANIFEST_DIR"), "/data/stock.arrow");
        if !std::path::Path::new(file_path).exists() {
            return Ok(Vec::new());
        }
        
        // 这里我们将Arrow文件作为二进制数据嵌入到项目中
        let arrow_data = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/data/stock.arrow"));
        
        // 从内存中读取Arrow数据
        arrow_utils::read_stock_data_from_memory(arrow_data)
    }
    
    /// 获取最新日期
    pub fn get_latest_date(&self) -> Option<i32> {
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
}
