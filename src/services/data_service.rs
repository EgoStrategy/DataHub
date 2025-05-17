use crate::models::stock::StockData;
use crate::scrapers::base::StockScraper;
use crate::errors::{Result, DataHubError};
use crate::config::Config;
use crate::data_provider::StockDataProvider;
use crate::util;
use chrono::NaiveDate;
use log::{info, warn};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::collections::HashMap;

/// 数据服务，处理数据的获取、合并和存储
pub struct DataService {
    config: Config,
    scrapers: Vec<Arc<dyn StockScraper + Send + Sync>>,
    data_path: PathBuf,
}

impl DataService {
    /// 创建新的数据服务实例
    pub fn new(config: Config, scrapers: Vec<Arc<dyn StockScraper + Send + Sync>>) -> Self {
        let data_path = PathBuf::from(&config.data_dir).join("stock.arrow");
        Self {
            config,
            scrapers,
            data_path,
        }
    }
    
    /// 获取数据文件路径
    pub fn data_path(&self) -> &Path {
        &self.data_path
    }
    
    /// 加载数据提供者
    pub async fn load_provider(&self) -> Result<StockDataProvider> {
        if self.data_path.exists() {
            info!("Loading existing data from {}", self.data_path.display());
            StockDataProvider::load_from_file(self.data_path.to_str().unwrap())
        } else {
            info!("No existing data found, creating new dataset");
            Ok(StockDataProvider::new()?)
        }
    }
    
    /// 处理单个股票
    pub async fn process_single_stock(&self, symbol: &str, date: Option<&NaiveDate>) -> Result<()> {
        let actual_date = date.cloned().unwrap_or_else(|| chrono::Local::now().naive_local().date());
        info!("Processing stock: {} for date: {}", symbol, actual_date);
        
        // 一次性获取所有股票列表数据
        let mut stock_data_map = HashMap::new();
        for scraper in &self.scrapers {
            let stock_list = scraper.fetch_stock_list(&actual_date).await?;
            for stock in stock_list {
                stock_data_map.insert(format!("{}:{}", stock.exchange, stock.symbol), stock);
            }
        }
        
        // 查找匹配的股票
        let mut found_stock = None;
        
        // 查找匹配的股票
        for (_, stock) in &stock_data_map {
            if stock.symbol == symbol {
                found_stock = Some(stock.clone());
                break;
            }
        }
        
        if let Some(stock) = found_stock {
            // 处理单个股票
            let stocks_to_process = vec![stock];
            self.process_stocks_with_data(&stocks_to_process, &stock_data_map).await?;
            info!("Successfully processed stock: {}", symbol);
            Ok(())
        } else {
            Err(DataHubError::DataError(format!(
                "Stock {} not found in any exchange for date {}", symbol, actual_date
            )))
        }
    }
    
    /// 处理指定日期的所有股票
    pub async fn process_daily_stocks(&self, date: &NaiveDate) -> Result<()> {
        info!("Processing stocks for date: {}", date);
        
        // 一次性获取所有交易所的股票列表
        let mut daily_stocks = Vec::new();
        let mut stock_data_map = HashMap::new();
        
        for scraper in &self.scrapers {
            info!("Scraping from {}", scraper.exchange_code());
            let mut stocks = scraper.fetch_stock_list(date).await?;
            
            // 调试模式：只处理前N个股票
            if self.config.debug_mode {
                let original_count = stocks.len();
                stocks.truncate(self.config.debug_stock_limit);
                info!("DEBUG MODE: Processing only {} out of {} stocks from {}", 
                      stocks.len(), original_count, scraper.exchange_code());
            }
            
            info!("Found {} stocks in {}", stocks.len(), scraper.exchange_code());
            
            // 添加到映射和列表
            for stock in &stocks {
                stock_data_map.insert(format!("{}:{}", stock.exchange, stock.symbol), stock.clone());
            }
            daily_stocks.extend(stocks);
        }
        
        if daily_stocks.is_empty() {
            warn!("No stocks found for date {}", date);
            return Ok(());
        }
        
        // 处理所有股票
        self.process_stocks_with_data(&daily_stocks, &stock_data_map).await?;
        
        info!("Successfully processed {} stocks for date: {}", daily_stocks.len(), date);
        Ok(())
    }
    
    /// 内部方法：使用已获取的数据处理股票
    async fn process_stocks_with_data(&self, stocks: &[StockData], _stock_data_map: &HashMap<String, StockData>) -> Result<()> {
        // 加载现有数据
        let provider = self.load_provider().await?;
        let mut all_stocks = provider.get_all_stocks().to_vec();
        
        // 创建一个映射，用于快速查找现有数据
        let mut existing_map = HashMap::new();
        for (i, stock) in all_stocks.iter().enumerate() {
            let key = format!("{}:{}", stock.exchange, stock.symbol);
            existing_map.insert(key, i);
        }
        
        // 处理每个股票
        let mut stocks_to_update = Vec::new();
        
        for stock in stocks {
            let symbol = &stock.symbol;
            let exchange = &stock.exchange;
            let key = format!("{}:{}", exchange, symbol);
            
            // 检查是否需要获取完整历史数据
            let need_full_history = if let Some(&idx) = existing_map.get(&key) {
                // 股票已存在，检查是否需要全量更新
                self.config.force_full_history || all_stocks[idx].daily.is_empty()
            } else {
                // 股票不存在，需要获取完整历史
                true
            };
            
            let mut updated_stock = if let Some(&idx) = existing_map.get(&key) {
                // 股票已存在，更新名称
                let mut updated = all_stocks[idx].clone();
                updated.name = stock.name.clone(); // 始终使用最新的股票名称
                updated
            } else {
                // 创建新的股票数据
                stock.clone()
            };
            
            if need_full_history {
                // 需要获取完整历史数据
                info!("Fetching full history for stock {}", symbol);
                for scraper in &self.scrapers {
                    if scraper.exchange_code() == exchange {
                        match scraper.fetch_stock_history(symbol).await {
                            Ok(daily_data) => {
                                if !daily_data.is_empty() {
                                    updated_stock.daily = daily_data;
                                    // 应用K线记录数量限制
                                    util::limit_kline_records(&mut updated_stock.daily, self.config.max_kline_records, symbol);
                                }
                            },
                            Err(e) => {
                                warn!("Failed to fetch history for {}: {}: {}", exchange, symbol, e);
                            }
                        }
                        break;
                    }
                }
            } else if !stock.daily.is_empty() {
                // 增量更新：检查是否已有该日期的数据
                let new_daily = &stock.daily[0]; // 最新的日线数据
                let date_exists = updated_stock.daily.iter().any(|d| d.date == new_daily.date);
                
                if !date_exists {
                    // 插入新的日线数据到前部
                    updated_stock.daily.insert(0, new_daily.clone());
                    
                    // 重新排序（确保按日期降序）
                    updated_stock.daily.sort_by(|a, b| b.date.cmp(&a.date));
                    
                    // 应用K线记录数量限制
                    util::limit_kline_records(&mut updated_stock.daily, self.config.max_kline_records, symbol);
                }
            }
            
            stocks_to_update.push(updated_stock);
        }
        
        // 更新所有股票
        for stock in stocks_to_update {
            let key = format!("{}:{}", stock.exchange, stock.symbol);
            if let Some(&idx) = existing_map.get(&key) {
                all_stocks[idx] = stock;
            } else {
                all_stocks.push(stock);
            }
        }
        
        // 保存更新后的数据
        let provider = StockDataProvider::new_with_data(all_stocks)?;
        provider.save_to_file(self.data_path.to_str().unwrap())?;
        
        Ok(())
    }
    
    /// 获取指定日期的所有股票数据
    pub async fn fetch_daily_data(&self, date: &NaiveDate) -> Result<Vec<StockData>> {
        let mut all_stocks = Vec::new();
        
        for scraper in &self.scrapers {
            info!("Scraping from {}", scraper.exchange_code());
            let mut stocks = scraper.fetch_stock_list(date).await?;
            
            // 调试模式：只处理前N个股票
            if self.config.debug_mode {
                let original_count = stocks.len();
                stocks.truncate(self.config.debug_stock_limit);
                info!("DEBUG MODE: Processing only {} out of {} stocks from {}", 
                      stocks.len(), original_count, scraper.exchange_code());
            }
            
            info!("Found {} stocks in {}", stocks.len(), scraper.exchange_code());
            all_stocks.extend(stocks);
        }
        
        if all_stocks.is_empty() {
            warn!("No stocks found for date {}", date);
        }
        
        Ok(all_stocks)
    }
}
