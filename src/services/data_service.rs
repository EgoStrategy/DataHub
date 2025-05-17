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
        
        // 加载现有数据
        let provider = self.load_provider().await?;
        let mut stocks_to_update = Vec::new();
        
        // 查找匹配的交易所和股票名称
        let mut found_stock = false;
        
        for scraper in &self.scrapers {
            // 获取股票列表，查找匹配的股票
            let stock_list = scraper.fetch_stock_list(&actual_date).await?;
            
            for stock in stock_list {
                if stock.symbol == symbol {
                    found_stock = true;
                    
                    // 检查现有数据中是否已有该股票
                    if let Some(existing_stock) = provider.get_stock_by_symbol(symbol) {
                        let mut updated_stock = existing_stock.clone();
                        
                        // 始终使用最新的股票名称
                        updated_stock.name = stock.name;
                        
                        // 如果强制获取全量历史数据，或者现有数据为空
                        if self.config.force_full_history || updated_stock.daily.is_empty() {
                            info!("Fetching full history for stock {}", symbol);
                            let daily_data = scraper.fetch_stock_history(symbol).await?;
                            
                            if !daily_data.is_empty() {
                                updated_stock.daily = daily_data;
                                
                                // 应用K线记录数量限制
                                util::limit_kline_records(&mut updated_stock.daily, self.config.max_kline_records, symbol);
                            }
                        } else if !stock.daily.is_empty() {
                            // 增量更新：检查是否已有该日期的数据
                            let new_daily = &stock.daily[0]; // 最新的日线数据
                            
                            // 检查是否已存在该日期的数据
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
                    } else {
                        // 股票不存在于现有数据中，需要获取完整历史
                        let mut new_stock = stock.clone();
                        
                        // 如果daily为空，获取历史数据
                        if new_stock.daily.is_empty() {
                            let daily_data = scraper.fetch_stock_history(symbol).await?;
                            
                            if !daily_data.is_empty() {
                                new_stock.daily = daily_data;
                                
                                // 应用K线记录数量限制
                                util::limit_kline_records(&mut new_stock.daily, self.config.max_kline_records, symbol);
                            }
                        }
                        
                        stocks_to_update.push(new_stock);
                    }
                    
                    break;
                }
            }
            
            if found_stock {
                break;
            }
        }
        
        if !found_stock {
            return Err(DataHubError::DataError(format!(
                "Stock {} not found in any exchange for date {}", symbol, actual_date
            )));
        }
        
        // 更新数据提供者
        let mut all_stocks = provider.get_all_stocks().to_vec();
        
        // 更新或添加股票
        for stock_to_update in stocks_to_update {
            let index = all_stocks.iter().position(|s| 
                s.exchange == stock_to_update.exchange && s.symbol == stock_to_update.symbol
            );
            
            if let Some(idx) = index {
                all_stocks[idx] = stock_to_update;
            } else {
                all_stocks.push(stock_to_update);
            }
        }
        
        // 保存更新后的数据
        self.save_data(&all_stocks).await?;
        
        info!("Successfully processed stock: {}", symbol);
        Ok(())
    }
    
    /// 处理指定日期的所有股票
    pub async fn process_daily_stocks(&self, date: &NaiveDate) -> Result<()> {
        info!("Processing stocks for date: {}", date);
        
        // 加载现有数据
        let provider = self.load_provider().await?;
        let mut all_stocks = provider.get_all_stocks().to_vec();
        
        // 创建一个映射，用于快速查找现有数据
        let mut existing_map = HashMap::new();
        for (i, stock) in all_stocks.iter().enumerate() {
            let key = format!("{}:{}", stock.exchange, stock.symbol);
            existing_map.insert(key, i);
        }
        
        // 一次性获取所有交易所的股票列表
        let mut daily_stocks = Vec::new();
        
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
            daily_stocks.extend(stocks);
        }
        
        if daily_stocks.is_empty() {
            warn!("No stocks found for date {}", date);
            return Ok(());
        }
        
        // 创建一个映射，用于快速查找当日股票数据
        let mut daily_stock_map = HashMap::new();
        for stock in daily_stocks {
            daily_stock_map.insert(format!("{}:{}", stock.exchange, stock.symbol), stock);
        }
        
        // 处理每个股票
        let mut stocks_to_update = Vec::new();
        
        for (key, stock) in daily_stock_map.iter() {
            let symbol = &stock.symbol;
            let exchange = &stock.exchange;
            
            // 检查是否需要获取完整历史数据
            let need_full_history = if let Some(&idx) = existing_map.get(key) {
                // 股票已存在，检查是否需要全量更新
                self.config.force_full_history || all_stocks[idx].daily.is_empty()
            } else {
                // 股票不存在，需要获取完整历史
                true
            };
            
            let mut updated_stock = if let Some(&idx) = existing_map.get(key) {
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
                    
                    // 应���K线记录数量限制
                    util::limit_kline_records(&mut updated_stock.daily, self.config.max_kline_records, symbol);
                }
            }
            
            stocks_to_update.push(updated_stock);
        }
        
        // 更新所有股票
        for stock in &stocks_to_update {
            let key = format!("{}:{}", stock.exchange, stock.symbol);
            if let Some(&idx) = existing_map.get(&key) {
                all_stocks[idx] = stock.clone();
            } else {
                all_stocks.push(stock.clone());
            }
        }
        
        // 保存更新后的数据
        self.save_data(&all_stocks).await?;
        
        info!("Successfully processed {} stocks for date: {}", stocks_to_update.len(), date);
        Ok(())
    }
    
    /// 保存数据
    pub async fn save_data(&self, data: &[StockData]) -> Result<()> {
        // 保存到主数据文件
        let provider = StockDataProvider::new_with_data(data.to_vec())?;
        provider.save_to_file(self.data_path.to_str().unwrap())?;
        
        // 同时保存到 docs/data 目录
        let docs_dir = "docs/data";
        let docs_path = format!("{}/stock.arrow", docs_dir);
        
        // 确保目录存在
        std::fs::create_dir_all(docs_dir)?;
        
        // 保存数据
        util::arrow_utils::save_stock_data_to_arrow(data, &docs_path)?;
        
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
