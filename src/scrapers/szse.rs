use crate::models::stock::{StockData, DailyData};
use crate::scrapers::base::StockScraper;
use crate::errors::{Result, DataHubError};
use async_trait::async_trait;
use calamine::{open_workbook_auto_from_rs, Reader, DataType};
use chrono::NaiveDate;
use log::info;
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::Mutex;
use std::time::Instant;

// 用于限制请求频率的全局变量
static LAST_REQUEST: Mutex<Option<Instant>> = Mutex::const_new(None);

pub struct SZSEScraper {
    client: Client,
    request_interval: Duration,
}

impl SZSEScraper {
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| DataHubError::RequestError(e))?;
        
        Ok(Self { 
            client,
            request_interval: Duration::from_millis(500),
        })
    }
    
    // 添加请求限速机制
    async fn wait_for_rate_limit(&self) {
        let now = Instant::now();
        let mut last = LAST_REQUEST.lock().await;
        
        if let Some(time) = *last {
            let elapsed = time.elapsed();
            if elapsed < self.request_interval {
                tokio::time::sleep(self.request_interval - elapsed).await;
            }
        }
        
        *last = Some(now);
    }
    
}

#[async_trait]
impl StockScraper for SZSEScraper {
    fn exchange_code(&self) -> &'static str {
        "SZSE"
    }
    
    async fn fetch_stock_list(&self, date: &NaiveDate) -> Result<Vec<StockData>> {
        let date_str = date.format("%Y-%m-%d").to_string();
        info!("开始获取深交所股票列表，日期: {}", date_str);
        
        // 限制请求频率
        self.wait_for_rate_limit().await;
        
        // 发送请求获取股票快照数据
        let response = self.client
            .get(format!("https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1815_stock_snapshot&txtBeginDate={}&txtEndDate={}", &date_str, &date_str))
            .send()
            .await?;

        // 获取文件内容
        let bytes = response.bytes().await?;

        // 使用 calamine 打开工作簿
        let mut workbook = open_workbook_auto_from_rs(std::io::Cursor::new(bytes))
            .map_err(|e| DataHubError::ExcelError(e))?;
        
        // 获取第一个工作表
        let range = workbook
            .worksheet_range_at(0)
            .ok_or_else(|| DataHubError::DataError("XLSX文件中没有工作表".to_string()))?
            .map_err(|e| DataHubError::ExcelError(e))?;

        let mut stocks = Vec::new();
        let date_int = date.format("%Y%m%d").to_string().parse::<i32>()?;

        // 跳过表头行，从第二行开始解析
        for row in range.rows().skip(1) {
            if row.len() >= 11 {  // 确保有足够的列
                let code = match row.get(1) {
                    Some(cell) => cell.to_string(),
                    None => continue,
                };
                
                let name = match row.get(2) {
                    Some(cell) => cell.to_string(),
                    None => continue,
                };
                
                // 使用安全的解析方法
                let open = match row.get(4) {
                    Some(cell) => cell.as_f64().unwrap() as f32,
                    None => 0.0,
                };
                
                let high = match row.get(5) {
                    Some(cell) => cell.as_f64().unwrap() as f32,
                    None => 0.0,
                };
                
                let low = match row.get(6) {
                    Some(cell) => cell.as_f64().unwrap() as f32,
                    None => 0.0,
                };
                
                let close = match row.get(7) {
                    Some(cell) => cell.as_f64().unwrap() as f32,
                    None => 0.0,
                };
                
                let volume = match row.get(9) {
                    Some(cell) => {
                        (cell.as_string().unwrap().replace(",", "").parse::<f64>().unwrap() * 10000.0).round() as i64
                    },
                    None => 0,
                };
                
                let amount = match row.get(10) {
                    Some(cell) => {
                        (cell.as_string().unwrap().replace(",", "").parse::<f64>().unwrap() * 10000.0).round() as i64
                    },
                    None => 0,
                };
                
                stocks.push(StockData {
                    exchange: self.exchange_code().to_string(),
                    symbol: code,
                    name,
                    daily: vec![DailyData {
                        date: date_int,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        amount,
                    }],
                });
            }
        }

        info!("成功获取 {} 的 {} 支股票信息", date_str, stocks.len());
        Ok(stocks)
    }
    
    async fn fetch_stock_history(&self, symbol: &str) -> Result<Vec<DailyData>> {
        info!("开始获取深交所股票{}的历史数据", symbol);
        
        // 限制请求频率
        self.wait_for_rate_limit().await;
        
        let url = format!(
            "https://www.szse.cn/api/market/ssjjhq/getHistoryData?cycleType=32&marketId=1&code={}",
            symbol
        );
        
        let response = self.client.get(&url)
            .send()
            .await?;
            
        let json: Value = response.json().await?;

        // 创建日线数据向量
        let mut daily_data = Vec::new();
        
        if let Some(data) = json.get("data").and_then(|d| d.get("picupdata")).and_then(|d| d.as_array()) {
            for item in data {
                if let Some(array) = item.as_array() {
                    if array.len() < 9 {
                        continue;
                    }
                    
                    let date_str = match array[0].as_str() {
                        Some(s) => s.replace("-", ""),
                        None => continue,
                    };
                    
                    let date = date_str.parse::<i32>()
                        .map_err(|_| DataHubError::DataError(format!("Invalid date format: {}", date_str)))?;
                    
                    // 使用更安全的方式解析价格数据
                    let open = match array[1].as_str() {
                        Some(s) => s.parse::<f32>()  // 改为f32
                            .map_err(|_| DataHubError::DataError("Invalid open price format".to_string()))?,
                        None => continue,
                    };
                    
                    let high = match array[4].as_str() {
                        Some(s) => s.parse::<f32>()  // 改为f32
                            .map_err(|_| DataHubError::DataError("Invalid high price format".to_string()))?,
                        None => continue,
                    };
                    
                    let low = match array[3].as_str() {
                        Some(s) => s.parse::<f32>()  // 改为f32
                            .map_err(|_| DataHubError::DataError("Invalid low price format".to_string()))?,
                        None => continue,
                    };
                    
                    let close = match array[2].as_str() {
                        Some(s) => s.parse::<f32>()  // 改为f32
                            .map_err(|_| DataHubError::DataError("Invalid close price format".to_string()))?,
                        None => continue,
                    };
                    
                    let volume = array[7].as_i64().unwrap_or_default() * 100;
                    let amount = array[8].as_f64().unwrap_or_default() as i64;
                    
                    daily_data.push(DailyData {
                        date,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        amount,
                    });
                }
            }
        }
        
        // 按日期降序排序
        daily_data.sort_by(|a, b| b.date.cmp(&a.date));
        
        info!("获取到 {} 条K线记录", daily_data.len());
        
        // 返回日线数据向量
        Ok(daily_data)
    }
}
