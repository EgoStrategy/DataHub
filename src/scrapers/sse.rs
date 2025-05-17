use crate::models::stock::{StockData, DailyData};
use crate::errors::{Result, DataHubError};
use crate::scrapers::base::StockScraper;
use async_trait::async_trait;
use chrono::NaiveDate;
use reqwest::Client;
use serde_json::Value;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use log::{debug, info};

/// 上海证券交易所数据抓取器
pub struct SSEScraper {
    client: Client,
    last_request: Mutex<Option<Instant>>,
}

impl SSEScraper {
    /// 创建新的上交所数据抓取器
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| DataHubError::RequestError(e))?;
        
        Ok(Self {
            client,
            last_request: Mutex::new(None),
        })
    }
    
    /// 等待请求频率限制
    async fn wait_for_rate_limit(&self) {
        const MIN_INTERVAL: Duration = Duration::from_millis(500);
        
        let now = Instant::now();
        let should_wait = {
            let mut last = self.last_request.lock().unwrap();
            let should_wait = if let Some(instant) = *last {
                let elapsed = instant.elapsed();
                if elapsed < MIN_INTERVAL {
                    Some(MIN_INTERVAL - elapsed)
                } else {
                    None
                }
            } else {
                None
            };
            *last = Some(now);
            should_wait
        };
        
        if let Some(wait_time) = should_wait {
            debug!("等待 {:?} 以遵守频率限制", wait_time);
            tokio::time::sleep(wait_time).await;
        }
    }
}

#[async_trait]
impl StockScraper for SSEScraper {
    fn exchange_code(&self) -> &'static str {
        "SSE"
    }
    
    async fn fetch_stock_list(&self, date: &NaiveDate) -> Result<Vec<StockData>> {
        // Format date as YYYYMMDD integer
        let date_int = date.format("%Y%m%d").to_string().parse::<i32>()?;
        info!("获取上交所{}股票列表", date_int);

        // 限制请求频率
        self.wait_for_rate_limit().await;

        // 发送请求获取股票列表
        let response = self.client
            .get("https://yunhq.sse.com.cn:32042/v1/sh1/list/exchange/equity")
            .query(&[
                ("select", "code,name,open,high,low,last,volume,amount"),
                ("begin", "0"),
                ("end", "5000"),
            ])
            .header("Referer", "https://www.sse.com.cn/")
            .send()
            .await
            .map_err(|e| DataHubError::RequestError(e))?;
        
        let text = response.text().await?;
        debug!("成功获取响应");

        // 提取JSON部分
        let json_str = text
            .trim_start_matches("jsonpCallback31050241(")
            .trim_end_matches(")");
        
        let json: Value = serde_json::from_str(json_str)?;
        
        let mut stocks = Vec::new();
        if json.get("date").is_some_and(|x| x.as_i64().unwrap_or_default() as i32 != date_int) {
            return Ok(stocks);
        }
        // 解析股票列表
        if let Some(list) = json.get("list").and_then(|l| l.as_array()) {
            for stock_data in list {
                if let Some(stock_array) = stock_data.as_array() {
                    if stock_array.len() >= 8 {
                        let code = stock_array[0].as_str().unwrap_or_default().to_string();
                        let name = stock_array[1].as_str().unwrap_or_default().to_string();
                        let open = stock_array[2].as_f64().unwrap_or_default() as f32;  // 转换为f32
                        let high = stock_array[3].as_f64().unwrap_or_default() as f32;  // 转换为f32
                        let low = stock_array[4].as_f64().unwrap_or_default() as f32;   // 转换为f32
                        let close = stock_array[5].as_f64().unwrap_or_default() as f32; // 转换为f32
                        let volume = stock_array[6].as_i64().unwrap_or_default();
                        let amount = stock_array[7].as_i64().unwrap_or_default();
                        
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
            }
        }

        info!("成功获取 {} 支股票信息", stocks.len());
        Ok(stocks)
    }
    
    async fn fetch_stock_history(&self, symbol: &str) -> Result<Vec<DailyData>> {
        debug!("获取股票 {} 的历史K线数据", symbol);
        
        // 限制请求频率
        self.wait_for_rate_limit().await;
        
        let response = self.client
            .get(format!(
                "https://yunhq.sse.com.cn:32042/v1/sh1/dayk/{}",
                symbol
            ))
            .query(&[
                ("begin", "-1000"),
                ("end", "-1"),
                ("period", "day"),
            ])
            .header("Referer", "https://www.sse.com.cn/")
            .send()
            .await
            .map_err(|e| DataHubError::RequestError(e))?;
        
        let text = response.text().await?;
        
        // 提取JSON部分
        let json_str = text
            .trim_start_matches("jQuery")
            .split('(')
            .nth(1)
            .map(|s| s.trim_end_matches(')'))
            .unwrap_or(&text);
        
        let json: Value = serde_json::from_str(json_str)?;
        
        // 创建日线数据向量
        let mut daily_data = Vec::new();
        
        // 解析K线数据
        if let Some(kline) = json.get("kline").and_then(|k| k.as_array()) {
            for item in kline {
                if let Some(data) = item.as_array() {
                    if data.len() < 7 { continue; }
                    
                    let date = data[0].as_i64().unwrap_or_default() as i32;
                    let open = data[1].as_f64().unwrap_or_default() as f32;  // 转换为f32
                    let high = data[2].as_f64().unwrap_or_default() as f32;  // 转换为f32
                    let low = data[3].as_f64().unwrap_or_default() as f32;   // 转换为f32
                    let close = data[4].as_f64().unwrap_or_default() as f32; // 转换为f32
                    let volume = data[5].as_i64().unwrap_or_default();
                    let amount = data[6].as_i64().unwrap_or_default();
                    
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
        
        debug!("获取到 {} 条K线记录", daily_data.len());
        
        Ok(daily_data)
    }
}
