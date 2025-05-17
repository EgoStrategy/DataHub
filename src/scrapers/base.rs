use crate::models::stock::{StockData, DailyData};
use crate::errors::Result;
use async_trait::async_trait;
use chrono::NaiveDate;

/// Base trait for stock data scrapers
#[async_trait]
pub trait StockScraper {
    /// Get the exchange code this scraper is for
    fn exchange_code(&self) -> &'static str;
    
    /// Fetch stock list for the given date
    async fn fetch_stock_list(&self, date: &NaiveDate) -> Result<Vec<StockData>>;
    
    /// Fetch historical data for a specific stock
    /// Returns daily data for the specified stock
    async fn fetch_stock_history(&self, symbol: &str) -> Result<Vec<DailyData>>;
}
