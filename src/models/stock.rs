use serde::Serialize;

/// 日线数据结构
#[derive(Debug, Clone, Serialize)]
pub struct DailyData {
    pub date: i32,
    pub open: f32,  // 从f64改为f32，减少内存占用
    pub high: f32,  // 从f64改为f32
    pub low: f32,   // 从f64改为f32
    pub close: f32, // 从f64改为f32
    pub volume: i64,
    pub amount: i64,
}

/// Stock data structure with nested daily data
#[derive(Debug, Clone, Serialize)]
pub struct StockData {
    pub exchange: String,
    pub symbol: String,
    pub name: String,
    pub daily: Vec<DailyData>,
}
