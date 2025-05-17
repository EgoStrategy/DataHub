// 公开导出的模块，供外部使用
pub mod models;
pub mod data_provider;
pub mod errors;

// 为了支持主程序，暂时保持这些模块公开
// 但在库使用场景中，这些应该是内部模块
#[doc(hidden)]
pub mod scrapers;
#[doc(hidden)]
pub mod config;
#[doc(hidden)]
pub mod services;
#[doc(hidden)]
pub mod util;

// 重新导出常用类型，方便使用
pub use models::stock::{StockData, DailyData};
pub use data_provider::StockDataProvider;
pub use errors::{Result, DataHubError};
