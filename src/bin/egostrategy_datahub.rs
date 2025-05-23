use egostrategy_datahub::models::stock::StockData;
use egostrategy_datahub::scrapers::base::StockScraper;
use egostrategy_datahub::scrapers::sse::SSEScraper;
use egostrategy_datahub::scrapers::szse::SZSEScraper;
use egostrategy_datahub::services::data_service::DataService;
use egostrategy_datahub::util::arrow_utils;
use egostrategy_datahub::config::Config;

use clap::{value_parser, Arg, Command};
use chrono::{Local, NaiveDate};
use log::{info, error};
use std::error::Error;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logger
    env_logger::init();
    
    // 创建基本的命令行应用
    let app = Command::new("DataHub")
        .version("1.0.0")
        .author("DataHub Team")
        .about("Stock market data processing system");

    // 在开发模式下添加调试参数
    #[cfg(debug_assertions)]
    let app = app.arg(
        Arg::new("debug")
            .long("debug")
            .help("Enable debug mode")
            .action(clap::ArgAction::SetTrue),
    )
    .arg(
        Arg::new("debug-limit")
            .long("debug-limit")
            .help("Limit the number of stocks to process in debug mode")
            .value_parser(value_parser!(usize))
            .default_value("2"),
    );

    // 添加子命令
    let app = app.subcommand(
        Command::new("scrape")
            .about("Scrape stock data from various exchanges")
            .arg(
                Arg::new("exchange")
                    .short('e')
                    .long("exchange")
                    .value_name("EXCHANGE")
                    .help("Exchange to scrape data from (sse, szse, all)")
                    .required(true)
                    .value_parser(value_parser!(String)),
            )
            .arg(
                Arg::new("date")
                    .short('d')
                    .long("date")
                    .value_name("DATE")
                    .help("Date to scrape data for (YYYY-MM-DD)")
                    .value_parser(value_parser!(String))
                    .default_value(Local::now().format("%Y-%m-%d").to_string()),
            )
            .arg(
                Arg::new("symbol")
                    .short('s')
                    .long("symbol")
                    .value_name("SYMBOL")
                    .help("Stock symbol to scrape history for (optional)")
                    .value_parser(value_parser!(String)),
            )
            .arg(
                Arg::new("max-records")
                    .long("max-records")
                    .value_name("MAX_RECORDS")
                    .help("Maximum number of kline records to keep per stock")
                    .value_parser(value_parser!(usize))
                    .default_value("200"),
            )
            .arg(
                Arg::new("force-full")
                    .short('f')
                    .long("force-full")
                    .help("Force fetching full history data even if incremental data exists")
                    .action(clap::ArgAction::SetTrue),
            ),
    ).subcommand(
        Command::new("explore")
            .about("Explore stock data")
            .arg(
                Arg::new("symbol")
                    .short('s')
                    .long("symbol")
                    .value_name("SYMBOL")
                    .value_parser(value_parser!(String))
                    .help("Stock symbol to explore"),
            )
            .arg(
                Arg::new("exchange")
                    .short('e')
                    .long("exchange")
                    .value_name("EXCHANGE")
                    .value_parser(value_parser!(String))
                    .help("Exchange to filter by (sse, szse)"),
            )
            .arg(
                Arg::new("limit")
                    .short('l')
                    .long("limit")
                    .value_name("LIMIT")
                    .help("Limit the number of records to display")
                    .value_parser(value_parser!(usize))
                    .default_value("10"),
            )
    );

    let matches = app.get_matches();

    // 获取调试模式设置
    #[cfg(debug_assertions)]
    let debug_mode = matches.get_flag("debug");
    #[cfg(not(debug_assertions))]
    let debug_mode = false;

    #[cfg(debug_assertions)]
    let debug_stock_limit = matches.get_one::<usize>("debug-limit").unwrap().clone();
    #[cfg(not(debug_assertions))]
    let debug_stock_limit = usize::MAX;

    if let Some(matches) = matches.subcommand_matches("scrape") {
        let exchange = matches.get_one::<String>("exchange").unwrap();
        let date_str = matches.get_one::<String>("date").unwrap();
        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")?;
        let symbol = matches.get_one::<String>("symbol");
        let force_full = matches.get_flag("force-full");
        
        // 获取最大K线记录数量
        let max_kline_records = matches.get_one::<usize>("max-records").unwrap().clone();
        
        // Create scrapers
        let scrapers: Vec<Arc<dyn StockScraper + Send + Sync>> = match exchange.to_lowercase().as_str() {
            "sse" => vec![Arc::new(SSEScraper::new()?)],
            "szse" => vec![Arc::new(SZSEScraper::new()?)],
            "all" => vec![Arc::new(SSEScraper::new()?), Arc::new(SZSEScraper::new()?)],
            _ => {
                error!("Unknown exchange: {}", exchange);
                return Err(format!("Unknown exchange: {}", exchange).into());
            }
        };
        
        // 创建配置
        let config = Config::new()
            .with_debug_mode(debug_mode)
            .with_debug_stock_limit(debug_stock_limit)
            .with_max_kline_records(max_kline_records)
            .with_force_full_history(force_full);
        
        info!("Using max kline records: {}", config.max_kline_records);
        if force_full {
            info!("Force full history mode enabled");
        }
        
        // 创建数据服务
        let data_service = DataService::new(config, scrapers);
        
        if let Some(symbol) = symbol {
            // 处理单个股票
            data_service.process_single_stock(symbol, Some(&date)).await?;
        } else {
            // 处理指定日期的所有股票
            data_service.process_daily_stocks(&date).await?;
        }
    } else if let Some(matches) = matches.subcommand_matches("explore") {
        let symbol_filter = matches.get_one::<String>("symbol");
        let exchange_filter = matches.get_one::<String>("exchange");
        let limit = matches.get_one::<usize>("limit").unwrap().clone();
        
        // 读取数据
        let stocks = arrow_utils::read_stock_data_from_arrow("docs/data/stock.arrow")?;
        
        info!("Found {} stocks in database", stocks.len());
        
        // 过滤数据
        let filtered_stocks: Vec<&StockData> = stocks.iter()
            .filter(|s| {
                if let Some(symbol) = symbol_filter {
                    if !s.symbol.contains(symbol) {
                        return false;
                    }
                }
                
                if let Some(exchange) = exchange_filter {
                    if s.exchange.to_lowercase() != exchange.to_lowercase() {
                        return false;
                    }
                }
                
                true
            })
            .collect();
        
        info!("Filtered to {} stocks", filtered_stocks.len());
        
        // 显示结果
        for (i, stock) in filtered_stocks.iter().enumerate() {
            if i >= limit {
                break;
            }
            
            info!("Stock: {} ({}) - {}", stock.name, stock.symbol, stock.exchange);
            info!("{:-<60}", "");
            info!("{:<10} {:<10} {:<10} {:<10} {:<10} {:<15} {:<15}", 
                     "Date", "Open", "High", "Low", "Close", "Volume", "Amount");
            info!("{:-<60}", "");
            
            for daily in stock.daily.iter().take(limit) {
                // Format date as YYYY-MM-DD
                let date_str = format!("{}", daily.date);
                let year = &date_str[0..4];
                let month = &date_str[4..6];
                let day = &date_str[6..8];
                let formatted_date = format!("{}-{}-{}", year, month, day);
                
                info!("{:<10} {:<10.2} {:<10.2} {:<10.2} {:<10.2} {:<15} {:<15}", 
                         formatted_date, daily.open, daily.high, daily.low, daily.close, 
                         daily.volume, daily.amount);
            }
            
            if stock.daily.len() > limit {
                info!("... and {} more records", stock.daily.len() - limit);
            } else if stock.daily.is_empty() {
                info!("No daily data available for this stock");
            }
        }
    } else {
        info!("No command specified. Use --help for usage information.");
    }
    
    Ok(())
}
