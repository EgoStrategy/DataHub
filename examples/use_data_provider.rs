use egostrategy_datahub::data_provider::StockDataProvider;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建数据提供者
    let provider = StockDataProvider::new()?;
    
    // 获取最新交易日期
    if let Some(latest_date) = provider.get_latest_trading_date() {
        println!("最新交易日期: {}", latest_date);
    } else {
        println!("无法获取最新交易日期");
    }
    
    // 获取特定股票数据
    let symbol = "600000"; // 浦发银行
    if let Some(stock) = provider.get_stock_by_symbol(symbol) {
        println!("\n股票: {} ({})", stock.name, stock.symbol);
        println!("交易所: {}", stock.exchange);
        println!("日线数据数量: {}", stock.daily.len());
        
        // 显示最近5天数据
        println!("\n最近5天数据:");
        println!("{:<10} {:<10} {:<10} {:<10} {:<10} {:<15} {:<15}", 
                 "日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额");
        println!("{:-<80}", "");
        
        for daily in stock.daily.iter().take(5) {
            // 格式化日期
            let date_str = format!("{}", daily.date);
            let year = &date_str[0..4];
            let month = &date_str[4..6];
            let day = &date_str[6..8];
            let formatted_date = format!("{}-{}-{}", year, month, day);
            
            println!("{:<10} {:<10.2} {:<10.2} {:<10.2} {:<10.2} {:<15} {:<15}", 
                     formatted_date, daily.open, daily.high, daily.low, daily.close, 
                     daily.volume, daily.amount);
        }
    } else {
        println!("未找到股票: {}", symbol);
    }
    
    // 获取上交所所有股票
    let sse_stocks = provider.get_stocks_by_exchange("SSE");
    println!("\n上交所股票数量: {}", sse_stocks.len());
    
    // 获取深交所所有股票
    let szse_stocks = provider.get_stocks_by_exchange("SZSE");
    println!("深交所股票数量: {}", szse_stocks.len());
    
    // 统计所有股票数量
    println!("总股票数量: {}", provider.get_all_stocks().len());
    
    Ok(())
}
