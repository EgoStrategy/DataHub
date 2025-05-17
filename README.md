# DataHub

股票市场数据处理系统，支持多交易所数据抓取和分析。

## 功能特点

- 支持多交易所数据抓取（上交所、深交所）
- 高效的数据存储和检索（使用Apache Arrow格式）
- 增量更新和全量更新模式
- 命令行工具支持数据抓取和浏览
- 可作为库集成到其他Rust项目中

## 安装

```bash
cargo install --git https://github.com/EgoStrategy/DataHub.git
```

## 使用方法

### 命令行工具

#### 抓取股票数据

```bash
# 抓取指定交易所的所有股票数据
egostrategy_datahub scrape --exchange sse --date 2025-05-16

# 抓取指定股票的历史数据
egostrategy_datahub scrape --exchange sse --symbol 600519

# 强制全量更新
egostrategy_datahub scrape --exchange sse --symbol 600519 --force-full

# 限制K线记录数量
egostrategy_datahub scrape --exchange sse --symbol 600519 --max-records 100
```

#### 浏览股票数据

```bash
# 浏览所有股票
egostrategy_datahub explore

# 浏览指定股票
egostrategy_datahub explore --symbol 600519

# 浏览指定交易所的股票
egostrategy_datahub explore --exchange sse
```

### 作为库使用

在 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
egostrategy_datahub = { git = "https://github.com/EgoStrategy/DataHub.git" }
```

示例代码：

```rust
use egostrategy_datahub::StockDataProvider;
use egostrategy_datahub::Result;

fn main() -> Result<()> {
    // 创建数据提供者
    let provider = StockDataProvider::new()?;
    
    // 获取特定股票数据
    if let Some(stock) = provider.get_stock_by_symbol("600519") {
        println!("股票: {} ({})", stock.name, stock.symbol);
        println!("交易所: {}", stock.exchange);
        println!("日线数据数量: {}", stock.daily.len());
        
        // 打印最新的日线数据
        if let Some(latest) = stock.daily.first() {
            println!("最新日期: {}", latest.date);
            println!("开盘价: {:.2}", latest.open);
            println!("收盘价: {:.2}", latest.close);
        }
    }
    
    Ok(())
}
```

## 开发

### 构建项目

```bash
cargo build
```

### 运行测试

```bash
cargo test
```

### 生成文档

```bash
cargo doc --open
```

## 许可证

MIT
