# DataHub

DataHub是一个用于股票市场数据的处理系统，使用Apache Arrow和Rust来高效地存储、处理和分析来自各种交易所的股票数据。

## 特性

- 从多个交易所抓取股票数据（上海证券交易所、深圳证券交易所）
- 将数据存储为Apache Arrow格式
- 支持增量更新数据
- 支持合并多个交易所的数据
- 高效的列式存储和查询
- 提供数据访问API，可作为依赖库使用

## 项目结构

```
DataHub/
├── src/
│   ├── models/         # 数据模型和Arrow模式定义
│   │   └── stock.rs    # 股票数据模型
│   ├── scrapers/       # 数据抓取器
│   │   ├── base.rs     # 抓取器基础特性
│   │   ├── sse.rs      # 上交所数据抓取器
│   │   └── szse.rs     # 深交所数据抓取器
│   ├── services/       # 服务层
│   │   └── data_service.rs # 数据处理服务
│   ├── data_provider/  # 数据提供API
│   ├── errors.rs       # 错误处理
│   └── config.rs       # 配置管理
├── data/               # 数据存储目录
│   └── stock.arrow     # 所有股票数据的统一存储文件
├── scripts/            # 脚本工具
│   ├── release.rs      # 发布脚本
│   └── extract_latest_date.rs # 提取最新日期
├── examples/           # 示例代码
│   └── use_data_provider.rs # 数据提供者使用示例
```

## 股票数据模型

股票数据模型采用嵌套结构，包括以下字段：

- **顶层字段**:
  - **交易所(exchange)**: 例如，SSE
  - **代码(symbol)**: 例如，603505
  - **简称(name)**: 例如，ST春天
  
- **嵌套字段**:
  - **日线数据(daily)**: 包含以下子字段的列表
    - 日期(date): YYYYMMDD格式，例如20250515
    - 开盘价(open): 例如，23.17
    - 最高价(high): 例如，23.44
    - 最低价(low): 例如，23.07
    - 收盘价(close): 例如，23.10
    - 成交量(volume): 例如，33929429
    - 成交额(amount): 例如，24828429392

## 开始使用

### 前提条件

- Rust（最新稳定版本）
- Cargo

### 安装

```bash
# 克隆仓库
git clone https://github.com/EgoStrategy/DataHub.git
cd DataHub

# 构建项目
cargo build --release
```

### 使用方法

#### 抓取数据

```bash
# 从上交所抓取特定日期的数据
cargo run -- scrape --exchange sse --date 2023-01-01

# 从深交所抓取特定股票的历史数据
cargo run -- scrape --exchange szse --symbol 000001

# 从所有交易所抓取数据
cargo run -- scrape --exchange all

# 指定输出目录
cargo run -- scrape --exchange all --output data/custom_dir
```

#### 作为库使用

在你的项目中添加依赖：

```toml
[dependencies]
egostrategy_datahub = { git = "https://github.com/EgoStrategy/DataHub.git", tag = "v2025.05.15" }
```

然后在代码中使用：

```rust
use egostrategy_datahub::data_provider::StockDataProvider;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建数据提供者
    let provider = StockDataProvider::new()?;
    
    // 获取特定股票数据
    if let Some(stock) = provider.get_stock_by_symbol("600000") {
        println!("股票: {} ({})", stock.name, stock.symbol);
        println!("交易所: {}", stock.exchange);
        println!("日线数据数量: {}", stock.daily.len());
        
        // 使用股票数据...
    }
    
    Ok(())
}
```

## 数据格式

数据以Arrow IPC文件格式存储在单一的stock.arrow文件中，采用嵌套结构。这种格式具有以下优势：

1. 列式存储，适合分析查询
2. 高效的内存映射和数据访问
3. 支持复杂的嵌套数据结构
4. 按股票代码组织数据，减少冗余
5. 跨语言兼容性

## 发布新版本

当有新的股票数据时，可以使用发布脚本自动更新版本号并发布：

```bash
# 提取最新交易日期
cargo run --bin extract_latest_date

# 发布新版本
cargo run --bin release
```

发布脚本会自动：
1. 从Arrow文件中提取最新交易日期作为版本号
2. 更新Cargo.toml中的版本号
3. 提交所有更改
4. 创建Git标签
5. 推送到远程仓库

## 许可证

本项目采用MIT许可证 - 详见LICENSE文件。
