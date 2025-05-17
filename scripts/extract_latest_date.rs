use std::error::Error;
use std::fs;
use arrow::ipc::reader::FileReader;
use arrow_array::{Int32Array, ListArray, StructArray, Array};
use std::path::Path;
use chrono::NaiveDate;

fn main() -> Result<(), Box<dyn Error>> {
    // 检查数据文件是否存在
    let arrow_path = Path::new("data/stock.arrow");
    if !arrow_path.exists() {
        return Err("数据文件不存在，请先运行 'cargo run -- scrape --exchange all'".into());
    }
    
    // 读取Arrow文件
    let file = fs::File::open(arrow_path)?;
    let reader = FileReader::try_new(file, None)?;
    
    // 查找最新交易日期
    let mut latest_date = 0;
    
    for batch_result in reader {
        let batch = batch_result?;
        let daily_array = batch.column(3).as_any().downcast_ref::<ListArray>().unwrap();
        
        for i in 0..batch.num_rows() {
            if !daily_array.is_null(i) {
                let daily_list = daily_array.value(i);
                if let Some(daily_struct) = daily_list.as_any().downcast_ref::<StructArray>() {
                    if let Some(date_array) = daily_struct.column_by_name("date")
                        .and_then(|a| a.as_any().downcast_ref::<Int32Array>()) {
                        if date_array.len() > 0 {
                            let date = date_array.value(0);
                            if date > latest_date {
                                latest_date = date;
                            }
                        }
                    }
                }
            }
        }
    }
    
    if latest_date > 0 {
        // 将整数日期转换为NaiveDate
        let date_str = latest_date.to_string();
        if date_str.len() == 8 {
            let year = date_str[0..4].parse::<i32>()?;
            let month = date_str[4..6].parse::<u32>()?;
            let day = date_str[6..8].parse::<u32>()?;
            
            if let Some(date) = NaiveDate::from_ymd_opt(year, month, day) {
                println!("最新交易日期: {}", date);
                
                // 格式化为 YYYY.M.D 版本号（移除前导零）
                let formatted_version = format!("{}.{}.{}", year, month, day);
                println!("版本号: {}", formatted_version);
            } else {
                println!("无效的日期: {}", latest_date);
            }
        } else {
            println!("无效的日期格式: {}", latest_date);
        }
    } else {
        println!("未找到有效的日期");
    }
    
    Ok(())
}
