use chrono::NaiveDate;
use log::info;
use crate::models::stock::{StockData, DailyData};
use crate::errors::{Result, DataHubError};

// 日期转换工具
pub fn date_string_to_int(date_str: &str) -> Result<i32> {
    date_str.parse::<i32>().map_err(|e| DataHubError::DataError(e.to_string()))
}

pub fn int_to_naive_date(date_int: i32) -> Result<NaiveDate> {
    let date_str = date_int.to_string();
    if date_str.len() != 8 {
        return Err(DataHubError::DataError(format!("Invalid date format: {}", date_str)));
    }
    
    let year = date_str[0..4].parse::<i32>()
        .map_err(|e| DataHubError::DataError(e.to_string()))?;
    let month = date_str[4..6].parse::<u32>()
        .map_err(|e| DataHubError::DataError(e.to_string()))?;
    let day = date_str[6..8].parse::<u32>()
        .map_err(|e| DataHubError::DataError(e.to_string()))?;
    
    NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| DataHubError::DataError(format!("Invalid date: {}-{}-{}", year, month, day)))
}

// 限制K线记录数量
pub fn limit_kline_records(daily_data: &mut Vec<DailyData>, max_records: usize, symbol: &str) {
    if daily_data.len() > max_records {
        info!("Limiting {} K-line records to {} for stock {}", 
                 daily_data.len(), max_records, symbol);
        daily_data.truncate(max_records);
    }
}

// Arrow数据转换工具
pub mod arrow_utils {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, Fields};
    use arrow::array::{ArrayRef, StringBuilder};
    use arrow_array::{Int32Array, Float32Array, Int64Array, StructArray, ListArray, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow::buffer::NullBuffer;
    use log::info;
    use std::sync::Arc;
    use std::io::Cursor;
    use arrow::ipc::reader::FileReader;
    use arrow::ipc::writer::FileWriter;
    use std::fs::File;
    use arrow_array::Array;

    // 将股票数据转换为Arrow记录批次
    pub fn stock_data_to_record_batch(data: &[StockData]) -> Result<RecordBatch> {
        // 创建数组构建器
        let mut exchange_builder = StringBuilder::new();
        let mut symbol_builder = StringBuilder::new();
        let mut name_builder = StringBuilder::new();
        
        // 创建日线数据的字段
        let daily_fields = Fields::from(vec![
            Field::new("date", DataType::Int32, false),
            Field::new("open", DataType::Float32, false),
            Field::new("high", DataType::Float32, false),
            Field::new("low", DataType::Float32, false),
            Field::new("close", DataType::Float32, false),
            Field::new("volume", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]);
        
        // 创建日线数据数组
        let mut date_values = Vec::new();
        let mut open_values = Vec::new();
        let mut high_values = Vec::new();
        let mut low_values = Vec::new();
        let mut close_values = Vec::new();
        let mut volume_values = Vec::new();
        let mut amount_values = Vec::new();
        let mut offsets = vec![0];
        let mut validity = Vec::new();
        
        // 填充数据
        for stock in data {
            exchange_builder.append_value(&stock.exchange);
            symbol_builder.append_value(&stock.symbol);
            name_builder.append_value(&stock.name);
            
            // 添加日线数据
            for daily in &stock.daily {
                date_values.push(daily.date);
                open_values.push(daily.open);
                high_values.push(daily.high);
                low_values.push(daily.low);
                close_values.push(daily.close);
                volume_values.push(daily.volume);
                amount_values.push(daily.amount);
            }
            
            offsets.push(offsets.last().unwrap() + stock.daily.len() as i32);
            validity.push(true);
        }
        
        // 创建日线数据的结构数组
        let date_array = Int32Array::from(date_values);
        let open_array = Float32Array::from(open_values);
        let high_array = Float32Array::from(high_values);
        let low_array = Float32Array::from(low_values);
        let close_array = Float32Array::from(close_values);
        let volume_array = Int64Array::from(volume_values);
        let amount_array = Int64Array::from(amount_values);
        
        let struct_array = StructArray::try_new(
            daily_fields.clone(),
            vec![
                Arc::new(date_array),
                Arc::new(open_array),
                Arc::new(high_array),
                Arc::new(low_array),
                Arc::new(close_array),
                Arc::new(volume_array),
                Arc::new(amount_array),
            ],
            None,
        ).map_err(|e| DataHubError::ArrowError(e.to_string()))?;
        
        // 创建列表数组
        let offset_buffer = arrow::buffer::ScalarBuffer::from(offsets);
        let list_array = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Struct(daily_fields.clone()), false)),
            arrow::buffer::OffsetBuffer::new(offset_buffer),
            Arc::new(struct_array),
            Some(NullBuffer::from(validity)),
        ).map_err(|e| DataHubError::ArrowError(e.to_string()))?;
        
        // 构建最终的数组
        let exchange_array: ArrayRef = Arc::new(exchange_builder.finish());
        let symbol_array: ArrayRef = Arc::new(symbol_builder.finish());
        let name_array: ArrayRef = Arc::new(name_builder.finish());
        let daily_array: ArrayRef = Arc::new(list_array);
        
        // 创建Schema
        let schema = Schema::new(vec![
            Field::new("exchange", DataType::Utf8, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "daily",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(daily_fields),
                    false,
                ))),
                true,
            ),
        ]);
        
        // 创建RecordBatch
        RecordBatch::try_new(
            Arc::new(schema),
            vec![exchange_array, symbol_array, name_array, daily_array],
        )
        .map_err(|e| DataHubError::ArrowError(e.to_string()))
    }

    // 从Arrow文件读取股票数据
    pub fn read_stock_data_from_arrow(path: &str) -> Result<Vec<StockData>> {
        let file = File::open(path)?;
        let reader = FileReader::try_new(file, None)
            .map_err(|e| DataHubError::ArrowError(e.to_string()))?;
        
        let mut result = Vec::new();
        
        for batch in reader {
            let batch = batch.map_err(|e| DataHubError::ArrowError(e.to_string()))?;
            
            let exchange_array = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| DataHubError::ArrowError("Failed to downcast exchange column".to_string()))?;
            let symbol_array = batch.column(1).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| DataHubError::ArrowError("Failed to downcast symbol column".to_string()))?;
            let name_array = batch.column(2).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| DataHubError::ArrowError("Failed to downcast name column".to_string()))?;
            let daily_array = batch.column(3).as_any().downcast_ref::<ListArray>()
                .ok_or_else(|| DataHubError::ArrowError("Failed to downcast daily column".to_string()))?;
            
            for i in 0..batch.num_rows() {
                let exchange = exchange_array.value(i).to_string();
                let symbol = symbol_array.value(i).to_string();
                let name = name_array.value(i).to_string();
                
                let mut daily_data = Vec::new();
                
                if !daily_array.is_null(i) {
                    let daily_list = daily_array.value(i);
                    if let Some(daily_struct) = daily_list.as_any().downcast_ref::<StructArray>() {
                        if let (Some(date_array), Some(open_array), Some(high_array), 
                                Some(low_array), Some(close_array), Some(volume_array), Some(amount_array)) = (
                            daily_struct.column_by_name("date").and_then(|a| a.as_any().downcast_ref::<Int32Array>()),
                            daily_struct.column_by_name("open").and_then(|a| a.as_any().downcast_ref::<Float32Array>()),
                            daily_struct.column_by_name("high").and_then(|a| a.as_any().downcast_ref::<Float32Array>()),
                            daily_struct.column_by_name("low").and_then(|a| a.as_any().downcast_ref::<Float32Array>()),
                            daily_struct.column_by_name("close").and_then(|a| a.as_any().downcast_ref::<Float32Array>()),
                            daily_struct.column_by_name("volume").and_then(|a| a.as_any().downcast_ref::<Int64Array>()),
                            daily_struct.column_by_name("amount").and_then(|a| a.as_any().downcast_ref::<Int64Array>())
                        ) {
                            for j in 0..daily_struct.len() {
                                daily_data.push(DailyData {
                                    date: date_array.value(j),
                                    open: open_array.value(j),
                                    high: high_array.value(j),
                                    low: low_array.value(j),
                                    close: close_array.value(j),
                                    volume: volume_array.value(j),
                                    amount: amount_array.value(j),
                                });
                            }
                        } else {
                            return Err(DataHubError::ArrowError("Missing required columns in daily data".to_string()));
                        }
                    } else {
                        return Err(DataHubError::ArrowError("Failed to downcast daily struct".to_string()));
                    }
                }
                
                result.push(StockData {
                    exchange,
                    symbol,
                    name,
                    daily: daily_data,
                });
            }
        }
        
        Ok(result)
    }

    // 将股票数据保存到Arrow文件
    pub fn save_stock_data_to_arrow(data: &[StockData], path: &str) -> Result<()> {
        // 打印保存的数据信息
        info!("Saving {} stocks to {}", data.len(), path);
        for stock in data {
            info!("  - {} ({}) - {}: {} daily records", 
                     stock.name, stock.symbol, stock.exchange, stock.daily.len());
        }
        
        let batch = stock_data_to_record_batch(data)?;
        let file = File::create(path)?;
        
        // 使用默认选项，不启用压缩，确保与JavaScript SDK兼容
        let mut writer = FileWriter::try_new(file, &batch.schema())
            .map_err(|e| DataHubError::ArrowError(e.to_string()))?;
        
        writer.write(&batch)
            .map_err(|e| DataHubError::ArrowError(e.to_string()))?;
        writer.finish()
            .map_err(|e| DataHubError::ArrowError(e.to_string()))?;
        
        Ok(())
    }

    // 从内存中读取Arrow数据
    pub fn read_stock_data_from_memory(data: &[u8]) -> Result<Vec<StockData>> {
        let reader = FileReader::try_new(
            Cursor::new(data), 
            None
        ).map_err(|e| DataHubError::ArrowError(e.to_string()))?;
        
        let mut result = Vec::new();
        
        for batch in reader {
            let batch = batch.map_err(|e| DataHubError::ArrowError(e.to_string()))?;
            
            let exchange_array = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| DataHubError::ArrowError("Failed to downcast exchange column".to_string()))?;
            let symbol_array = batch.column(1).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| DataHubError::ArrowError("Failed to downcast symbol column".to_string()))?;
            let name_array = batch.column(2).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| DataHubError::ArrowError("Failed to downcast name column".to_string()))?;
            let daily_array = batch.column(3).as_any().downcast_ref::<ListArray>()
                .ok_or_else(|| DataHubError::ArrowError("Failed to downcast daily column".to_string()))?;
            
            for i in 0..batch.num_rows() {
                let exchange = exchange_array.value(i).to_string();
                let symbol = symbol_array.value(i).to_string();
                let name = name_array.value(i).to_string();
                
                let mut daily_data = Vec::new();
                
                if !daily_array.is_null(i) {
                    let daily_list = daily_array.value(i);
                    if let Some(daily_struct) = daily_list.as_any().downcast_ref::<StructArray>() {
                        if let (Some(date_array), Some(open_array), Some(high_array), 
                                Some(low_array), Some(close_array), Some(volume_array), Some(amount_array)) = (
                            daily_struct.column_by_name("date").and_then(|a| a.as_any().downcast_ref::<Int32Array>()),
                            daily_struct.column_by_name("open").and_then(|a| a.as_any().downcast_ref::<Float32Array>()),
                            daily_struct.column_by_name("high").and_then(|a| a.as_any().downcast_ref::<Float32Array>()),
                            daily_struct.column_by_name("low").and_then(|a| a.as_any().downcast_ref::<Float32Array>()),
                            daily_struct.column_by_name("close").and_then(|a| a.as_any().downcast_ref::<Float32Array>()),
                            daily_struct.column_by_name("volume").and_then(|a| a.as_any().downcast_ref::<Int64Array>()),
                            daily_struct.column_by_name("amount").and_then(|a| a.as_any().downcast_ref::<Int64Array>())
                        ) {
                            for j in 0..daily_struct.len() {
                                daily_data.push(DailyData {
                                    date: date_array.value(j),
                                    open: open_array.value(j),
                                    high: high_array.value(j),
                                    low: low_array.value(j),
                                    close: close_array.value(j),
                                    volume: volume_array.value(j),
                                    amount: amount_array.value(j),
                                });
                            }
                        } else {
                            return Err(DataHubError::ArrowError("Missing required columns in daily data".to_string()));
                        }
                    } else {
                        return Err(DataHubError::ArrowError("Failed to downcast daily struct".to_string()));
                    }
                }
                
                result.push(StockData {
                    exchange,
                    symbol,
                    name,
                    daily: daily_data,
                });
            }
        }
        
        Ok(result)
    }
}
