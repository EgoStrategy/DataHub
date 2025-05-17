use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema, Fields};
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::FileWriter;
use std::fs::File;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建日线数据的结构
    let daily_fields = Fields::from(vec![
        Field::new("date", DataType::Int32, false),
        Field::new("open", DataType::Float32, false),
        Field::new("high", DataType::Float32, false),
        Field::new("low", DataType::Float32, false),
        Field::new("close", DataType::Float32, false),
        Field::new("volume", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]);
    
    // 创建股票数据的结构
    let schema = Schema::new(vec![
        Field::new("exchange", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "daily",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(daily_fields.clone()),
                false,
            ))),
            true,
        ),
    ]);
    
    // 创建空的列数据
    let exchange_array: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
    let symbol_array: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
    let name_array: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
    
    // 创建一个空的列表数组
    let empty_list = arrow::array::new_empty_array(
        &DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(daily_fields),
            false,
        ))),
    );
    
    // 创建空的记录批次
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![exchange_array, symbol_array, name_array, empty_list],
    )?;
    
    // 确保目录存在
    std::fs::create_dir_all("data")?;
    
    // 创建文件并写入
    let file = File::create("data/stock.arrow")?;
    let mut writer = FileWriter::try_new(file, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    
    println!("成功创建空的 stock.arrow 文件");
    Ok(())
}
