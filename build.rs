use std::env;
use std::fs;
use std::path::Path;

fn main() {
    // 获取项目根目录
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    
    // 确保data目录存在
    let data_dir = Path::new(&manifest_dir).join("data");
    if !data_dir.exists() {
        fs::create_dir_all(&data_dir).expect("Failed to create data directory");
    }
    
    // 检查stock.arrow是否存在
    let arrow_file = data_dir.join("stock.arrow");
    if !arrow_file.exists() {
        println!("cargo:warning=stock.arrow file not found. Data provider will not work properly.");
        println!("cargo:warning=Please run 'cargo run -- scrape --exchange all' first to generate the data file.");
    } else {
        println!("cargo:rerun-if-changed=data/stock.arrow");
    }
}
