use std::error::Error;
use std::fs;
use std::process::Command;
use arrow::ipc::reader::FileReader;
use arrow_array::{Int32Array, ListArray, StructArray, Array};
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    // 获取最新交易日期
    let latest_date = get_latest_trading_date()?;
    
    // 将日期转换为 YYYY.M.D 格式（移除前导零）
    let date_str = latest_date.to_string();
    if date_str.len() != 8 {
        return Err(format!("无效的日期格式: {}", latest_date).into());
    }
    
    let year = &date_str[0..4];
    let month = date_str[4..6].parse::<u32>()?.to_string(); // 移除前导零
    let day = date_str[6..8].parse::<u32>()?.to_string(); // 移除前导零
    let version = format!("{}.{}.{}", year, month, day);
    
    println!("准备发布版本: {}", version);
    
    // 读取Cargo.toml
    let cargo_toml_path = "Cargo.toml";
    let cargo_toml = fs::read_to_string(cargo_toml_path)?;
    
    // 更新版本号
    let version_pattern = cargo_toml.lines()
        .find(|line| line.trim().starts_with("version = "))
        .unwrap_or("version = \"0.1.0\"");
    
    let current_version = version_pattern
        .trim()
        .trim_start_matches("version = ")
        .trim_matches('"');
    
    let updated_toml = cargo_toml.replace(
        &format!("version = \"{}\"", current_version),
        &format!("version = \"{}\"", version)
    );
    
    // 写回Cargo.toml
    fs::write(cargo_toml_path, updated_toml)?;
    
    println!("已更新版本号: {} -> {}", current_version, version);
    
    // 执行git命令
    println!("添加所有更改到git...");
    run_command("git", &["add", "."])?;
    
    println!("提交更改...");
    run_command("git", &["commit", "-m", &format!("Release version {}", version)])?;
    
    println!("创建标签...");
    run_command("git", &["tag", "-f", &format!("v{}", version)])?;
    
    // 检查是否有远程仓库配置
    let has_remote = Command::new("git")
        .args(&["remote", "-v"])
        .output()
        .map(|output| !output.stdout.is_empty())
        .unwrap_or(false);
    
    if has_remote {
        println!("推送到远程仓库...");
        // 检查当前分支是否有上游分支
        let branch_name = get_current_branch()?;
        
        // 尝试推送，如果失败则设置上游分支
        match Command::new("git")
            .args(&["push"])
            .status() {
            Ok(status) if status.success() => {
                // 推送成功
            },
            _ => {
                // 推送失败，尝试设置上游分支
                println!("设置上游分支...");
                run_command("git", &["push", "--set-upstream", "origin", &branch_name])?;
            }
        }
        
        println!("推送标签...");
        run_command("git", &["push", "--force", "origin", &format!("v{}", version)])?;
    } else {
        println!("未配置远程仓库，跳过推送步骤");
    }
    
    println!("发布完成! 版本: {}", version);
    
    Ok(())
}

fn run_command(cmd: &str, args: &[&str]) -> Result<(), Box<dyn Error>> {
    let status = Command::new(cmd)
        .args(args)
        .status()?;
    
    if !status.success() {
        return Err(format!("命令执行失败: {} {:?}", cmd, args).into());
    }
    
    Ok(())
}

fn get_current_branch() -> Result<String, Box<dyn Error>> {
    let output = Command::new("git")
        .args(&["rev-parse", "--abbrev-ref", "HEAD"])
        .output()?;
    
    if !output.status.success() {
        return Err("无法获取当前分支名称".into());
    }
    
    let branch = String::from_utf8(output.stdout)?
        .trim()
        .to_string();
    
    Ok(branch)
}

fn get_latest_trading_date() -> Result<i32, Box<dyn Error>> {
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
    
    if latest_date == 0 {
        return Err("未找到有效的交易日期".into());
    }
    
    Ok(latest_date)
}
