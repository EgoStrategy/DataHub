pub struct Config {
    pub debug_mode: bool,
    pub debug_stock_limit: usize,
    pub data_dir: String,
    pub max_kline_records: usize,
    pub force_full_history: bool,  // 新增字段
}

impl Config {
    pub fn new() -> Self {
        Self {
            debug_mode: false,
            debug_stock_limit: 10,
            data_dir: "data".to_string(),
            max_kline_records: 200,
            force_full_history: false,  // 默认为 false
        }
    }
    
    pub fn with_debug_mode(mut self, debug_mode: bool) -> Self {
        self.debug_mode = debug_mode;
        self
    }
    
    pub fn with_debug_stock_limit(mut self, limit: usize) -> Self {
        self.debug_stock_limit = limit;
        self
    }
    
    pub fn with_data_dir(mut self, dir: &str) -> Self {
        self.data_dir = dir.to_string();
        self
    }
    
    pub fn with_max_kline_records(mut self, max: usize) -> Self {
        self.max_kline_records = max;
        self
    }
    
    // 添加新的方法
    pub fn with_force_full_history(mut self, force_full: bool) -> Self {
        self.force_full_history = force_full;
        self
    }
}
