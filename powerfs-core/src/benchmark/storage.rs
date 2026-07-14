
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, Error as SqliteError, Result, NO_PARAMS};
use serde_json;
use std::sync::{Arc, Mutex};

use super::{BenchmarkResult, BenchmarkStorage, OptimizationFlags};

// SQLite 持久化存储实现
pub struct SqliteBenchmarkStorage {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteBenchmarkStorage {
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        Self::init_tables(&conn)?;
        
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn init_tables(conn: &Connection) -> Result<()> {
        // 创建基准测试结果表
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS benchmark_results (
                id TEXT PRIMARY KEY,
                test_name TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                flags_json TEXT NOT NULL,
                metrics_json TEXT NOT NULL,
                environment_json TEXT NOT NULL,
                duration_seconds INTEGER NOT NULL,
                comparison_json TEXT
            )
            "#,
            NO_PARAMS,
        )?;

        // 创建索引
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_benchmark_timestamp ON benchmark_results(timestamp)",
            NO_PARAMS,
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_benchmark_test_name ON benchmark_results(test_name)",
            NO_PARAMS,
        )?;

        Ok(())
    }

    pub fn new_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::init_tables(&conn)?;
        
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

impl BenchmarkStorage for SqliteBenchmarkStorage {
    fn save_result(&self, result: &BenchmarkResult) -> Result<(), String> {
        let flags_json = serde_json::to_string(&result.flags)
            .map_err(|e| format!("Failed to serialize flags: {}", e))?;
        
        let metrics_json = serde_json::to_string(&result.metrics)
            .map_err(|e| format!("Failed to serialize metrics: {}", e))?;
        
        let environment_json = serde_json::to_string(&result.environment)
            .map_err(|e| format!("Failed to serialize environment: {}", e))?;
        
        let comparison_json = match &result.comparison {
            Some(c) => serde_json::to_string(c)
                .map_err(|e| format!("Failed to serialize comparison: {}", e))?,
            None => "null".to_string(),
        };

        let conn = self.conn.lock().map_err(|e| format!("Failed to lock connection: {}", e))?;
        
        conn.execute(
            r#"
            INSERT OR REPLACE INTO benchmark_results (
                id, test_name, timestamp, flags_json, metrics_json,
                environment_json, duration_seconds, comparison_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                result.id,
                result.test_name,
                result.timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                flags_json,
                metrics_json,
                environment_json,
                result.duration_seconds,
                comparison_json,
            ],
        ).map_err(|e| format!("Failed to insert result: {}", e))?;

        Ok(())
    }

    fn get_result(&self, id: &str) -> Result<Option<BenchmarkResult>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Failed to lock connection: {}", e))?;
        
        let mut stmt = conn.prepare(
            "SELECT id, test_name, timestamp, flags_json, metrics_json, 
             environment_json, duration_seconds, comparison_json 
             FROM benchmark_results WHERE id = ?",
        ).map_err(|e| format!("Failed to prepare statement: {}", e))?;

        let rows = stmt.query_map(params![id], |row| {
            let id: String = row.get(0)?;
            let test_name: String = row.get(1)?;
            let timestamp_str: String = row.get(2)?;
            let flags_json: String = row.get(3)?;
            let metrics_json: String = row.get(4)?;
            let environment_json: String = row.get(5)?;
            let duration_seconds: i64 = row.get(6)?;
            let comparison_json: String = row.get(7)?;

            let timestamp = DateTime::parse_from_rfc3339(&timestamp_str)
                .map_err(|e| SqliteError::InvalidColumnType(7, format!("Invalid timestamp: {}", e)))?
                .with_timezone(&chrono::Utc);

            let flags: OptimizationFlags = serde_json::from_str(&flags_json)
                .map_err(|e| SqliteError::InvalidColumnType(3, format!("Invalid flags JSON: {}", e)))?;

            let metrics: super::BenchmarkMetrics = serde_json::from_str(&metrics_json)
                .map_err(|e| SqliteError::InvalidColumnType(4, format!("Invalid metrics JSON: {}", e)))?;

            let environment: super::EnvironmentInfo = serde_json::from_str(&environment_json)
                .map_err(|e| SqliteError::InvalidColumnType(5, format!("Invalid environment JSON: {}", e)))?;

            let comparison = if comparison_json == "null" || comparison_json.is_empty() {
                None
            } else {
                Some(serde_json::from_str(&comparison_json)
                    .map_err(|e| SqliteError::InvalidColumnType(7, format!("Invalid comparison JSON: {}", e)))?
                )
            };

            Ok(BenchmarkResult {
                id,
                test_name,
                timestamp,
                flags,
                metrics,
                environment,
                duration_seconds: duration_seconds as u64,
                comparison,
            })
        }).map_err(|e| format!("Failed to query result: {}", e))?;

        let result: Option<BenchmarkResult> = rows.into_iter().next().transpose()
            .map_err(|e| format!("Failed to parse result: {}", e))?;

        Ok(result)
    }

    fn list_results(
        &self,
        limit: usize,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<Vec<BenchmarkResult>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Failed to lock connection: {}", e))?;

        let mut query = String::from(
            "SELECT id, test_name, timestamp, flags_json, metrics_json, 
             environment_json, duration_seconds, comparison_json 
             FROM benchmark_results WHERE 1=1",
        );

        let mut params: Vec<String> = Vec::new();

        if let Some(start) = start_time {
            query.push_str(" AND timestamp >= ?");
            params.push(start.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
        }

        if let Some(end) = end_time {
            query.push_str(" AND timestamp <= ?");
            params.push(end.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
        }

        query.push_str(" ORDER BY timestamp DESC LIMIT ?");

        let mut stmt = conn.prepare(&query)
            .map_err(|e| format!("Failed to prepare statement: {}", e))?;

        let query_params: Vec<&dyn rusqlite::ToSql> = params.iter()
            .map(|s| s as &dyn rusqlite::ToSql)
            .chain(std::iter::once(&(limit as i64) as &dyn rusqlite::ToSql))
            .collect();

        let rows = stmt.query_map(rusqlite::params_from_iter(query_params), |row| {
            let id: String = row.get(0)?;
            let test_name: String = row.get(1)?;
            let timestamp_str: String = row.get(2)?;
            let flags_json: String = row.get(3)?;
            let metrics_json: String = row.get(4)?;
            let environment_json: String = row.get(5)?;
            let duration_seconds: i64 = row.get(6)?;
            let comparison_json: String = row.get(7)?;

            let timestamp = DateTime::parse_from_rfc3339(&timestamp_str)
                .map_err(|e| SqliteError::InvalidColumnType(2, format!("Invalid timestamp: {}", e)))?
                .with_timezone(&chrono::Utc);

            let flags: OptimizationFlags = serde_json::from_str(&flags_json)
                .map_err(|e| SqliteError::InvalidColumnType(3, format!("Invalid flags JSON: {}", e)))?;

            let metrics: super::BenchmarkMetrics = serde_json::from_str(&metrics_json)
                .map_err(|e| SqliteError::InvalidColumnType(4, format!("Invalid metrics JSON: {}", e)))?;

            let environment: super::EnvironmentInfo = serde_json::from_str(&environment_json)
                .map_err(|e| SqliteError::InvalidColumnType(5, format!("Invalid environment JSON: {}", e)))?;

            let comparison = if comparison_json == "null" || comparison_json.is_empty() {
                None
            } else {
                Some(serde_json::from_str(&comparison_json)
                    .map_err(|e| SqliteError::InvalidColumnType(7, format!("Invalid comparison JSON: {}", e)))?
                )
            };

            Ok(BenchmarkResult {
                id,
                test_name,
                timestamp,
                flags,
                metrics,
                environment,
                duration_seconds: duration_seconds as u64,
                comparison,
            })
        }).map_err(|e| format!("Failed to query results: {}", e))?;

        let results: Vec<BenchmarkResult> = rows.into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to parse results: {}", e))?;

        Ok(results)
    }

    fn delete_result(&self, id: &str) -> Result<bool, String> {
        let conn = self.conn.lock().map_err(|e| format!("Failed to lock connection: {}", e))?;
        
        let changes = conn.execute(
            "DELETE FROM benchmark_results WHERE id = ?",
            params![id],
        ).map_err(|e| format!("Failed to delete result: {}", e))?;

        Ok(changes > 0)
    }

    // 额外方法：获取测试名称列表
    pub fn list_test_names(&self) -> Result<Vec<String>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Failed to lock connection: {}", e))?;
        
        let mut stmt = conn.prepare("SELECT DISTINCT test_name FROM benchmark_results ORDER BY test_name")
            .map_err(|e| format!("Failed to prepare statement: {}", e))?;

        let rows = stmt.query_map(NO_PARAMS, |row| {
            let name: String = row.get(0)?;
            Ok(name)
        }).map_err(|e| format!("Failed to query test names: {}", e))?;

        let names: Vec<String> = rows.into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to parse test names: {}", e))?;

        Ok(names)
    }

    // 额外方法：统计各测试名称的结果数量
    pub fn count_by_test_name(&self) -> Result<Vec<(String, i64)>, String> {
        let conn = self.conn.lock().map_err(|e| format!("Failed to lock connection: {}", e))?;
        
        let mut stmt = conn.prepare(
            "SELECT test_name, COUNT(*) as count 
             FROM benchmark_results 
             GROUP BY test_name 
             ORDER BY count DESC",
        ).map_err(|e| format!("Failed to prepare statement: {}", e))?;

        let rows = stmt.query_map(NO_PARAMS, |row| {
            let name: String = row.get(0)?;
            let count: i64 = row.get(1)?;
            Ok((name, count))
        }).map_err(|e| format!("Failed to query counts: {}", e))?;

        let counts: Vec<(String, i64)> = rows.into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to parse counts: {}", e))?;

        Ok(counts)
    }
}

// 测试用例
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_sqlite_storage() {
        // 创建内存数据库
        let storage = SqliteBenchmarkStorage::new_in_memory().unwrap();

        // 创建测试结果
        let flags = OptimizationFlags::default();
        let environment = super::super::EnvironmentInfo {
            cpu_model: "Intel i7".to_string(),
            cpu_cores: 8,
            memory_gb: 16.0,
            node_count: 3,
            os_version: "Linux".to_string(),
            rust_version: "1.76".to_string(),
            powerfs_version: "0.1.0".to_string(),
        };

        let mut result = super::super::BenchmarkResult::new("test_sqlite", flags, environment);
        result.metrics.ec_throughput_mbps = 100.0;
        result.metrics.ec_latency_ms = 2.0;
        result.duration_seconds = 10;

        // 保存结果
        storage.save_result(&result).unwrap();

        // 获取结果
        let retrieved = storage.get_result(&result.id).unwrap().unwrap();
        assert_eq!(retrieved.id, result.id);
        assert_eq!(retrieved.test_name, "test_sqlite");
        assert_eq!(retrieved.metrics.ec_throughput_mbps, 100.0);

        // 列出结果
        let results = storage.list_results(10, None, None).unwrap();
        assert_eq!(results.len(), 1);

        // 删除结果
        let deleted = storage.delete_result(&result.id).unwrap();
        assert!(deleted);

        // 验证已删除
        let not_found = storage.get_result(&result.id).unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_sqlite_list_with_time_range() {
        let storage = SqliteBenchmarkStorage::new_in_memory().unwrap();

        // 创建两个不同时间的测试结果
        let flags = OptimizationFlags::default();
        let environment = super::super::EnvironmentInfo {
            cpu_model: "Intel i7".to_string(),
            cpu_cores: 8,
            memory_gb: 16.0,
            node_count: 3,
            os_version: "Linux".to_string(),
            rust_version: "1.76".to_string(),
            powerfs_version: "0.1.0".to_string(),
        };

        let result1 = super::super::BenchmarkResult::new("test_range", flags.clone(), environment.clone());
        storage.save_result(&result1).unwrap();

        // 等待一小段时间
        std::thread::sleep(std::time::Duration::from_millis(10));

        let result2 = super::super::BenchmarkResult::new("test_range", flags, environment);
        storage.save_result(&result2).unwrap();

        // 列出所有结果
        let results = storage.list_results(10, None, None).unwrap();
        assert_eq!(results.len(), 2);

        // 使用时间范围过滤
        let start_time = result1.timestamp + chrono::Duration::milliseconds(5);
        let filtered = storage.list_results(10, Some(start_time), None).unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, result2.id);
    }

    #[test]
    fn test_sqlite_count_by_test_name() {
        let storage = SqliteBenchmarkStorage::new_in_memory().unwrap();

        let flags = OptimizationFlags::default();
        let environment = super::super::EnvironmentInfo {
            cpu_model: "Intel i7".to_string(),
            cpu_cores: 8,
            memory_gb: 16.0,
            node_count: 3,
            os_version: "Linux".to_string(),
            rust_version: "1.76".to_string(),
            powerfs_version: "0.1.0".to_string(),
        };

        // 添加多个测试结果
        for _ in 0..3 {
            let result = super::super::BenchmarkResult::new("test_a", flags.clone(), environment.clone());
            storage.save_result(&result).unwrap();
        }

        for _ in 0..2 {
            let result = super::super::BenchmarkResult::new("test_b", flags.clone(), environment.clone());
            storage.save_result(&result).unwrap();
        }

        // 统计
        let counts = storage.count_by_test_name().unwrap();
        assert_eq!(counts.len(), 2);
        assert_eq!(counts[0].0, "test_a");
        assert_eq!(counts[0].1, 3);
        assert_eq!(counts[1].0, "test_b");
        assert_eq!(counts[1].1, 2);
    }
}
