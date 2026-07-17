use powerfs_core::config::{Config, ConfigError, LogFormat, NodeType};
use powerfs_core::storage_backend::{BackendConfigDetails, BackendFactory};
use tempfile::NamedTempFile;

#[test]
fn test_config_default() {
    let config = Config::default();

    assert_eq!(config.node.node_id, "node-0");
    assert_eq!(config.node.node_type, NodeType::Volume);
    assert_eq!(config.storage.checksum_algorithm.to_string(), "crc32c");
    assert_eq!(config.network.grpc_port, 8080);
    assert_eq!(config.reliability.ec_data_shards, 8);
    assert_eq!(config.reliability.ec_parity_shards, 4);
}

#[test]
fn test_config_validate_default() {
    let config = Config::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_validate_empty_node_id() {
    let mut config = Config::default();
    config.node.node_id = "".to_string();
    assert!(
        matches!(config.validate(), Err(ConfigError::ValidationError(e)) if e == "node_id is required")
    );
}

#[test]
fn test_config_to_yaml() {
    let config = Config::default();
    let yaml = config.to_yaml();
    assert!(yaml.is_ok());
    let yaml_str = yaml.unwrap();
    assert!(yaml_str.contains("node_id"));
    assert!(yaml_str.contains("backend_type"));
}

#[test]
fn test_config_save_and_load() {
    let config = Config::default();

    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    config.save_to_file(path).unwrap();

    let loaded_config = Config::load_from_file(path).unwrap();

    assert_eq!(loaded_config.node.node_id, config.node.node_id);
    assert_eq!(
        loaded_config.storage.checksum_algorithm,
        config.storage.checksum_algorithm
    );
    assert_eq!(loaded_config.network.grpc_port, config.network.grpc_port);
}

#[test]
fn test_config_load_or_default() {
    let config = Config::load_or_default("/nonexistent/config.yaml");
    assert_eq!(config.node.node_id, "node-0");
}

#[test]
fn test_config_with_local_file_backend() {
    let yaml = r#"
node:
  node_id: "volume-001"
  node_type: "Volume"
  zone: "zone-a"

storage:
  backend:
    backend_type: "LocalFile"
    node_id: "volume-001"
    type: "local_file"
    data_dir: "/tmp/test_data"
    devices:
      - name: "test_device"
        total_capacity: 107374182400
  checksum_algorithm: "Crc32c"
  volume_size_gib: 100

network:
  grpc_port: 8080
  master_addresses:
    - "http://localhost:9333"

reliability:
  ec_data_shards: 8
  ec_parity_shards: 4

performance:
  ec_thread_count: 8

logging:
  level: "info"
  format: "Text"
"#;

    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.node.node_id, "volume-001");
    assert_eq!(config.storage.backend.backend_type.to_string(), "LocalFile");
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_with_spdk_backend() {
    let yaml = r#"
node:
  node_id: "volume-001"
  node_type: "Volume"
  zone: "zone-a"

storage:
  backend:
    backend_type: "Spdk"
    node_id: "volume-001"
    type: "spdk"
    devices:
      - name: "nvme0"
        transport_string: "trtype:tcp traddr:127.0.0.1 trsvcid:4420"
  checksum_algorithm: "Crc32c"

network:
  grpc_port: 8080

reliability:
  ec_data_shards: 8
  ec_parity_shards: 4

logging:
  level: "info"
  format: "Json"
"#;

    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.node.node_id, "volume-001");
    assert_eq!(config.storage.backend.backend_type.to_string(), "Spdk");
    assert_eq!(config.logging.format, LogFormat::Json);
}

#[test]
fn test_backend_factory_from_config() {
    let temp_dir = tempfile::tempdir().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    let mut config = Config::default();
    if let BackendConfigDetails::LocalFile(ref mut cfg) = config.storage.backend.config {
        cfg.data_dir = data_dir.to_string();
    }

    let backend = BackendFactory::create_from_config(&config);
    assert!(backend.is_ok());
}

#[test]
fn test_config_validation_missing_device() {
    let yaml = r#"
node:
  node_id: "volume-001"
  node_type: "Volume"
  zone: "zone-a"

storage:
  backend:
    backend_type: "LocalFile"
    node_id: "volume-001"
    type: "local_file"
    data_dir: "/tmp/test_data"
    devices: []

network:
  grpc_port: 8080

reliability:
  ec_data_shards: 8
  ec_parity_shards: 4
"#;

    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert!(
        matches!(config.validate(), Err(ConfigError::ValidationError(e)) if e.contains("at least one device"))
    );
}
