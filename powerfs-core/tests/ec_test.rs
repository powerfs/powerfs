use powerfs_core::ec_thread::{EcConfig, EcEncoder, EcThread};

#[tokio::test]
async fn test_ec_encoder_encode_basic() {
    let config = EcConfig::default();
    let encoder = EcEncoder::new(config);

    let data = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
    let shards = encoder.encode(&data);

    assert_eq!(shards.len(), 6);
    assert_eq!(shards[0].len(), 2);
    assert_eq!(shards[1].len(), 2);
    assert_eq!(shards[2].len(), 2);
    assert_eq!(shards[3].len(), 2);
    assert_eq!(shards[4].len(), 2);
    assert_eq!(shards[5].len(), 2);
}

#[tokio::test]
async fn test_ec_encoder_roundtrip() {
    let config = EcConfig::default();
    let encoder = EcEncoder::new(config);

    let data = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
    let shards = encoder.encode(&data);

    let restored = encoder.decode(&shards);

    assert_eq!(restored.len(), 8);
    assert_eq!(restored, data);
}

#[tokio::test]
async fn test_ec_encoder_roundtrip_large_data() {
    let config = EcConfig::default();
    let encoder = EcEncoder::new(config);

    let data: Vec<u8> = (0..1024).map(|i| i as u8).collect();
    let shards = encoder.encode(&data);

    let restored = encoder.decode(&shards);

    assert_eq!(restored.len(), 1024);
    assert_eq!(restored, data);
}

#[tokio::test]
async fn test_ec_encoder_can_recover() {
    let config = EcConfig::default();
    let encoder = EcEncoder::new(config);

    let available_shards = vec![true, true, true, true, false, false];
    assert!(encoder.can_recover(&available_shards));

    let available_shards = vec![true, true, true, false, true, false];
    assert!(encoder.can_recover(&available_shards));

    let available_shards = vec![true, true, false, false, true, true];
    assert!(encoder.can_recover(&available_shards));

    let available_shards = vec![true, true, true, false, false, false];
    assert!(!encoder.can_recover(&available_shards));

    let available_shards = vec![true, true, false, false, false, false];
    assert!(!encoder.can_recover(&available_shards));
}

#[tokio::test]
async fn test_ec_thread_encode() {
    let config = EcConfig::default();
    let ec_thread = EcThread::start(config.clone());

    let data = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
    let result = ec_thread.encode(data, config).await;

    assert!(result.is_ok());
    let shards = result.unwrap();
    assert_eq!(shards.len(), 6);
}

#[tokio::test]
async fn test_ec_thread_decode() {
    let config = EcConfig::default();
    let ec_thread = EcThread::start(config.clone());

    let data = vec![1u8, 2, 3, 4, 5, 6, 7, 8];

    let encoder = EcEncoder::new(config.clone());
    let shards = encoder.encode(&data);

    let result = ec_thread.decode(shards, config).await;

    assert!(result.is_ok());
    let restored = result.unwrap();
    assert_eq!(restored, data);
}

#[tokio::test]
async fn test_ec_thread_roundtrip() {
    let config = EcConfig::default();
    let ec_thread = EcThread::start(config.clone());

    let data = vec![1u8, 2, 3, 4, 5, 6, 7, 8];

    let shards = ec_thread
        .encode(data.clone(), config.clone())
        .await
        .unwrap();

    let restored = ec_thread.decode(shards, config).await.unwrap();

    assert_eq!(restored, data);
}
