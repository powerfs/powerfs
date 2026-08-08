//! powerfs-layout 集成测试
//!
//! 通过公共 API 测试跨模块协作:
//! - FileLayout::for_new_file → codec::encode → codec::decode 完整生命周期
//! - auto_promote 阈值边界
//! - xattr 解析 → 布局决策管线
//! - anti_affinity volume 选择
//! - Placement::locate() 一致性
//! - StripeDescriptor 展开
//! - Placement × Reliability × ChunkEncoding 笛卡尔积

use powerfs_layout::codec::{
    decode_file_layout, encode_file_layout, FEATURE_CHUNK_LAYOUT_V2,
};
use powerfs_layout::placement::auto_promote;
use powerfs_layout::reliability::{CompressionState, Reliability, ReliabilityState};
use powerfs_layout::anti_affinity::{select_volumes_with_anti_affinity, NodeId, VolumeInfo};
use powerfs_layout::encoding::{ChunkEncoding, ChunkRef};
use powerfs_layout::layout::FileLayout;
use powerfs_layout::placement::{Placement, PlacementSpec};
use powerfs_layout::policy::PlacementPolicy;
use powerfs_layout::xattr::{parse_inline_xattr, parse_placement_xattr};
use powerfs_net::{TlvDecoder, TlvEncoder};
use std::collections::HashSet;

// =========================================================================
// 辅助函数
// =========================================================================

/// 编码后解码, 验证往返一致性
fn round_trip(layout: &FileLayout) -> FileLayout {
    let mut enc = TlvEncoder::new();
    encode_file_layout(&mut enc, layout, FEATURE_CHUNK_LAYOUT_V2).unwrap();
    let bytes = enc.into_bytes();
    let mut dec = TlvDecoder::new(&bytes);
    decode_file_layout(&mut dec).unwrap()
}

fn make_chunk(offset: u64, size: u64, needle: u64, vol: u64) -> ChunkRef {
    ChunkRef {
        offset,
        size,
        needle_id: needle,
        volume_id: vol,
        crc32: 0xDEAD_BEEF,
        mtime: 1700000000,
    }
}

fn make_layout(placement: Placement, reliability: Reliability, encoding: ChunkEncoding) -> FileLayout {
    FileLayout {
        placement,
        reliability,
        reliability_state: ReliabilityState::PendingReplicated,
        compression: CompressionState::None,
        encoding,
    }
}

// =========================================================================
// 1. 完整生命周期: for_new_file → encode → decode → verify
// =========================================================================

#[test]
fn lifecycle_inline_file() {
    let policy = PlacementPolicy::default();
    let layout = FileLayout::for_new_file(100, None, None, &policy);
    assert!(layout.is_inline());

    let decoded = round_trip(&layout);
    assert!(decoded.is_inline());
    assert_eq!(decoded.placement, layout.placement);
    assert_eq!(decoded.encoding, layout.encoding);
}

#[test]
fn lifecycle_flat_file() {
    let policy = PlacementPolicy::default();
    let layout = FileLayout::for_new_file(4096, None, None, &policy);
    assert!(matches!(layout.placement, Placement::Flat));

    let decoded = round_trip(&layout);
    assert_eq!(decoded.placement, Placement::Flat);
}

#[test]
fn lifecycle_stripe_file() {
    let policy = PlacementPolicy::default();
    let layout = FileLayout::for_new_file(64 * 1024 * 1024, None, None, &policy);
    assert!(matches!(layout.placement, Placement::Stripe { .. }));

    let decoded = round_trip(&layout);
    match decoded.placement {
        Placement::Stripe { stripe_count, .. } => assert_eq!(stripe_count, 4),
        _ => panic!("expected Stripe"),
    }
}

// =========================================================================
// 2. auto_promote 阈值边界
// =========================================================================

#[test]
fn auto_promote_all_thresholds() {
    let policy = PlacementPolicy::default();

    // < inline_max_size → Inline
    assert!(matches!(
        auto_promote(0, &policy),
        Placement::Inline { .. }
    ));
    assert!(matches!(
        auto_promote(4095, &policy),
        Placement::Inline { .. }
    ));

    // == inline_max_size → Flat (边界: 不小于)
    assert_eq!(auto_promote(4096, &policy), Placement::Flat);

    // < flat_max_size → Flat
    assert_eq!(auto_promote(63 * 1024 * 1024, &policy), Placement::Flat);

    // == flat_max_size → Stripe(4)
    let p = auto_promote(64 * 1024 * 1024, &policy);
    assert!(matches!(p, Placement::Stripe { stripe_count: 4, .. }));

    // < stripe4_max_size → Stripe(4)
    let p = auto_promote(1024 * 1024 * 1024 - 1, &policy);
    assert!(matches!(p, Placement::Stripe { stripe_count: 4, .. }));

    // == stripe4_max_size → Stripe(16)
    let p = auto_promote(1024 * 1024 * 1024, &policy);
    assert!(matches!(p, Placement::Stripe { stripe_count: 16, .. }));

    // < stripe16_max_size → Stripe(16)
    let p = auto_promote(99 * 1024 * 1024 * 1024, &policy);
    assert!(matches!(p, Placement::Stripe { stripe_count: 16, .. }));

    // >= stripe16_max_size, no auto widestripe → Stripe(16)
    let p = auto_promote(200 * 1024 * 1024 * 1024, &policy);
    assert!(matches!(p, Placement::Stripe { stripe_count: 16, .. }));
}

#[test]
fn auto_promote_widestripe_when_enabled() {
    let mut policy = PlacementPolicy::default();
    policy.allow_auto_widestripe = true;

    let p = auto_promote(200 * 1024 * 1024 * 1024, &policy);
    assert!(matches!(p, Placement::WideStripe { .. }));
}

// =========================================================================
// 3. xattr → 布局决策管线
// =========================================================================

#[test]
fn xattr_to_layout_flat() {
    let spec = parse_placement_xattr("flat").unwrap();
    let policy = PlacementPolicy::default();
    // 提供 dir_placement=Flat → spec_to_placement 直接返回 Flat,
    // 绕过 auto_promote 的 inline 决策
    let layout = FileLayout::for_new_file(1024, Some(&spec), None, &policy);
    assert_eq!(layout.placement, Placement::Flat);
}

#[test]
fn xattr_to_layout_flat_with_inline_disabled() {
    let spec = parse_placement_xattr("flat").unwrap();
    let policy = PlacementPolicy::default();
    // inline=0 禁用 inline, 强制走 placement spec
    let layout = FileLayout::for_new_file(1024, Some(&spec), Some(0), &policy);
    assert_eq!(layout.placement, Placement::Flat);
}

#[test]
fn xattr_to_layout_stripe() {
    let spec = parse_placement_xattr("stripe:4:64MB").unwrap();
    assert_eq!(
        spec,
        PlacementSpec::Stripe {
            count: 4,
            stripe_size: 64 * 1024 * 1024,
        }
    );

    let policy = PlacementPolicy::default();
    // 大文件 + stripe xattr → Stripe
    let layout = FileLayout::for_new_file(
        100 * 1024 * 1024,
        Some(&spec),
        Some(0), // 禁用 inline
        &policy,
    );
    match layout.placement {
        Placement::Stripe {
            stripe_count,
            stripe_size,
            ..
        } => {
            assert_eq!(stripe_count, 4);
            assert_eq!(stripe_size, 64 * 1024 * 1024);
        }
        _ => panic!("expected Stripe"),
    }
}

#[test]
fn xattr_to_layout_inline_threshold() {
    // powerfs.inline=8192 → 8KB 以下文件 inline
    let threshold = parse_inline_xattr("8192").unwrap();
    assert_eq!(threshold, Some(8192));

    let policy = PlacementPolicy::default();
    let layout = FileLayout::for_new_file(5000, None, threshold, &policy);
    assert!(layout.is_inline());
    match layout.placement {
        Placement::Inline { max_size } => assert_eq!(max_size, 8192),
        _ => panic!("expected Inline"),
    }
}

#[test]
fn xattr_inline_disabled() {
    // parse_inline_xattr("0") 返回 None (表示未设置阈值)
    let threshold = parse_inline_xattr("0").unwrap();
    assert_eq!(threshold, None);

    let policy = PlacementPolicy::default();
    // 显式传 Some(0) 禁用 inline: 即使 100 字节也不 inline
    let layout = FileLayout::for_new_file(100, None, Some(0), &policy);
    assert!(!layout.is_inline());
}

// =========================================================================
// 4. anti_affinity volume 选择
// =========================================================================

#[test]
fn anti_affinity_basic_selection() {
    let vols = vec![
        VolumeInfo { volume_id: 1, node_id: NodeId(1), free_bytes: 100, total_bytes: 200 },
        VolumeInfo { volume_id: 2, node_id: NodeId(2), free_bytes: 80, total_bytes: 200 },
        VolumeInfo { volume_id: 3, node_id: NodeId(3), free_bytes: 60, total_bytes: 200 },
        VolumeInfo { volume_id: 4, node_id: NodeId(1), free_bytes: 50, total_bytes: 200 },
    ];
    let selected = select_volumes_with_anti_affinity(&vols, 3, &HashSet::new()).unwrap();
    assert_eq!(selected.len(), 3);

    // 验证每个 volume 在不同 node
    let mut nodes = HashSet::new();
    for vid in &selected {
        let vol = vols.iter().find(|v| v.volume_id == *vid).unwrap();
        nodes.insert(vol.node_id.0);
    }
    assert_eq!(nodes.len(), 3);
}

#[test]
fn anti_affinity_insufficient_nodes() {
    let vols = vec![
        VolumeInfo { volume_id: 1, node_id: NodeId(1), free_bytes: 100, total_bytes: 200 },
        VolumeInfo { volume_id: 2, node_id: NodeId(1), free_bytes: 80, total_bytes: 200 },
    ];
    let result = select_volumes_with_anti_affinity(&vols, 2, &HashSet::new());
    assert!(result.is_err());
}

#[test]
fn anti_affinity_with_excluded_nodes() {
    let vols = vec![
        VolumeInfo { volume_id: 1, node_id: NodeId(1), free_bytes: 100, total_bytes: 200 },
        VolumeInfo { volume_id: 2, node_id: NodeId(2), free_bytes: 80, total_bytes: 200 },
        VolumeInfo { volume_id: 3, node_id: NodeId(3), free_bytes: 60, total_bytes: 200 },
    ];
    let mut exclude = HashSet::new();
    exclude.insert(NodeId(1));
    let selected = select_volumes_with_anti_affinity(&vols, 2, &exclude).unwrap();
    // 不应包含 node 1 的 volume
    assert!(!selected.contains(&1));
    assert_eq!(selected.len(), 2);
}

#[test]
fn anti_affinity_prefers_higher_free_ratio() {
    let vols = vec![
        VolumeInfo { volume_id: 1, node_id: NodeId(1), free_bytes: 180, total_bytes: 200 },
        VolumeInfo { volume_id: 2, node_id: NodeId(2), free_bytes: 20, total_bytes: 200 },
    ];
    let selected = select_volumes_with_anti_affinity(&vols, 2, &HashSet::new()).unwrap();
    // vol 1 空闲比更高, 应先选
    assert_eq!(selected[0], 1);
    assert_eq!(selected[1], 2);
}

// =========================================================================
// 5. Placement::locate() 一致性
// =========================================================================

#[test]
fn locate_stripe_continuous_offset() {
    // 验证连续 offset 的 locate 结果连续
    let p = Placement::Stripe {
        stripe_size: 1024,
        stripe_count: 4,
        start_volume_idx: 0,
        volume_ids: vec![10, 20, 30, 40],
    };

    for i in 0..100 {
        let offset = i * 1024;
        let (idx, vol_offset) = p.locate(offset).unwrap();
        let vol_id = p.volume_id_at(idx).unwrap();
        // vol_offset = (i / stripe_count) * stripe_size
        let expected_vol_offset = (i / 4) * 1024;
        assert_eq!(
            vol_offset, expected_vol_offset as u64,
            "offset {} vol_offset mismatch", offset
        );

        // 验证 volume 轮转: 10, 20, 30, 40, 10, 20, ...
        let expected_vol = match i % 4 {
            0 => 10,
            1 => 20,
            2 => 30,
            3 => 40,
            _ => unreachable!(),
        };
        assert_eq!(vol_id, expected_vol, "offset {} should map to vol {}", offset, expected_vol);
    }
}

#[test]
fn locate_stripe_wraps_correctly() {
    // stripe_count=2, 验证 wrap 后 vol_offset 增加 stripe_size
    let p = Placement::Stripe {
        stripe_size: 100,
        stripe_count: 2,
        start_volume_idx: 0,
        volume_ids: vec![1, 2],
    };

    // offset 0 → vol 0, vol_offset 0
    assert_eq!(p.locate(0), Some((0, 0)));
    // offset 100 → vol 1, vol_offset 0
    assert_eq!(p.locate(100), Some((1, 0)));
    // offset 200 → wraps to vol 0, vol_offset 100
    assert_eq!(p.locate(200), Some((0, 100)));
    // offset 300 → wraps to vol 1, vol_offset 100
    assert_eq!(p.locate(300), Some((1, 100)));
    // offset 400 → wraps to vol 0, vol_offset 200
    assert_eq!(p.locate(400), Some((0, 200)));
}

#[test]
fn locate_inline_and_flat_return_none() {
    assert_eq!(Placement::Inline { max_size: 4096 }.locate(0), None);
    assert_eq!(Placement::Flat.locate(0), None);
    assert_eq!(Placement::Flat.locate(99999), None);
}

#[test]
fn locate_wide_stripe_256_volumes() {
    let vol_ids: Vec<u64> = (1..=256).collect();
    let p = Placement::WideStripe {
        stripe_size: 4 * 1024 * 1024,
        stripe_count: 256,
        start_volume_idx: 0,
        volume_ids: vol_ids.clone(),
    };

    // offset 0 → vol 0 (vol_id=1)
    let (idx, vol_offset) = p.locate(0).unwrap();
    assert_eq!(idx, 0);
    assert_eq!(vol_offset, 0);

    // offset 4MB → vol 1 (vol_id=2)
    let (idx, _) = p.locate(4 * 1024 * 1024).unwrap();
    assert_eq!(idx, 1);

    // offset 255*4MB → vol 255 (vol_id=256)
    let (idx, _) = p.locate(255 * 4 * 1024 * 1024).unwrap();
    assert_eq!(idx, 255);

    // offset 256*4MB → wraps to vol 0, vol_offset 4MB
    let (idx, vol_offset) = p.locate(256 * 4 * 1024 * 1024).unwrap();
    assert_eq!(idx, 0);
    assert_eq!(vol_offset, 4 * 1024 * 1024);
}

#[test]
fn file_layout_locate_integration() {
    // FileLayout::locate() 统一入口
    let layout = FileLayout {
        placement: Placement::Stripe {
            stripe_size: 1024,
            stripe_count: 4,
            start_volume_idx: 0,
            volume_ids: vec![10, 20, 30, 40],
        },
        reliability: Reliability::SingleReplica,
        reliability_state: ReliabilityState::PendingReplicated,
        compression: CompressionState::None,
        encoding: ChunkEncoding::PerChunk { chunks: vec![] },
    };

    assert_eq!(layout.locate(0), Some((10, 0)));
    assert_eq!(layout.locate(1024), Some((20, 0)));
    assert_eq!(layout.locate(2048), Some((30, 0)));
    assert_eq!(layout.locate(3072), Some((40, 0)));
}

// =========================================================================
// 6. Placement × Reliability × ChunkEncoding 笛卡尔积
// =========================================================================

#[test]
fn cartesian_product_all_combinations() {
    let placements = vec![
        Placement::Inline { max_size: 4096 },
        Placement::Flat,
        Placement::Stripe {
            stripe_size: 64 * 1024 * 1024,
            stripe_count: 4,
            start_volume_idx: 0,
            volume_ids: vec![1, 2, 3, 4],
        },
        Placement::WideStripe {
            stripe_size: 4 * 1024 * 1024,
            stripe_count: 256,
            start_volume_idx: 0,
            volume_ids: (1..=256).collect(),
        },
    ];

    let reliabilities = vec![
        Reliability::SingleReplica,
        Reliability::Replicated { count: 3 },
        Reliability::EC { data: 4, parity: 2 },
    ];

    let encodings = vec![
        ChunkEncoding::InlineData { data: vec![1, 2, 3] },
        ChunkEncoding::PerChunk {
            chunks: vec![make_chunk(0, 1024, 42, 10)],
        },
        ChunkEncoding::StripeDescriptor {
            start_needle_id: 100,
            chunk_size: 2 * 1024 * 1024,
            chunk_count: 4,
            volume_ids: vec![1, 2, 3, 4],
            start_volume_idx: 0,
        },
        ChunkEncoding::Paginated {
            chunks: vec![make_chunk(0, 1024, 42, 10)],
            total_count: 100,
            has_more: true,
            next_offset: 1024,
        },
    ];

    let mut tested = 0;
    for placement in &placements {
        for reliability in &reliabilities {
            for encoding in &encodings {
                let layout = make_layout(
                    placement.clone(),
                    reliability.clone(),
                    encoding.clone(),
                );
                let decoded = round_trip(&layout);
                assert_eq!(decoded.placement, *placement, "placement mismatch");
                assert_eq!(decoded.reliability, *reliability, "reliability mismatch");
                assert_eq!(decoded.encoding, *encoding, "encoding mismatch");
                tested += 1;
            }
        }
    }
    // 4 * 3 * 4 = 48 组合
    assert_eq!(tested, 48, "should test all 48 combinations");
}

// =========================================================================
// 7. StripeDescriptor 展开
// =========================================================================

#[test]
fn stripe_descriptor_expand_to_perchunk() {
    let encoding = ChunkEncoding::StripeDescriptor {
        start_needle_id: 100,
        chunk_size: 1024,
        chunk_count: 8,
        volume_ids: vec![10, 20],
        start_volume_idx: 0,
    };
    let expanded = encoding.expand_to_perchunk().unwrap();
    match expanded {
        ChunkEncoding::PerChunk { chunks } => {
            assert_eq!(chunks.len(), 8);
            // 验证 needle_id 连续递增
            for (i, chunk) in chunks.iter().enumerate() {
                assert_eq!(chunk.needle_id, 100 + i as u64);
                assert_eq!(chunk.offset, i as u64 * 1024);
                assert_eq!(chunk.size, 1024);
            }
            // 验证 volume 轮转: vol 10, 20, 10, 20, ...
            assert_eq!(chunks[0].volume_id, 10);
            assert_eq!(chunks[1].volume_id, 20);
            assert_eq!(chunks[2].volume_id, 10);
            assert_eq!(chunks[3].volume_id, 20);
        }
        _ => panic!("expected PerChunk"),
    }
}

#[test]
fn stripe_descriptor_expand_with_start_offset() {
    let encoding = ChunkEncoding::StripeDescriptor {
        start_needle_id: 500,
        chunk_size: 4096,
        chunk_count: 4,
        volume_ids: vec![1, 2, 3, 4],
        start_volume_idx: 2, // 从 vol 3 开始
    };
    let expanded = encoding.expand_to_perchunk().unwrap();
    match expanded {
        ChunkEncoding::PerChunk { chunks } => {
            assert_eq!(chunks[0].volume_id, 3); // (2+0)%4=2 → vol[2]=3
            assert_eq!(chunks[1].volume_id, 4); // (2+1)%4=3 → vol[3]=4
            assert_eq!(chunks[2].volume_id, 1); // (2+2)%4=0 → vol[0]=1
            assert_eq!(chunks[3].volume_id, 2); // (2+3)%4=1 → vol[1]=2
        }
        _ => panic!("expected PerChunk"),
    }
}

#[test]
fn per_chunk_expand_is_identity() {
    let encoding = ChunkEncoding::PerChunk {
        chunks: vec![make_chunk(0, 1024, 42, 10)],
    };
    let expanded = encoding.expand_to_perchunk().unwrap();
    assert_eq!(expanded, encoding);
}

// =========================================================================
// 8. for_new_file 决策逻辑
// =========================================================================

#[test]
fn for_new_file_inline_takes_priority() {
    // 即使有 placement spec, inline 阈值优先
    let spec = PlacementSpec::Stripe { count: 4, stripe_size: 64 * 1024 * 1024 };
    let policy = PlacementPolicy::default();
    let layout = FileLayout::for_new_file(100, Some(&spec), Some(4096), &policy);
    // 100 < 4096 → Inline
    assert!(layout.is_inline());
}

#[test]
fn for_new_file_inline_overrides_auto_promote() {
    // auto_promote 默认 4KB inline, 但 xattr 设为 8KB
    let policy = PlacementPolicy::default();
    let layout = FileLayout::for_new_file(6000, None, Some(8192), &policy);
    // 6000 < 8192 → Inline (即使 policy 默认 4096)
    assert!(layout.is_inline());
    match layout.placement {
        Placement::Inline { max_size } => assert_eq!(max_size, 8192),
        _ => panic!("expected Inline with max_size=8192"),
    }
}

#[test]
fn for_new_file_inline_uses_policy_default() {
    let policy = PlacementPolicy::default();
    let layout = FileLayout::for_new_file(100, None, None, &policy);
    // 无 xattr, 用 policy 默认 4096
    match layout.placement {
        Placement::Inline { max_size } => assert_eq!(max_size, 4096),
        _ => panic!("expected Inline"),
    }
}

// =========================================================================
// 9. 编码大小对比 (二进制 vs JSON)
// =========================================================================

#[test]
fn binary_vs_json_size_per_chunk() {
    // 100 个 chunk: 二进制 vs JSON 大小对比
    let chunks: Vec<ChunkRef> = (0..100)
        .map(|i| make_chunk(i * 4096, 4096, 1000 + i, 10 + (i % 4)))
        .collect();

    // 二进制编码
    let layout_bin = make_layout(
        Placement::Flat,
        Reliability::SingleReplica,
        ChunkEncoding::PerChunk { chunks: chunks.clone() },
    );
    let mut enc = TlvEncoder::new();
    encode_file_layout(&mut enc, &layout_bin, FEATURE_CHUNK_LAYOUT_V2).unwrap();
    let bin_size = enc.into_bytes().len();

    // JSON 编码 (仅 chunks 部分大小)
    let json_size = serde_json::to_vec(&chunks).unwrap().len();

    // 二进制应远小于 JSON
    // 100 chunks: 二进制 = ~4400 bytes (100*44 + overhead)
    // JSON: ~8000+ bytes
    assert!(
        bin_size < json_size,
        "binary {} should be < json {}",
        bin_size,
        json_size
    );
}

#[test]
fn binary_vs_json_size_stripe_descriptor() {
    // 512 chunk StripeDescriptor: 二进制 vs JSON 等价
    let vol_ids: Vec<u64> = (1..=4).collect();
    let layout = make_layout(
        Placement::Stripe {
            stripe_size: 64 * 1024 * 1024,
            stripe_count: 4,
            start_volume_idx: 0,
            volume_ids: vol_ids.clone(),
        },
        Reliability::SingleReplica,
        ChunkEncoding::StripeDescriptor {
            start_needle_id: 1000,
            chunk_size: 2 * 1024 * 1024,
            chunk_count: 512,
            volume_ids: vol_ids.clone(),
            start_volume_idx: 0,
        },
    );

    let mut enc = TlvEncoder::new();
    encode_file_layout(&mut enc, &layout, FEATURE_CHUNK_LAYOUT_V2).unwrap();
    let bin_size = enc.into_bytes().len();

    // JSON 等价: 512 chunks * ~80 bytes/chunk = ~40KB
    let json_equiv: Vec<ChunkRef> = (0..512usize)
        .map(|i| make_chunk(
            (i as u64) * 2 * 1024 * 1024,
            2 * 1024 * 1024,
            1000 + i as u64,
            vol_ids[i % 4],
        ))
        .collect();
    let json_size = serde_json::to_vec(&json_equiv).unwrap().len();

    // 二进制应远小于 JSON (压缩比 > 100x)
    let ratio = json_size as f64 / bin_size as f64;
    assert!(
        ratio > 100.0,
        "compression ratio {} should be > 100x (binary={}, json={})",
        ratio,
        bin_size,
        json_size
    );
}

// =========================================================================
// 10. ChunkEncoding 辅助方法
// =========================================================================

#[test]
fn chunk_encoding_total_size() {
    let inline = ChunkEncoding::InlineData { data: vec![1, 2, 3, 4] };
    assert_eq!(inline.total_size(), 4);

    let perchunk = ChunkEncoding::PerChunk {
        chunks: vec![
            make_chunk(0, 1024, 1, 10),
            make_chunk(1024, 2048, 2, 10),
        ],
    };
    assert_eq!(perchunk.total_size(), 3072);

    let stripe_desc = ChunkEncoding::StripeDescriptor {
        start_needle_id: 0,
        chunk_size: 2 * 1024 * 1024,
        chunk_count: 512,
        volume_ids: vec![1, 2, 3, 4],
        start_volume_idx: 0,
    };
    assert_eq!(stripe_desc.total_size(), 512 * 2 * 1024 * 1024);
}

#[test]
fn chunk_encoding_chunk_count() {
    let inline = ChunkEncoding::InlineData { data: vec![1] };
    assert_eq!(inline.chunk_count(), 0);

    let perchunk = ChunkEncoding::PerChunk {
        chunks: vec![make_chunk(0, 1024, 1, 10); 5],
    };
    assert_eq!(perchunk.chunk_count(), 5);

    let stripe_desc = ChunkEncoding::StripeDescriptor {
        start_needle_id: 0,
        chunk_size: 1024,
        chunk_count: 100,
        volume_ids: vec![1, 2],
        start_volume_idx: 0,
    };
    assert_eq!(stripe_desc.chunk_count(), 100);
}

// =========================================================================
// 11. Placement validate
// =========================================================================

#[test]
fn placement_validate_all_variants() {
    assert!(Placement::Inline { max_size: 4096 }.validate().is_ok());
    assert!(Placement::Inline { max_size: 8192 }.validate().is_ok());
    assert!(Placement::Inline { max_size: 0 }.validate().is_err());
    assert!(Placement::Inline { max_size: 16384 }.validate().is_err()); // > 8KB

    assert!(Placement::Flat.validate().is_ok());

    assert!(Placement::Stripe {
        stripe_size: 64 * 1024 * 1024,
        stripe_count: 4,
        start_volume_idx: 0,
        volume_ids: vec![1, 2, 3, 4],
    }
    .validate()
    .is_ok());

    assert!(Placement::Stripe {
        stripe_size: 64,
        stripe_count: 0, // 非法
        start_volume_idx: 0,
        volume_ids: vec![1, 2],
    }
    .validate()
    .is_err());

    assert!(Placement::Stripe {
        stripe_size: 64,
        stripe_count: 4,
        start_volume_idx: 0,
        volume_ids: vec![], // 非法
    }
    .validate()
    .is_err());
}

// =========================================================================
// 12. Reliability 辅助方法
// =========================================================================

#[test]
fn reliability_overhead_and_survival() {
    // SingleReplica
    let r = Reliability::SingleReplica;
    assert_eq!(r.overhead_factor(), 1.0);
    assert_eq!(r.min_survivable_failures(), 0);

    // Replicated(3)
    let r = Reliability::Replicated { count: 3 };
    assert_eq!(r.overhead_factor(), 3.0);
    assert_eq!(r.min_survivable_failures(), 2);

    // EC(4+2)
    let r = Reliability::EC { data: 4, parity: 2 };
    assert_eq!(r.overhead_factor(), 1.5);
    assert_eq!(r.min_survivable_failures(), 2);

    // EC(8+4)
    let r = Reliability::EC { data: 8, parity: 4 };
    assert_eq!(r.overhead_factor(), 1.5);
    assert_eq!(r.min_survivable_failures(), 4);
}

// =========================================================================
// 13. 二进制 TLV 编码验证 (features 无关, 始终二进制)
// =========================================================================

#[test]
fn encode_always_uses_binary_tlv() {
    let layout = make_layout(
        Placement::Flat,
        Reliability::SingleReplica,
        ChunkEncoding::PerChunk {
            chunks: vec![make_chunk(0, 1024, 42, 10)],
        },
    );

    // 无论 features 为何值, 始终输出二进制 TLV
    for features in [0u32, FEATURE_CHUNK_LAYOUT_V2] {
        let mut enc = TlvEncoder::new();
        encode_file_layout(&mut enc, &layout, features).unwrap();
        let bytes = enc.into_bytes();

        let mut dec = TlvDecoder::new(&bytes);
        let mut found_chunk_layout = false;
        let mut found_json_chunks = false;
        while let Some((field, length)) = dec.next_field() {
            match field {
                powerfs_net::FieldId::ChunkLayout => found_chunk_layout = true,
                powerfs_net::FieldId::Chunks => found_json_chunks = true,
                _ => {}
            }
            dec.skip(length).unwrap();
        }
        assert!(
            found_chunk_layout,
            "features={}: should have FieldId::ChunkLayout",
            features
        );
        assert!(
            !found_json_chunks,
            "features={}: should NOT have FieldId::Chunks (JSON path removed)",
            features
        );
    }
}
