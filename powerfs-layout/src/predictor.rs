//! LayoutPredictor — 布局预测器 (Phase 1: 规则驱动)
//!
//! 设计文档: `docs/file-layout-prediction-design.md` §3.2 / §3.3
//!
//! 核心思路: 创建时空文件为 `StorageMode::Empty`, 第一次 write 时
//! 由 `LayoutPredictor::predict()` 根据文件名/路径/扩展名等特征决定
//! 实际布局 (Inline/Flat/Stripe), 避免初始布局误判导致的 Inline→Flat
//! 运行时迁移.

use crate::placement::{Placement, PlacementSpec};
use crate::policy::PlacementPolicy;

/// 客户端类型 (影响预测信号强度)
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ClientType {
    /// FUSE 内核客户端 (强信号: 用户真实工作负载)
    #[default]
    FuseKernel,
    /// FUSE 用户态客户端
    FuseUserspace,
    /// S3 网关
    S3,
    /// 未知/其他
    Other,
}

/// 布局预测输入
#[derive(Clone, Debug, Default)]
pub struct PredictContext {
    /// 文件名 (扩展名是强信号)
    pub filename: String,
    /// 父目录路径 (目录上下文是弱信号)
    pub parent_path: String,
    /// 父目录 placement xattr (显式策略, 最高优先级)
    pub dir_placement: Option<PlacementSpec>,
    /// 父目录 inline 阈值 (powerfs.inline xattr)
    pub dir_inline_threshold: Option<u32>,
    /// 创建标志 (O_CREAT, O_TRUNC, O_APPEND)
    pub create_flags: u32,
    /// 客户端类型
    pub client_type: ClientType,
    /// 首次写入大小 (open 时已知, 如 O_CREAT + 立即 write)
    pub initial_write_size: Option<u64>,
}

/// 布局预测结果 + 置信度
#[derive(Clone, Debug)]
pub struct PredictResult {
    /// 预测的 Placement
    pub placement: Placement,
    /// 置信度 [0.0, 1.0]
    pub confidence: f32,
    /// 命中的规则名 (用于日志/统计)
    pub rule_name: String,
}

impl PredictResult {
    /// 构造一个低置信度的 DeferToPolicy 结果
    /// (调用方应回退到 auto_promote)
    pub fn defer() -> Self {
        Self {
            placement: Placement::Flat,
            confidence: 0.0,
            rule_name: "defer".to_string(),
        }
    }

    /// 是否为高置信度预测 (>= min_confidence)
    pub fn is_confident(&self, min_confidence: f32) -> bool {
        self.confidence >= min_confidence
    }
}

/// 布局预测器 trait (可插拔)
pub trait LayoutPredictor: Send + Sync {
    /// 根据上下文预测文件布局
    fn predict(&self, ctx: &PredictContext) -> PredictResult;
}

// ---------------------------------------------------------------------------
// RuleBasedPredictor — 规则驱动预测器
// ---------------------------------------------------------------------------

/// 规则匹配器
#[derive(Clone, Debug)]
pub enum RuleMatcher {
    /// 扩展名匹配 (大小写不敏感, 含前导点)
    Extension { exts: Vec<String> },
    /// 文件名 glob (支持 * 和 ?)
    FilenameGlob { pattern: String },
    /// 路径前缀匹配
    PathPrefix { prefix: String },
    /// 父目录名匹配
    ParentDir { names: Vec<String> },
    /// 大小区间匹配 (闭区间)
    SizeRange { min: u64, max: u64 },
    /// 客户端类型匹配
    ClientType { ctype: ClientType },
    /// 组合条件 (AND)
    All { matchers: Vec<RuleMatcher> },
    /// 组合条件 (OR)
    Any { matchers: Vec<RuleMatcher> },
}

impl RuleMatcher {
    /// 测试是否匹配给定上下文
    pub fn matches(&self, ctx: &PredictContext) -> bool {
        match self {
            Self::Extension { exts } => {
                let file_ext = Self::extract_extension(&ctx.filename);
                file_ext
                    .as_ref()
                    .map(|e| exts.iter().any(|x| x.eq_ignore_ascii_case(e)))
                    .unwrap_or(false)
            }
            Self::FilenameGlob { pattern } => Self::glob_match(pattern, &ctx.filename),
            Self::PathPrefix { prefix } => ctx.parent_path.starts_with(prefix),
            Self::ParentDir { names } => {
                let parent_name = Self::extract_basename(&ctx.parent_path);
                parent_name
                    .as_ref()
                    .map(|p| names.iter().any(|n| n.eq_ignore_ascii_case(p)))
                    .unwrap_or(false)
            }
            Self::SizeRange { min, max } => ctx
                .initial_write_size
                .map(|s| s >= *min && s <= *max)
                .unwrap_or(false),
            Self::ClientType { ctype } => ctx.client_type == *ctype,
            Self::All { matchers } => matchers.iter().all(|m| m.matches(ctx)),
            Self::Any { matchers } => matchers.iter().any(|m| m.matches(ctx)),
        }
    }

    fn extract_extension(filename: &str) -> Option<String> {
        let dot = filename.rfind('.')?;
        // 排除 .bashrc 这类隐藏文件
        if dot == 0 {
            return None;
        }
        let ext = &filename[dot..];
        if ext.len() > 10 {
            return None; // 过长的"扩展名"可能是文件名的一部分
        }
        Some(ext.to_lowercase())
    }

    fn extract_basename(path: &str) -> Option<String> {
        let trimmed = path.trim_end_matches('/');
        let last_slash = trimmed.rfind('/')?;
        let name = &trimmed[last_slash + 1..];
        if name.is_empty() {
            None
        } else {
            Some(name.to_string())
        }
    }

    /// 简单 glob 匹配 (支持 * 和 ?)
    fn glob_match(pattern: &str, text: &str) -> bool {
        Self::glob_helper(pattern.as_bytes(), text.as_bytes())
    }

    fn glob_helper(pattern: &[u8], text: &[u8]) -> bool {
        let mut pi = 0;
        let mut ti = 0;
        let mut star_pi = None;
        let mut star_ti = 0;

        while ti < text.len() {
            if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
                pi += 1;
                ti += 1;
            } else if pi < pattern.len() && pattern[pi] == b'*' {
                star_pi = Some(pi);
                star_ti = ti;
                pi += 1;
            } else if let Some(sp) = star_pi {
                pi = sp + 1;
                star_ti += 1;
                ti = star_ti;
            } else {
                return false;
            }
        }

        while pi < pattern.len() && pattern[pi] == b'*' {
            pi += 1;
        }

        pi == pattern.len()
    }
}

/// 规则预测布局 (轻量, 不含完整 Stripe 参数)
#[derive(Clone, Debug)]
pub enum RulePlacement {
    Inline { max_size: u32 },
    Flat,
    Stripe { stripe_count: u32, stripe_size: u64 },
}

impl RulePlacement {
    /// 转换为实际 Placement (填充默认参数)
    ///
    /// 注: `policy` 参数保留供后续 Phase 2 使用 (例如根据策略动态调整 stripe_size),
    /// Phase 1 中规则自带完整参数, 暂不使用 policy.
    pub fn to_placement(&self, _policy: &PlacementPolicy) -> Placement {
        match self {
            Self::Inline { max_size } => Placement::Inline {
                max_size: *max_size,
            },
            Self::Flat => Placement::Flat,
            Self::Stripe {
                stripe_count,
                stripe_size,
            } => Placement::Stripe {
                stripe_size: *stripe_size,
                stripe_count: *stripe_count,
                start_volume_idx: 0,
                volume_ids: Vec::new(),
            },
        }
    }
}

/// 布局规则
#[derive(Clone, Debug)]
pub struct LayoutRule {
    /// 规则名
    pub name: String,
    /// 匹配条件
    pub matcher: RuleMatcher,
    /// 预测布局
    pub placement: RulePlacement,
    /// 置信度 (匹配后赋予的置信度)
    pub confidence: f32,
    /// 优先级 (高优先级先匹配)
    pub priority: u32,
}

/// 规则驱动预测器
pub struct RuleBasedPredictor {
    /// 规则列表 (按 priority 降序排列)
    rules: Vec<LayoutRule>,
    /// 全局策略 (用于 to_placement 转换)
    policy: PlacementPolicy,
}

impl RuleBasedPredictor {
    /// 创建预测器并加载默认规则集
    pub fn with_defaults(policy: PlacementPolicy) -> Self {
        Self {
            rules: default_rules(),
            policy,
        }
    }

    /// 创建预测器并使用自定义规则
    pub fn new(policy: PlacementPolicy, rules: Vec<LayoutRule>) -> Self {
        let mut predictor = Self {
            rules,
            policy,
        };
        // 按 priority 降序排列
        predictor.rules.sort_by_key(|a| std::cmp::Reverse(a.priority));
        predictor
    }

    /// 追加规则 (会重新排序)
    pub fn add_rule(&mut self, rule: LayoutRule) {
        self.rules.push(rule);
        self.rules.sort_by_key(|a| std::cmp::Reverse(a.priority));
    }
}

impl LayoutPredictor for RuleBasedPredictor {
    fn predict(&self, ctx: &PredictContext) -> PredictResult {
        // Step 1: 显式 xattr 覆盖 (最高优先级)
        if let Some(spec) = &ctx.dir_placement {
            let placement = match spec {
                PlacementSpec::Flat => Placement::Flat,
                PlacementSpec::Stripe { count, stripe_size } => Placement::Stripe {
                    stripe_size: *stripe_size,
                    stripe_count: *count,
                    start_volume_idx: 0,
                    volume_ids: Vec::new(),
                },
                PlacementSpec::WideStripe {
                    count,
                    stripe_size,
                } => Placement::WideStripe {
                    stripe_size: *stripe_size,
                    stripe_count: *count,
                    start_volume_idx: 0,
                    volume_ids: Vec::new(),
                },
            };
            return PredictResult {
                placement,
                confidence: 1.0,
                rule_name: "dir_xattr".to_string(),
            };
        }

        // Step 2: 父目录 inline 阈值
        if let Some(threshold) = ctx.dir_inline_threshold {
            if threshold > 0
                && ctx
                    .initial_write_size
                    .map(|s| s < threshold as u64)
                    .unwrap_or(true)
            {
                return PredictResult {
                    placement: Placement::Inline {
                        max_size: threshold,
                    },
                    confidence: 0.9,
                    rule_name: "dir_inline_threshold".to_string(),
                };
            }
        }

        // Step 3: 规则匹配 (按 priority 降序)
        for rule in &self.rules {
            if rule.matcher.matches(ctx) {
                let placement = rule.placement.to_placement(&self.policy);
                return PredictResult {
                    placement,
                    confidence: rule.confidence,
                    rule_name: rule.name.clone(),
                };
            }
        }

        // Step 4: 无规则命中, 返回低置信度 DeferToPolicy
        PredictResult::defer()
    }
}

// ---------------------------------------------------------------------------
// 默认规则集
// ---------------------------------------------------------------------------

/// 默认规则集 (按业务常见文件类型)
///
/// 优先级约定:
///   200+  : 基准测试专用规则 (IO500)
///   100   : 深度学习/大文件
///   50    : 中等优先级 (可执行/媒体)
///   10    : 低优先级兜底
fn default_rules() -> Vec<LayoutRule> {
    let stripe4 = RulePlacement::Stripe {
        stripe_count: 4,
        stripe_size: 64 * 1024 * 1024,
    };
    let stripe16 = RulePlacement::Stripe {
        stripe_count: 16,
        stripe_size: 64 * 1024 * 1024,
    };

    vec![
        // --- IO500 专用规则 (优先级 200) ---
        LayoutRule {
            name: "io500_mdtest".to_string(),
            matcher: RuleMatcher::FilenameGlob {
                pattern: "mdtest*".to_string(),
            },
            placement: RulePlacement::Inline { max_size: 4096 },
            confidence: 0.95,
            priority: 200,
        },
        LayoutRule {
            name: "io500_ior".to_string(),
            matcher: RuleMatcher::FilenameGlob {
                pattern: "ior*".to_string(),
            },
            placement: stripe4.clone(),
            confidence: 0.95,
            priority: 200,
        },
        LayoutRule {
            name: "io500_stonewall".to_string(),
            matcher: RuleMatcher::FilenameGlob {
                pattern: "stonewfile*".to_string(),
            },
            placement: stripe4.clone(),
            confidence: 0.85,
            priority: 200,
        },
        // --- 深度学习模型 (优先级 100) ---
        // 注: .bin 不纳入 (过于泛化), ML 场景可通过 ParentDir + Extension 组合规则精配
        LayoutRule {
            name: "ml_checkpoints".to_string(),
            matcher: RuleMatcher::Extension {
                exts: [".pt", ".pth", ".ckpt", ".safetensors"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            placement: stripe16.clone(),
            confidence: 0.9,
            priority: 100,
        },
        LayoutRule {
            name: "ml_datasets".to_string(),
            matcher: RuleMatcher::Extension {
                exts: [".h5", ".npy", ".npz", ".parquet", ".arrow"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            placement: stripe4.clone(),
            confidence: 0.8,
            priority: 100,
        },
        // --- 媒体文件 (优先级 50) ---
        LayoutRule {
            name: "video_files".to_string(),
            matcher: RuleMatcher::Extension {
                exts: [".mp4", ".mkv", ".avi", ".mov", ".flv", ".webm"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            placement: stripe4.clone(),
            confidence: 0.85,
            priority: 50,
        },
        LayoutRule {
            name: "audio_files".to_string(),
            matcher: RuleMatcher::Extension {
                exts: [".mp3", ".flac", ".wav", ".aac", ".ogg"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            placement: RulePlacement::Flat,
            confidence: 0.7,
            priority: 50,
        },
        // --- 压缩包 (优先级 50) ---
        LayoutRule {
            name: "archives".to_string(),
            matcher: RuleMatcher::Extension {
                exts: [
                    ".tar", ".gz", ".zip", ".bz2", ".xz", ".7z", ".tgz", ".tbz",
                ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            },
            placement: stripe4.clone(),
            confidence: 0.75,
            priority: 50,
        },
        // --- 可执行/库文件 (优先级 30) ---
        LayoutRule {
            name: "executables".to_string(),
            matcher: RuleMatcher::Extension {
                exts: [".so", ".o", ".a", ".exe", ".dll", ".dylib"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            placement: RulePlacement::Flat,
            confidence: 0.7,
            priority: 30,
        },
        LayoutRule {
            name: "binaries".to_string(),
            matcher: RuleMatcher::Extension {
                exts: [".bin", ".run"].iter().map(|s| s.to_string()).collect(),
            },
            placement: RulePlacement::Flat,
            confidence: 0.6,
            priority: 30,
        },
        // --- 数据库 (优先级 30) ---
        LayoutRule {
            name: "databases".to_string(),
            matcher: RuleMatcher::Extension {
                exts: [".db", ".sqlite", ".sqlite3", ".mdb", ".bak"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            placement: RulePlacement::Flat,
            confidence: 0.7,
            priority: 30,
        },
        // --- 日志文件 (优先级 20, 增长型) ---
        LayoutRule {
            name: "logs".to_string(),
            matcher: RuleMatcher::Extension {
                exts: [".log", ".out", ".err"].iter().map(|s| s.to_string()).collect(),
            },
            placement: RulePlacement::Flat,
            confidence: 0.6,
            priority: 20,
        },
        // --- 配置/脚本文件 (优先级 10, 低优先级兜底) ---
        LayoutRule {
            name: "config_files".to_string(),
            matcher: RuleMatcher::Extension {
                exts: [
                    ".conf", ".ini", ".cfg", ".toml", ".yaml", ".yml", ".json", ".xml",
                    ".properties",
                ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            },
            placement: RulePlacement::Inline { max_size: 4096 },
            confidence: 0.8,
            priority: 10,
        },
        LayoutRule {
            name: "scripts".to_string(),
            matcher: RuleMatcher::Extension {
                exts: [".sh", ".py", ".rb", ".pl", ".lua", ".js", ".ts"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            placement: RulePlacement::Inline { max_size: 4096 },
            confidence: 0.75,
            priority: 10,
        },
        LayoutRule {
            name: "text_files".to_string(),
            matcher: RuleMatcher::Extension {
                exts: [".txt", ".md", ".rst", ".tex"].iter().map(|s| s.to_string()).collect(),
            },
            placement: RulePlacement::Inline { max_size: 4096 },
            confidence: 0.65,
            priority: 10,
        },
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageMode;

    fn ctx(name: &str, path: &str) -> PredictContext {
        PredictContext {
            filename: name.to_string(),
            parent_path: path.to_string(),
            ..Default::default()
        }
    }

    fn ctx_with_size(name: &str, path: &str, size: u64) -> PredictContext {
        PredictContext {
            filename: name.to_string(),
            parent_path: path.to_string(),
            initial_write_size: Some(size),
            ..Default::default()
        }
    }

    #[test]
    fn glob_basic() {
        assert!(RuleMatcher::glob_match("ior*", "ior_easy"));
        assert!(RuleMatcher::glob_match("ior*", "ior"));
        assert!(!RuleMatcher::glob_match("ior*", "mdtest"));
        assert!(RuleMatcher::glob_match("mdtest*", "mdtest_hard"));
        assert!(RuleMatcher::glob_match("file?.txt", "file1.txt"));
        assert!(!RuleMatcher::glob_match("file?.txt", "file12.txt"));
    }

    #[test]
    fn extension_extraction() {
        assert_eq!(
            RuleMatcher::extract_extension("config.toml"),
            Some(".toml".to_string())
        );
        assert_eq!(RuleMatcher::extract_extension(".bashrc"), None);
        assert_eq!(RuleMatcher::extract_extension("noext"), None);
    }

    #[test]
    fn io500_mdtest_predicts_inline() {
        let predictor = RuleBasedPredictor::with_defaults(PlacementPolicy::default());
        let result = predictor.predict(&ctx("mdtest_hard.000", "/data/io500"));
        assert!(result.placement.is_inline());
        assert!(result.confidence >= 0.9);
        assert_eq!(result.rule_name, "io500_mdtest");
    }

    #[test]
    fn io500_ior_predicts_stripe4() {
        let predictor = RuleBasedPredictor::with_defaults(PlacementPolicy::default());
        let result = predictor.predict(&ctx("ior_easy", "/data/io500"));
        match result.placement {
            Placement::Stripe { stripe_count, .. } => assert_eq!(stripe_count, 4),
            other => panic!("expected Stripe(4), got {:?}", other),
        }
        assert!(result.confidence >= 0.9);
        assert_eq!(result.rule_name, "io500_ior");
    }

    #[test]
    fn ml_checkpoint_predicts_stripe16() {
        let predictor = RuleBasedPredictor::with_defaults(PlacementPolicy::default());
        let result = predictor.predict(&ctx("model.pt", "/training/run1"));
        match result.placement {
            Placement::Stripe { stripe_count, .. } => assert_eq!(stripe_count, 16),
            other => panic!("expected Stripe(16), got {:?}", other),
        }
    }

    #[test]
    fn config_file_predicts_inline() {
        let predictor = RuleBasedPredictor::with_defaults(PlacementPolicy::default());
        let result = predictor.predict(&ctx("app.conf", "/etc/powerfs"));
        assert!(result.placement.is_inline());
    }

    #[test]
    fn executable_predicts_flat() {
        let predictor = RuleBasedPredictor::with_defaults(PlacementPolicy::default());
        let result = predictor.predict(&ctx("libpowerfs.so", "/usr/lib"));
        assert!(matches!(result.placement, Placement::Flat));
    }

    #[test]
    fn video_predicts_stripe4() {
        let predictor = RuleBasedPredictor::with_defaults(PlacementPolicy::default());
        let result = predictor.predict(&ctx("movie.mp4", "/media"));
        match result.placement {
            Placement::Stripe { stripe_count, .. } => assert_eq!(stripe_count, 4),
            other => panic!("expected Stripe(4), got {:?}", other),
        }
    }

    #[test]
    fn dir_xattr_overrides_rules() {
        let predictor = RuleBasedPredictor::with_defaults(PlacementPolicy::default());
        let mut c = ctx("model.pt", "/training");
        c.dir_placement = Some(PlacementSpec::Flat);
        let result = predictor.predict(&c);
        assert!(matches!(result.placement, Placement::Flat));
        assert!((result.confidence - 1.0).abs() < f32::EPSILON);
        assert_eq!(result.rule_name, "dir_xattr");
    }

    #[test]
    fn dir_inline_threshold_small_file() {
        let predictor = RuleBasedPredictor::with_defaults(PlacementPolicy::default());
        let mut c = ctx_with_size("unknown.bin", "/data", 100);
        c.dir_inline_threshold = Some(8192);
        let result = predictor.predict(&c);
        assert!(result.placement.is_inline());
        assert_eq!(result.rule_name, "dir_inline_threshold");
    }

    #[test]
    fn dir_inline_threshold_large_file_skipped() {
        let predictor = RuleBasedPredictor::with_defaults(PlacementPolicy::default());
        let mut c = ctx_with_size("unknown.bin", "/data", 1 << 20);
        c.dir_inline_threshold = Some(8192);
        let result = predictor.predict(&c);
        // 阈值不满足, 回退到规则: .bin → Flat (executables 规则)
        assert!(matches!(result.placement, Placement::Flat));
    }

    #[test]
    fn unknown_file_defers() {
        let predictor = RuleBasedPredictor::with_defaults(PlacementPolicy::default());
        let result = predictor.predict(&ctx("datafile", "/work"));
        assert_eq!(result.confidence, 0.0);
        assert_eq!(result.rule_name, "defer");
    }

    #[test]
    fn priority_ordering() {
        // 同名文件同时匹配多规则, 应取高优先级
        let rules = vec![
            LayoutRule {
                name: "low".to_string(),
                matcher: RuleMatcher::Extension {
                    exts: vec![".dat".to_string()],
                },
                placement: RulePlacement::Flat,
                confidence: 0.5,
                priority: 10,
            },
            LayoutRule {
                name: "high".to_string(),
                matcher: RuleMatcher::Extension {
                    exts: vec![".dat".to_string()],
                },
                placement: RulePlacement::Inline { max_size: 4096 },
                confidence: 0.9,
                priority: 100,
            },
        ];
        let predictor = RuleBasedPredictor::new(PlacementPolicy::default(), rules);
        let result = predictor.predict(&ctx("file.dat", "/data"));
        assert_eq!(result.rule_name, "high");
        assert!(result.placement.is_inline());
    }

    #[test]
    fn parent_dir_matcher() {
        let rule = LayoutRule {
            name: "tmp".to_string(),
            matcher: RuleMatcher::ParentDir {
                names: vec!["tmp".to_string()],
            },
            placement: RulePlacement::Inline { max_size: 4096 },
            confidence: 0.7,
            priority: 50,
        };
        let predictor = RuleBasedPredictor::new(PlacementPolicy::default(), vec![rule]);
        let result = predictor.predict(&ctx("random", "/var/tmp"));
        assert_eq!(result.rule_name, "tmp");
    }

    #[test]
    fn path_prefix_matcher() {
        let rule = LayoutRule {
            name: "tmpdir".to_string(),
            matcher: RuleMatcher::PathPrefix {
                prefix: "/tmp/".to_string(),
            },
            placement: RulePlacement::Inline { max_size: 4096 },
            confidence: 0.7,
            priority: 50,
        };
        let predictor = RuleBasedPredictor::new(PlacementPolicy::default(), vec![rule]);
        let result = predictor.predict(&ctx("random", "/tmp/work"));
        assert_eq!(result.rule_name, "tmpdir");
    }

    #[test]
    fn combined_all_matcher() {
        let rule = LayoutRule {
            name: "ml_bin".to_string(),
            matcher: RuleMatcher::All {
                matchers: vec![
                    RuleMatcher::Extension {
                        exts: vec![".bin".to_string()],
                    },
                    RuleMatcher::ParentDir {
                        names: vec!["checkpoints".to_string()],
                    },
                ],
            },
            placement: RulePlacement::Stripe {
                stripe_count: 16,
                stripe_size: 64 * 1024 * 1024,
            },
            confidence: 0.95,
            priority: 100,
        };
        let predictor = RuleBasedPredictor::new(PlacementPolicy::default(), vec![rule]);

        // 命中: checkpoints + .bin
        let result = predictor.predict(&ctx("model.bin", "/run/checkpoints"));
        assert_eq!(result.rule_name, "ml_bin");

        // 未命中: 不在 checkpoints 目录
        let result = predictor.predict(&ctx("model.bin", "/run/other"));
        assert_eq!(result.rule_name, "defer");
    }

    #[test]
    fn storage_mode_empty_default() {
        assert_eq!(StorageMode::default(), StorageMode::Empty);
        assert!(StorageMode::Empty.is_empty());
        assert!(!StorageMode::Empty.is_inline());
        assert!(!StorageMode::Empty.is_volume_backed());
    }
}
