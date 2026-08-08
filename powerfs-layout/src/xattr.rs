//! 目录属性解析 (设计文档 S4.4)
//!
//! 两个独立 xattr:
//! - `powerfs.placement`: 控制"数据放哪" (Flat/Stripe/WideStripe)
//! - `powerfs.inline`: 控制"是否绕过 Volume Server" (阈值, 0=禁用)
//!
//! 格式:
//! - `powerfs.placement`:
//!   - `flat`
//!   - `stripe:<count>:<size>` (如 `stripe:4:64MB`)
//!   - `wide_stripe:<count>:<size>` (如 `wide_stripe:256:4MB`)
//! - `powerfs.inline`:
//!   - `<size>` (如 `4096`, `8192`)
//!   - `0` (禁用 inline)

use crate::error::LayoutError;
use crate::placement::PlacementSpec;

/// 解析 `powerfs.placement` xattr 值
///
/// # 示例
/// ```
/// # use powerfs_layout::xattr::parse_placement_xattr;
/// let spec = parse_placement_xattr("stripe:4:64MB").unwrap();
/// let spec = parse_placement_xattr("flat").unwrap();
/// let spec = parse_placement_xattr("wide_stripe:256:4MB").unwrap();
/// ```
pub fn parse_placement_xattr(value: &str) -> Result<PlacementSpec, LayoutError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(LayoutError::InvalidXattr {
            attr: "powerfs.placement".into(),
            value: value.into(),
        });
    }

    if value == "flat" {
        return Ok(PlacementSpec::Flat);
    }

    if let Some(rest) = value.strip_prefix("stripe:") {
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() != 2 {
            return Err(LayoutError::InvalidXattr {
                attr: "powerfs.placement".into(),
                value: value.into(),
            });
        }
        let count: u32 = parts[0].parse().map_err(|_| LayoutError::InvalidXattr {
            attr: "powerfs.placement".into(),
            value: value.into(),
        })?;
        let stripe_size = parse_size(parts[1])?;
        if count == 0 {
            return Err(LayoutError::InvalidXattr {
                attr: "powerfs.placement".into(),
                value: value.into(),
            });
        }
        return Ok(PlacementSpec::Stripe { count, stripe_size });
    }

    if let Some(rest) = value.strip_prefix("wide_stripe:") {
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() != 2 {
            return Err(LayoutError::InvalidXattr {
                attr: "powerfs.placement".into(),
                value: value.into(),
            });
        }
        let count: u32 = parts[0].parse().map_err(|_| LayoutError::InvalidXattr {
            attr: "powerfs.placement".into(),
            value: value.into(),
        })?;
        let stripe_size = parse_size(parts[1])?;
        if count == 0 {
            return Err(LayoutError::InvalidXattr {
                attr: "powerfs.placement".into(),
                value: value.into(),
            });
        }
        return Ok(PlacementSpec::WideStripe { count, stripe_size });
    }

    Err(LayoutError::InvalidXattr {
        attr: "powerfs.placement".into(),
        value: value.into(),
    })
}

/// 解析 `powerfs.inline` xattr 值
///
/// 返回 `Some(threshold)` 或 `None` (禁用 inline).
///
/// # 示例
/// ```
/// # use powerfs_layout::xattr::parse_inline_xattr;
/// assert_eq!(parse_inline_xattr("4096").unwrap(), Some(4096));
/// assert_eq!(parse_inline_xattr("0").unwrap(), None);     // 禁用
/// assert_eq!(parse_inline_xattr("8192").unwrap(), Some(8192));
/// ```
pub fn parse_inline_xattr(value: &str) -> Result<Option<u32>, LayoutError> {
    let value = value.trim();
    let size: u32 = value.parse().map_err(|_| LayoutError::InvalidXattr {
        attr: "powerfs.inline".into(),
        value: value.into(),
    })?;
    if size == 0 {
        Ok(None) // 0 = 禁用 inline
    } else {
        Ok(Some(size))
    }
}

/// 解析大小后缀: KB/MB/GB, 无后缀=字节
fn parse_size(s: &str) -> Result<u64, LayoutError> {
    let s = s.trim().to_uppercase();
    if let Some(num) = s.strip_suffix("GB") {
        let n: u64 = num.parse().map_err(|_| LayoutError::InvalidXattr {
            attr: "powerfs.placement".into(),
            value: s.clone(),
        })?;
        Ok(n * 1024 * 1024 * 1024)
    } else if let Some(num) = s.strip_suffix("MB") {
        let n: u64 = num.parse().map_err(|_| LayoutError::InvalidXattr {
            attr: "powerfs.placement".into(),
            value: s.clone(),
        })?;
        Ok(n * 1024 * 1024)
    } else if let Some(num) = s.strip_suffix("KB") {
        let n: u64 = num.parse().map_err(|_| LayoutError::InvalidXattr {
            attr: "powerfs.placement".into(),
            value: s.clone(),
        })?;
        Ok(n * 1024)
    } else {
        let n: u64 = s.parse().map_err(|_| LayoutError::InvalidXattr {
            attr: "powerfs.placement".into(),
            value: s.clone(),
        })?;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_flat() {
        assert_eq!(parse_placement_xattr("flat").unwrap(), PlacementSpec::Flat);
    }

    #[test]
    fn parse_stripe() {
        let spec = parse_placement_xattr("stripe:4:64MB").unwrap();
        assert_eq!(
            spec,
            PlacementSpec::Stripe {
                count: 4,
                stripe_size: 64 * 1024 * 1024
            }
        );
    }

    #[test]
    fn parse_wide_stripe() {
        let spec = parse_placement_xattr("wide_stripe:256:4MB").unwrap();
        assert_eq!(
            spec,
            PlacementSpec::WideStripe {
                count: 256,
                stripe_size: 4 * 1024 * 1024
            }
        );
    }

    #[test]
    fn parse_stripe_bytes() {
        let spec = parse_placement_xattr("stripe:8:1048576").unwrap();
        assert_eq!(
            spec,
            PlacementSpec::Stripe {
                count: 8,
                stripe_size: 1024 * 1024
            }
        );
    }

    #[test]
    fn parse_inline() {
        assert_eq!(parse_inline_xattr("4096").unwrap(), Some(4096));
        assert_eq!(parse_inline_xattr("8192").unwrap(), Some(8192));
        assert_eq!(parse_inline_xattr("0").unwrap(), None);
    }

    #[test]
    fn parse_invalid() {
        assert!(parse_placement_xattr("").is_err());
        assert!(parse_placement_xattr("unknown").is_err());
        assert!(parse_placement_xattr("stripe:abc:64MB").is_err());
        assert!(parse_placement_xattr("stripe:0:64MB").is_err());
        assert!(parse_inline_xattr("abc").is_err());
    }
}
