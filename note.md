# PowerFS 开发规范注意事项

## 代码质量检查

每个开发阶段完成后，必须执行以下代码质量检查：

### 1. Clippy 静态分析

```bash
cargo clippy --all -- -D warnings
```

将所有警告视为错误，确保代码符合 Rust 最佳实践。

### 2. Rustfmt 代码格式化

```bash
cargo fmt --all
```

确保代码风格统一。

### 3. 编译检查

```bash
cargo check --all
```

确保项目能够正常编译。

### 4. 测试执行（如有测试）

```bash
cargo test --all
```

运行所有测试用例，确保功能正确性。

## 检查时机

- [ ] 每个功能模块开发完成后
- [ ] 修复 Bug 后
- [ ] 代码审查前
- [ ] 提交代码前
- [ ] 发布版本前

## 检查清单

```bash
# 完整检查流程
cargo fmt --all
cargo clippy --all -- -D warnings
cargo check --all
cargo test --all
```