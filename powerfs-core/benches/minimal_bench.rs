use criterion::{criterion_group, criterion_main, Criterion};

fn minimal_benchmark(c: &mut Criterion) {
    c.bench_function("minimal_test", |b| {
        b.iter(|| {
            let mut x = 0;
            for i in 0..1000 {
                x += i;
            }
            x
        });
    });
}

criterion_group!(benches, minimal_benchmark);
criterion_main!(benches);