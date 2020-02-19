#[macro_use]
extern crate criterion;

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use criterion::BenchmarkId;
use criterion::Criterion;
use kvs::engine::{KvStore, KvsEngine, SledStore};
use tempfile::TempDir;

fn write_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");

    let size: usize = 100;
    let input: Vec<(String, String)> = (0..size)
        .map(|_| {
            let rand_string: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(thread_rng().gen_range(1, 100000))
                .collect();
            (rand_string.clone(), rand_string)
        })
        .collect();

    group.bench_function(BenchmarkId::new("kvs_write", 1), |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.path()).unwrap();
        let mut i = 0;
        b.iter(|| {
            let kv = *&input.get(i).unwrap();
            &store.set(kv.0.clone(), kv.1.clone());
            i += 1;
            if i == *&input.len() {
                i = 0;
            }
        })
    });
    group.bench_function(BenchmarkId::new("sled_write", 1), |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut store = SledStore::open(temp_dir.path()).unwrap();
        let mut i = 0;
        b.iter(|| {
            let kv = *&input.get(i).unwrap();
            &store.set(kv.0.clone(), kv.1.clone());
            i += 1;
            if i == *&input.len() {
                i = 0;
            }
        })
    });

    group.finish();
}

fn read_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");

    let size: usize = 1000;
    let input: Vec<(String, String)> = (0..size)
        .map(|_| {
            let rand_string: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(thread_rng().gen_range(1, 100000))
                .collect();
            (rand_string.clone(), rand_string)
        })
        .collect();

    let read_iter: Vec<String> = (0..size)
        .map(|_| {
            let idx = thread_rng().gen_range(0, size);
            let k = &input.get(idx).unwrap().0;
            k.clone()
        })
        .collect();

    group.bench_function(BenchmarkId::new("kvs_read", 10), |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.path()).unwrap();
        let mut i = 0;

        for (k, v) in &input {
            &store.set(k.clone(), v.clone()).unwrap();
        }

        b.iter(|| {
            for _ in 0..10 {
                let k = *&read_iter.get(i).unwrap();
                &store.get(k.clone());
                i += 1;
                if i == *&read_iter.len() {
                    i = 0;
                }
            }
        })
    });

    group.bench_function(BenchmarkId::new("sled_read", 10), |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut store = SledStore::open(temp_dir.path()).unwrap();
        let mut i = 0;

        for (k, v) in &input {
            &store.set(k.clone(), v.clone()).unwrap();
        }

        b.iter(|| {
            for _ in 0..10 {
                let k = *&read_iter.get(i).unwrap();
                &store.get(k.clone());
                i += 1;
                if i == *&read_iter.len() {
                    i = 0;
                }
            }
        })
    });
    group.finish();
}

criterion_group!(benches, write_bench, read_bench);
criterion_main!(benches);
