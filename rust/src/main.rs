use rayon::prelude::*;
use std::hint::black_box;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{mpsc, Mutex};
use std::thread;
use std::time::Instant;

const N: usize = 1_000_000_000;

fn sum_seq(a: &[i8]) -> i64 {
    a.iter().map(|&x| x as i64).sum()
}

fn chunk_size(n: usize, parts: usize) -> usize {
    (n + parts - 1) / parts
}

fn sum_scope(a: &[i8], parts: usize) -> i64 {
    let cs = chunk_size(a.len(), parts);
    thread::scope(|s| {
        let handles: Vec<_> = a
            .chunks(cs)
            .map(|c| s.spawn(move || sum_seq(c)))
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).sum::<i64>()
    })
}

fn sum_channel(a: &[i8], parts: usize) -> i64 {
    let cs = chunk_size(a.len(), parts);
    let (tx, rx) = mpsc::channel();
    thread::scope(|s| {
        for c in a.chunks(cs) {
            let tx = tx.clone();
            s.spawn(move || tx.send(sum_seq(c)).unwrap());
        }
    });
    drop(tx);
    rx.iter().sum::<i64>()
}

fn sum_mutex(a: &[i8], parts: usize) -> i64 {
    let cs = chunk_size(a.len(), parts);
    let total = Mutex::new(0i64);
    thread::scope(|s| {
        for c in a.chunks(cs) {
            let total = &total;
            s.spawn(move || {
                let local = sum_seq(c);
                *total.lock().unwrap() += local;
            });
        }
    });
    total.into_inner().unwrap()
}

fn sum_rayon(a: &[i8], parts: usize) -> i64 {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(parts)
        .build()
        .unwrap();
    pool.install(|| a.par_iter().map(|&x| x as i64).sum::<i64>())
}

fn sum_atomic_bad(a: &[i8], parts: usize) -> i64 {
    let cs = chunk_size(a.len(), parts);
    let total = AtomicI64::new(0);
    thread::scope(|s| {
        for c in a.chunks(cs) {
            let total = &total;
            s.spawn(move || {
                for &x in c {
                    total.fetch_add(x as i64, Ordering::Relaxed);
                }
            });
        }
    });
    total.load(Ordering::Relaxed)
}

fn bench<F: Fn() -> i64>(name: &str, f: F) {
    const RUNS: usize = 5;
    let mut ts = Vec::with_capacity(RUNS);
    let mut r = 0i64;
    for _ in 0..RUNS {
        let t = Instant::now();
        r = black_box(f());
        ts.push(t.elapsed().as_millis());
    }
    ts.sort();
    println!(
        "{:<16} sum={}  median={} ms  min={} ms",
        name,
        r,
        ts[RUNS / 2],
        ts[0]
    );
}

fn main() {
    let mut a = vec![0i8; N];
    for (i, x) in a.iter_mut().enumerate() {
        *x = (i % 100) as i8;
    }
    let cpus = thread::available_parallelism().unwrap().get();

    bench("sequential", || sum_seq(&a));

    for parts in [1, 2, 4, cpus, cpus * 2] {
        println!("--- parts = {}", parts);
        bench("scope", || sum_scope(&a, parts));
        bench("channels", || sum_channel(&a, parts));
        bench("mutex", || sum_mutex(&a, parts));
        bench("rayon", || sum_rayon(&a, parts));
    }

    let small = &a[..50_000_000];
    println!("--- atomic (bad), 50M elements");
    bench("seq (50M)", || sum_seq(small));
    bench("atomic/element", || sum_atomic_bad(small, cpus));
}