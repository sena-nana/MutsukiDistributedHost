use mutsuki_distributed_runtime::is_remote_profitable;
use std::hint::black_box;
use std::time::Instant;

fn main() {
    let iterations = 1_000_000_u64;
    let started = Instant::now();
    let mut negative_remote_accepts = 0_u64;
    let mut profitable_remote_accepts = 0_u64;
    for index in 0..iterations {
        let local = black_box(5.0 + f64::from((index % 100) as u32));
        let remote = black_box(8.0);
        if is_remote_profitable(local, remote, 3.0, local <= 5.0, false) {
            if local <= remote + 3.0 {
                negative_remote_accepts += 1;
            } else {
                profitable_remote_accepts += 1;
            }
        }
    }
    assert_eq!(negative_remote_accepts, 0);
    assert!(profitable_remote_accepts > 0);
    println!(
        "placement_profitability: {iterations} decisions in {:?}; negative remote accepts: {negative_remote_accepts}",
        started.elapsed()
    );
}
