// SPDX-License-Identifier: MIT

//! Host-level metrics for the `/health` endpoint: CPU load average, RAM,
//! and a rolling network throughput rate.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde::Serialize;
use sysinfo::{Networks, System};

pub const SAMPLE_INTERVAL_SECS: u64 = 15;
pub const WINDOW_SECS: u64 = 300;
pub const WINDOW_BUCKETS: usize = (WINDOW_SECS / SAMPLE_INTERVAL_SECS) as usize;

#[derive(Clone, Default, Serialize)]
pub struct SysSnapshot {
    pub cpu: CpuSnapshot,
    pub memory: MemSnapshot,
    pub network: NetSnapshot,
}

#[derive(Clone, Default, Serialize)]
pub struct MemSnapshot {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Clone, Default, Serialize)]
pub struct CpuSnapshot {
    pub load_average: LoadAverage,
    pub num_cpus: usize,
}

#[derive(Clone, Copy, Default, Serialize)]
pub struct LoadAverage {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
}

#[derive(Clone, Default, Serialize)]
pub struct NetSnapshot {
    pub bytes_per_sec_in: u64,
    pub bytes_per_sec_out: u64,
    pub samples: usize,
    pub window_seconds: u64,
    pub sample_interval_seconds: u64,
}

#[derive(Clone)]
pub struct SysState {
    inner: Arc<Inner>,
}

struct Inner {
    latest: Mutex<SysSnapshot>,
}

impl SysState {
    pub fn new() -> Self {
        let initial = SysSnapshot {
            cpu: CpuSnapshot::default(),
            memory: MemSnapshot::default(),
            network: NetSnapshot {
                bytes_per_sec_in: 0,
                bytes_per_sec_out: 0,
                samples: 0,
                window_seconds: WINDOW_SECS,
                sample_interval_seconds: SAMPLE_INTERVAL_SECS,
            },
        };
        Self {
            inner: Arc::new(Inner {
                latest: Mutex::new(initial),
            }),
        }
    }

    pub fn snapshot(&self) -> SysSnapshot {
        self.inner.latest.lock().clone()
    }

    pub async fn run(self, shutdown: impl std::future::Future<Output = ()>) {
        let mut sys = System::new();
        let mut nets = Networks::new();
        let mut prev_bytes: HashMap<String, (u64, u64)> = HashMap::new();
        let mut ring: Vec<(f64, f64)> = Vec::with_capacity(WINDOW_BUCKETS);

        let mut shutdown = std::pin::pin!(shutdown);
        let mut tick = tokio::time::interval(Duration::from_secs(SAMPLE_INTERVAL_SECS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = tick.tick() => {
                    sys.refresh_cpu_all();
                    sys.refresh_memory();
                    nets.refresh(true);

                    let (mut d_in, mut d_out) = (0u64, 0u64);
                    for (name, data) in nets.list() {
                        if is_loopback_or_virtual(name) {
                            continue;
                        }
                        let received = data.total_received();
                        let transmitted = data.total_transmitted();
                        if let Some(&(pr, pt)) = prev_bytes.get(name.as_str()) {
                            d_in = d_in.saturating_add(received.saturating_sub(pr));
                            d_out = d_out.saturating_add(transmitted.saturating_sub(pt));
                        }
                        prev_bytes.insert(name.clone(), (received, transmitted));
                    }

                    let rate_in = d_in as f64 / SAMPLE_INTERVAL_SECS.max(1) as f64;
                    let rate_out = d_out as f64 / SAMPLE_INTERVAL_SECS.max(1) as f64;
                    ring.push((rate_in, rate_out));
                    if ring.len() > WINDOW_BUCKETS {
                        ring.remove(0);
                    }

                    let (sum_in, sum_out) =
                        ring.iter().fold((0.0_f64, 0.0_f64), |(si, so), (ri, ro)| {
                            (si + ri, so + ro)
                        });
                    let n = ring.len().max(1) as f64;
                    let net = NetSnapshot {
                        bytes_per_sec_in: (sum_in / n) as u64,
                        bytes_per_sec_out: (sum_out / n) as u64,
                        samples: ring.len(),
                        window_seconds: WINDOW_SECS,
                        sample_interval_seconds: SAMPLE_INTERVAL_SECS,
                    };

                    let la = System::load_average();
                    let cpu = CpuSnapshot {
                        load_average: LoadAverage {
                            one: la.one,
                            five: la.five,
                            fifteen: la.fifteen,
                        },
                        num_cpus: sys.cpus().len(),
                    };
                    let memory = MemSnapshot {
                        total_bytes: sys.total_memory(),
                        free_bytes: sys.free_memory(),
                        available_bytes: sys.available_memory(),
                    };
                    let mut latest = self.inner.latest.lock();
                    latest.cpu = cpu;
                    latest.memory = memory;
                    latest.network = net;
                }
            }
        }
    }
}

fn is_loopback_or_virtual(name: &str) -> bool {
    matches!(name, "lo" | "lo0")
        || name.starts_with("docker")
        || name.starts_with("br-")
        || name.starts_with("veth")
        || name.starts_with("vnet")
        || name.starts_with("virbr")
}

impl Default for SysState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_defaults_are_zero_when_unsampled() {
        let s = SysState::new();
        let snap = s.snapshot();
        assert_eq!(snap.cpu.num_cpus, 0);
        assert_eq!(snap.memory.total_bytes, 0);
        assert_eq!(snap.network.window_seconds, WINDOW_SECS);
    }

    #[test]
    fn loopback_and_virtual_interfaces_are_excluded() {
        assert!(is_loopback_or_virtual("lo"));
        assert!(!is_loopback_or_virtual("eth0"));
    }
}
