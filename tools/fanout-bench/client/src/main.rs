//! fanout-bench-client <ws-base> <db> <protocol> <subscribers> <tx_per_sec> <secs>
//! protocol: v1.json | v1.bsatn | v2.bsatn | v3.bsatn
//! Prints one line: frames, bytes, and reducer calls sent during the measured window.
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

const TABLES: [&str; 4] = ["experience_state", "progressive_action_state", "claim_local_state", "stamina_state"];

fn s(out: &mut Vec<u8>, v: &str) {
    out.extend((v.len() as u32).to_le_bytes());
    out.extend(v.as_bytes());
}

fn subscribe_msg(proto: &str) -> Message {
    let qs: Vec<String> = TABLES.iter().map(|t| format!("SELECT * FROM {t}")).collect();
    match proto.trim_end_matches("+c").trim_end_matches("-c") {
        "v1.json" => {
            let q: Vec<String> = qs.iter().map(|q| format!("\"{q}\"")).collect();
            Message::text(format!("{{\"Subscribe\":{{\"query_strings\":[{}],\"request_id\":1}}}}", q.join(",")))
        }
        "v1.bsatn" => {
            // ClientMessage::Subscribe = tag 1 { query_strings: Vec<String>, request_id: u32 }
            let mut b = vec![1u8];
            b.extend((qs.len() as u32).to_le_bytes());
            for q in &qs { s(&mut b, q); }
            b.extend(1u32.to_le_bytes());
            Message::binary(b)
        }
        _ => {
            // v2 ClientMessage::Subscribe = tag 0 { request_id: u32, query_set_id: u32, query_strings: Vec<String> }
            let mut b = vec![0u8];
            b.extend(1u32.to_le_bytes());
            b.extend(1u32.to_le_bytes());
            b.extend((qs.len() as u32).to_le_bytes());
            for q in &qs { s(&mut b, q); }
            Message::binary(b)
        }
    }
}

async fn connect(base: &str, db: &str, proto: &str)
    -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let (proto, confirmed) = if let Some(p) = proto.strip_suffix("+c") {
        (p, "&confirmed=true")
    } else if let Some(p) = proto.strip_suffix("-c") {
        (p, "&confirmed=false")
    } else {
        (proto, "")
    };
    let mut req = format!("{base}/v1/database/{db}/subscribe?compression=None{confirmed}").into_client_request().unwrap();
    req.headers_mut().insert("Sec-WebSocket-Protocol", format!("{proto}.spacetimedb").parse().unwrap());
    let cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(1 << 30)).max_frame_size(Some(1 << 30));
    tokio_tungstenite::connect_async_with_config(req, Some(cfg), true).await.expect("connect").0
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (base, db, proto) = (a[1].clone(), a[2].clone(), a[3].clone());
    let subs: usize = a[4].parse().unwrap();
    let rate: f64 = a[5].parse().unwrap();
    let secs: u64 = a[6].parse().unwrap();
    let frames = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let calls = Arc::new(AtomicU64::new(0));

    for _ in 0..subs {
        let (base, db, proto, frames, bytes) = (base.clone(), db.clone(), proto.clone(), frames.clone(), bytes.clone());
        tokio::spawn(async move {
            let mut ws = connect(&base, &db, &proto).await;
            ws.send(subscribe_msg(&proto)).await.unwrap();
            while let Some(Ok(m)) = ws.next().await {
                if m.is_binary() || m.is_text() {
                    frames.fetch_add(1, Relaxed);
                    bytes.fetch_add(m.len() as u64, Relaxed);
                }
            }
            eprintln!("subscriber closed");
        });
    }
    // Let the initial subscriptions (4 x 20k rows each) drain.
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Writer: v1.json CallReducer with flags=1 (no success notification).
    let mut w = connect(&base, &db, "v1.json").await;
    let c2 = calls.clone();
    tokio::spawn(async move {
        let (mut tx, mut rx) = w.split();
        tokio::spawn(async move { while let Some(Ok(_)) = rx.next().await {} });
        let mut iv = tokio::time::interval(Duration::from_secs_f64(1.0 / rate));
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
        let mut id = 0u32;
        loop {
            iv.tick().await;
            id += 1;
            let m = format!("{{\"CallReducer\":{{\"reducer\":\"tick\",\"args\":\"[]\",\"request_id\":{id},\"flags\":1}}}}");
            if tx.send(Message::text(m)).await.is_err() { break; }
            c2.fetch_add(1, Relaxed);
        }
    });

    tokio::time::sleep(Duration::from_secs(3)).await; // warm-up
    println!("WINDOW_START");
    let (f0, b0, c0) = (frames.load(Relaxed), bytes.load(Relaxed), calls.load(Relaxed));
    tokio::time::sleep(Duration::from_secs(secs)).await;
    let (f1, b1, c1) = (frames.load(Relaxed), bytes.load(Relaxed), calls.load(Relaxed));
    println!("WINDOW_END frames={} bytes={} calls={} secs={}", f1 - f0, b1 - b0, c1 - c0, secs);
}
