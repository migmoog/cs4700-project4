use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use anyhow::Result;
use jap::{Packet, PacketValue, SeqNum, check_for_packets, send_packets};
use tokio::io::{self, AsyncReadExt};
use tokio::net::UdpSocket;

fn make_window(
    packets: &mut BTreeMap<SeqNum, PacketValue>,
    window_size: SeqNum,
    received_acks: &BTreeSet<SeqNum>,
) -> BTreeMap<SeqNum, PacketValue> {
    packets.retain(|k, _| !received_acks.contains(k));

    packets
        .iter()
        .map(|(&k, v)| (k, v.clone()))
        .take(window_size as usize)
        .collect()
}

fn got_all_acks(packets: &BTreeMap<SeqNum, PacketValue>, received_acks: &BTreeSet<SeqNum>) -> bool {
    packets.keys().all(|k| received_acks.contains(k))
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args();
    let _program = args.next().unwrap();
    let host = args.next().unwrap();
    let port = args.next().unwrap();

    let mut stdin = io::stdin();
    let mut stdin_buffer = Vec::new();
    let mut sender = UdpSocket::bind("127.0.0.1:0").await?;
    sender.connect(format!("{host}:{port}")).await?;
    let bytes_read = stdin.read_to_end(&mut stdin_buffer).await?;
    // all packets will start at 0 and will wrap
    let mut seq = 0;
    // means we encountered an EOF
    let mut packets: BTreeMap<SeqNum, PacketValue> =
        Packet::data(&mut seq, stdin_buffer[..bytes_read].to_vec())?
            .into_iter()
            .map(|p| (p.seq, p.value))
            .collect();

    let mut received_acks = BTreeSet::<SeqNum>::new();
    let mut window_size = 4;

    let rtt = Duration::from_secs_f32(1.5);
    let mut last_send = Instant::now();
    let mut window = make_window(&mut packets, window_size, &received_acks);
    let send = async |w: &BTreeMap<SeqNum, PacketValue>, socket: &mut UdpSocket| {
        let v: Vec<Packet> = w
            .iter()
            .map(|(k, v)| Packet {
                seq: *k,
                value: v.clone(),
            })
            .collect();
        eprint!("Trying to send IDs ",);
        for id in v.iter().map(|p| p.seq) {
            eprint!("{}, ", id);
        }
        eprint!("\n");
        send_packets(socket, v.iter()).await
    };

    send(&window, &mut sender).await?;

    let mut acks_got = 0;
    loop {
        // success, all of our packets have been acked
        if got_all_acks(&packets, &received_acks) {
            eprintln!("Successfully got all Acks");
            break;
        }

        let (received_packets, remaining) = check_for_packets(&mut sender, window_size).await;
        if !remaining.is_empty() {
            eprintln!("Unserializable data");
        }

        if !received_packets.is_empty() {
            for p in received_packets {
                if let PacketValue::Ack { cum } = p.value {
                    for id in packets.keys().filter_map(|&k| (k <= cum).then_some(k)) {
                        if received_acks.insert(id) {
                            eprintln!("Got Ack {} from recv packet {}", id, seq);
                            acks_got += 1;
                        }
                    }
                }
            }
        }

        let time_since_send = Instant::now() - last_send;
        if time_since_send > rtt * 2 || window.keys().all(|k| received_acks.contains(k)) {
            if acks_got < window_size {
                let old_window_size = window_size;
                window_size = 1.max(window_size - 1);
                eprintln!(
                    "Sent {} packets. Got {} acks back. New window size is {}",
                    old_window_size, acks_got, window_size
                );
            }

            acks_got = 0;
            window = make_window(&mut packets, window_size, &received_acks);
            if window.is_empty() {
                eprintln!("I have nothing to send. Contuining");
                continue;
            }

            send(&window, &mut sender).await?;
            last_send = Instant::now();
        }
    }

    Ok(())
}
