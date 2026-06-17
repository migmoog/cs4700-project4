use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use anyhow::Result;
use jap::{Packet, PacketValue, SeqNum, adjust_rtt, check_for_packets, send_packets};
use tokio::io::{self, AsyncReadExt};
use tokio::net::UdpSocket;

/// Using the sender's window size this function eliminates acked packets from the list and creates
/// an ordered map of the packets to send
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

/// Checks a list of packets against received acks and returns true if all packets have been
/// acknowledged.
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
    let mut packets: BTreeMap<SeqNum, PacketValue> =
        Packet::data(&mut seq, stdin_buffer[..bytes_read].to_vec())?
            .into_iter()
            .map(|p| (p.seq, p.value))
            .collect();

    // Reusable ordered set to keep track of which acks we got in a round of sending
    let mut received_acks = BTreeSet::<SeqNum>::new();
    // Initial assumption for what the window size is. Will get adjusted
    let mut window_size = 4;

    // estimate the time needed for a packet to make a round trip
    let mut rtt = Duration::from_secs_f32(1.5);
    let mut rto = rtt * 2;
    let mut last_send = Instant::now();
    let mut window = make_window(&mut packets, window_size, &received_acks);
    // helper function that sends a window's packets on the socket
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

    // counter for how many acks we got in a round
    let mut acks_got = 0;
    loop {
        // success, all of our packets have been acked
        if got_all_acks(&packets, &received_acks) {
            eprintln!("Successfully got all Acks");
            break;
        }

        // Check to see what packets we've received
        let (received_packets, remaining) = check_for_packets(&mut sender, window_size).await;
        if !remaining.is_empty() {
            eprintln!("Unserializable data");
        }

        if !received_packets.is_empty() {
            let current_rtt = Instant::now() - last_send;
            rtt = adjust_rtt(rtt, current_rtt);
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
        // -----------------------------------------

        // check if we've timed out or gotten all the acks we need
        let time_since_send = Instant::now() - last_send;
        if time_since_send > rto || window.keys().all(|k| received_acks.contains(k)) {
            // adjust the window size based on how many acks we got
            if acks_got < window_size {
                let old_window_size = window_size;
                window_size = 1.max(window_size - 1);
                eprintln!(
                    "Sent {} packets. Got {} acks back. New window size is {}",
                    old_window_size, acks_got, window_size
                );
            } else {
                window_size += 1;
            }

            acks_got = 0;
            window = make_window(&mut packets, window_size, &received_acks);
            if window.is_empty() {
                eprintln!("I have nothing to send. Contuining");
                continue;
            }

            send(&window, &mut sender).await?;
            last_send = Instant::now();
            rto = rtt * 2;
        }
    }

    Ok(())
}
