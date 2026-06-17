use anyhow::Result;
use jap::{Packet, PacketValue, SeqNum, check_for_packets};
use std::collections::BTreeMap;
use tokio::{io::AsyncWriteExt, net::UdpSocket};

#[tokio::main]
async fn main() -> Result<()> {
    let mut receiver = UdpSocket::bind(format!("127.0.0.1:0")).await?;
    let port = receiver.local_addr()?.port();
    eprintln!("Bound to port {port}");

    let mut seq = 0;
    let adv_win: SeqNum = 4;
    let mut from_sender = BTreeMap::<SeqNum, PacketValue>::new();
    let mut file_data = BTreeMap::new();
    let mut ack_cum = 0;

    let mut out = tokio::io::stdout();
    let mut process = async |s: SeqNum, v: &PacketValue| match v {
        PacketValue::Data(d) => {
            file_data.insert(s, d.to_string());
            out.write_all(d.to_string().as_bytes()).await.ok();
            out.flush().await.ok();
        }
        _ => {}
    };

    loop {
        let (received_packets, remaining) = check_for_packets(&mut receiver, adv_win).await;
        if received_packets.is_empty() {
            continue;
        }

        // bad condition so it needs to be logged
        if !remaining.is_empty() {
            eprintln!(
                "R: got unserializable data: {:?}",
                String::from_utf8_lossy(&remaining)
            );
        }

        eprintln!("Got {} packets from sender", received_packets.len());
        for packet in received_packets {
            if packet.seq == ack_cum + 1 {
                process(packet.seq, &packet.value).await;
                eprintln!("In-order packet (prev ack: {}): {:?}", ack_cum, packet);
                ack_cum += 1;

                while from_sender.contains_key(&(ack_cum + 1)) {
                    if let Some(value) = from_sender.remove(&(ack_cum + 1)) {
                        process(ack_cum + 1, &value).await;
                        ack_cum += 1;
                    }
                }
            } else if let Packet {
                seq: _,
                value: PacketValue::Start,
            } = packet
            {
                // special case, do not ignore
                eprintln!("Got a start message");
            } else if packet.seq > ack_cum + 1 {
                // out of order
                eprintln!("OoO! Cum is {}, but this packet is {}", ack_cum, packet.seq);
                from_sender.insert(packet.seq, packet.value);
            } else {
                // ignore duplicates
                //eprintln!("Duplicate {}, AckCum {}", packet.seq, ack_cum);
            }
        }

        let p = Packet {
            seq,
            value: PacketValue::Ack { cum: ack_cum },
        };
        seq += 1;
        p.write_to(&mut receiver).await?;
    }
}
