use anyhow::Result;
use jap::{Ack, FileData, Packet, PacketValue, SeqNum, check_for_packets, send_packets};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::UdpSocket, task::JoinHandle};

fn print_file_data(fd: &BTreeMap<u32, FileData>) {
    let total_data: Vec<u8> = fd.values().map(|f| f.0.clone()).flatten().collect();
    print!("{}", String::from_utf8_lossy(&total_data));
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut receiver = UdpSocket::bind(format!("127.0.0.1:0")).await?;
    let port = receiver.local_addr()?.port();
    eprintln!("Bound to port {port}");

    let mut seq = 0;
    let adv_win: SeqNum = 4;
    let mut from_sender = BTreeMap::<SeqNum, PacketValue>::new();
    let mut file_data = BTreeMap::new();
    loop {
        let (received_packets, remaining) = check_for_packets(&mut receiver, adv_win).await;
        if !received_packets.is_empty() {
            // bad condition so it needs to be logged
            if !remaining.is_empty() {
                eprintln!(
                    "R: got unserializable data: {:?}",
                    String::from_utf8_lossy(&remaining)
                );
            }

            for packet in received_packets {
                if !from_sender.contains_key(&packet.seq) {
                    eprintln!("Packet {:?}: {:?}", packet.seq, packet.value);
                    match &packet.value {
                        PacketValue::Data(d) => {
                            file_data.insert(packet.seq, d.to_string());
                        }
                        PacketValue::Fin => {
                            for f in file_data.values() {
                                print!("{}", f);
                            }
                        }
                        _ => (),
                    }
                    from_sender.insert(packet.seq, packet.value);
                }
            }

            let p = Packet {
                seq,
                value: PacketValue::Ack(from_sender.keys().cloned().collect()),
            };
            seq += 1;
            p.write_to(&mut receiver).await?;
        }
    }
}
