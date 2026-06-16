use anyhow::Result;
use jap::{Ack, FileData, Packet, PacketValue, SeqNum, send_packets, wait_for_packets};
use std::collections::BTreeMap;
use tokio::net::UdpSocket;

fn print_file_data(fd: &BTreeMap<u32, FileData>) {
    let total_data: Vec<u8> = fd.values().map(|f| f.0.clone()).flatten().collect();
    print!("{}", String::from_utf8_lossy(&total_data));
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut receiver = UdpSocket::bind(format!("127.0.0.1:0")).await?;
    let port = receiver.local_addr()?.port();
    eprintln!("Bound to port {port}");

    let mut file_data = BTreeMap::new();
    // wait for the initial start message from the sender
    let adv_win: SeqNum = 4;
    let mut seq: SeqNum = 0;
    let mut finished = false;
    loop {
        let (received_packets, remaining) = wait_for_packets(&mut receiver, adv_win).await?;
        if !remaining.is_empty() {
            eprintln!(
                "R: got unserializable data: {:?}",
                String::from_utf8_lossy(&remaining)
            );
        }

        let ack = Ack::from_packets(received_packets.as_slice(), adv_win);
        for packet in received_packets {
            match packet.value {
                PacketValue::Start => {}
                PacketValue::Data(fd) => {
                    if !file_data.contains_key(&packet.seq) {
                        file_data.insert(packet.seq, fd);
                        eprintln!(
                            "R: Seq: {} {:?}",
                            packet.seq,
                            file_data.get(&packet.seq).unwrap()
                        );
                    }
                }
                PacketValue::Fin if !finished => {
                    finished = true;
                    print_file_data(&file_data);
                }

                // sender would never send an Ack to the receiver
                _ => unreachable!(),
            }
        }

        send_packets(
            &mut receiver,
            [Packet {
                seq,
                value: PacketValue::Ack(ack),
            }]
            .iter(),
        )
        .await?;
        seq += 1;
    }
}
