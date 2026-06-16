use std::time::Duration;

use anyhow::Result;
use tokio::net::UdpSocket;
use tokio::time::{Instant, timeout_at};

use crate::Ack;
use crate::{MTU, Packet, PacketList, SeqNum, value::PacketValue};

/// Adjusts the round trip time estimate with the provided formula
pub fn adjust_rtt(old_rtt: Duration, new_sample: Duration) -> Duration {
    const RTT_ALPHA: f32 = 0.85;
    Duration::from_secs_f32(
        RTT_ALPHA * old_rtt.as_secs_f32() + (1.0 - RTT_ALPHA) * new_sample.as_secs_f32(),
    )
}

pub async fn check_for_packets(
    socket: &mut UdpSocket,
    rwnd: SeqNum,
) -> (PacketList, Vec<u8>) {
    let mut buf = Vec::with_capacity(MTU * rwnd as usize);
    buf.reserve(MTU);

    // read all the bytes possible
    while let Ok(_) = if socket.peer_addr().is_ok() {
        socket.try_recv_buf(&mut buf)
    } else {
        let res = socket.try_recv_buf_from(&mut buf);
        match res {
            Ok((len, addr)) => {
                socket.connect(addr).await.expect("Failed to connect");
                Ok(len)
            },
            Err(e) => Err(e),
        }
    } {}

    let (list, remaining) = Packet::from_bytes(&buf);
    (list, remaining.to_vec())
}

/// Blocks reading on the socket until a response is received. Returns deserialized packets and any
/// remaining bytes that couldn't be deserialized. Will sort the packets
pub async fn wait_for_packets(
    socket: &mut UdpSocket,
    rwnd: SeqNum,
) -> Result<(PacketList, Vec<u8>)> {
    let mut buf = Vec::with_capacity(MTU * rwnd as usize);
    buf.reserve(MTU);
    let _bytes_read = if socket.peer_addr().is_ok() {
        socket.recv_buf(&mut buf).await?
    } else {
        let (len, addr) = socket.recv_buf_from(&mut buf).await?;
        socket.connect(addr).await?;
        len
    };

    // Drain every datagram already sitting in the receive buffer, so we always
    // act on the most recent ACK instead of a stale backlog.
    loop {
        buf.reserve(MTU);
        match socket.try_recv_buf(&mut buf) {
            Ok(_) => continue,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => return Err(e.into()),
        }
    }

    let (received_packets, remaining) = Packet::from_bytes(&buf);
    Ok((received_packets, remaining.to_vec()))
}

/// Writes packets to the socket.
/// NOTE: Assumes the packets do not exceed MTU
pub async fn send_packets<'a, T>(socket: &mut UdpSocket, packets: T) -> Result<()>
where
    T: Iterator<Item = &'a Packet>,
{
    for p in packets {
        p.write_to(socket).await?;
    }

    Ok(())
}

async fn share_packets<'a, T>(
    socket: &mut UdpSocket,
    packets: T,
    rwnd: SeqNum,
) -> Result<PacketList>
where
    T: Iterator<Item = &'a Packet>,
{
    send_packets(socket, packets).await?;

    let (received_packets, remaining) = wait_for_packets(socket, rwnd).await?;
    if !remaining.is_empty() {
        eprintln!(
            "Unserializable bytes: {}",
            String::from_utf8_lossy(&remaining)
        );
    }

    Ok(received_packets)
}

// Writes a list of packets to the provided socket. Will poll the socket for all responses.
// Returns the list of response packets on success, in the case of corruption or timeouts it will
// send the sample size of time taken. In the case of the socket failure it bubbles up the error.
// Will send an empty list on timing out.
pub async fn try_share_packets<'a, T>(
    rtt: Duration,
    packets: T,
    socket: &mut UdpSocket,
    rwnd: SeqNum,
) -> Result<(PacketList, Duration)>
where
    T: Iterator<Item = &'a Packet>,
{
    let rto = rtt * 2;
    let start = Instant::now();
    let quit_time = start + rto;
    send_packets(socket, packets).await?;
    match timeout_at(quit_time, wait_for_packets(socket, rwnd)).await {
        Ok(Ok((received_bytes, remaining))) => {
            if !remaining.is_empty() {
                eprintln!(
                    "Unserializable bytes: {}",
                    String::from_utf8_lossy(&remaining)
                );
            }
            Ok((received_bytes, Instant::now() - start))
        }
        Err(_) => Ok((vec![], Instant::now() - start)),
        Ok(Err(socket_error)) => Err(socket_error),
    }
}

/// Sends a "start" message packet. Will test RTT's with exponential backoff.
/// Returns an rtt sample or a socket error
pub async fn start(socket: &mut UdpSocket, fragments: SeqNum) -> Result<Duration> {
    let mut rtt = Duration::from_secs_f32(0.75);
    let start_slice = &[Packet {
        seq: 0,
        value: PacketValue::Start(fragments),
    }];
    loop {
        match try_share_packets(rtt, start_slice.iter(), socket, 1).await {
            Ok((received_packets, dur))
                if matches!(
                    received_packets.get(0),
                    Some(Packet {
                        seq: _,
                        value: PacketValue::Ack(Ack {
                            cum: 0,
                            sel: _,
                            adv_win: _
                        })
                    })
                ) =>
            {
                return Ok(dur);
            }
            Err(e) => return Err(e),
            _ => {}
        }
        rtt *= 2;
    }
}
