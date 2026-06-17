# Jeremy's Awesome Protocol (JAP)
My project is split into 3 different rust crates.

## Crate 1: JAP

the folder `jap/` (acronym for Jeremy's Awesome Protocol) contains a rust library with common types and functions used throughout the project. 
The most important aspects are the packet types:
- `Packet` in lib.rs, contains the packet's sequence number and accompanying message
- `PacketValue` in value.rs, contains one of three types that a packet can be. A start to measure RTT, a Data that contains the file from stdin, and an Ack that holds a cumulative ack
for all the packets acknowledged by the receiver.


## Crate 2: send

this is the binary crate for 4700send. It begins with an inital RTT assumption of 1.5 seconds. It begins with a window of 4 packets, and sends them. After every received packet the RTT is adjusted based on the new time with the according library function, `adjust_rtt`. 
The sender loops until all of its packets have been acknowledged. In the event of a timeout, it will decrease its window by 1 and if it is successful in sending every packet it will increase its window by 1.

## Crate 3: recv

This is the binary crate for 4700recv. It consistently polls for any incoming packets, and will send back a cumulative ack based on all the ordered packets it has received. If the 
receiver gets something OoO, it puts it into a packet buffer. Once it fills in the missing gaps of packets, it will drain the buffer of every unacked packet passed the filled gap. Every
time the receiver gets a packet in order it will immediately flush the packet's content to stdout.

# Challenges

By far the hardest challenge was implementing a sliding window protocol. The simulator drops off packets one by one and the sockets cannot buffer them together. So, I had to change my expectation of parsing packets from the socket in a single go, and instead check how many acks were received over the course of a timeout/receiving of every ack. My initial approach involved a lot OOP that quickly grew too complex, and relegating sliding window to a single function immediately improved it.

## Stuff I like

I am a big fan of my packet format. I used a new rust crate called "postcard" that serializes structs into binary code, and that made packets with file data very easy to fragment. I also am a big fan
of the PacketValue data type because I believe rust enums are the most elegant data type in any programming language.

## Testing

I relied on the simulator scripts to test. Printing to stderr was also helpful. As far as debugging I only really needed to check output.
