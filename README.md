# Kafka Client

This is a kafka client written in pure Rust.

It uses [kafka-protocol](https://github.com/tychedelia/kafka-protocol-rs) for the protocol implementation, and [tokio](https://github.com/tokio-rs/tokio) for async io.

## To-do

- Allow response type to be known at compile type based on request type.
  - Make transport codec just use length-delimited, handle decoding each frame outside the transport
