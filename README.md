# Kafka Client

This is a Kafka client written in pure Rust.

It uses [kafka-protocol](https://github.com/tychedelia/kafka-protocol-rs) for the protocol implementation, and [tokio](https://github.com/tokio-rs/tokio) for async io.

## To Do

- Get the producer working similarly to the Java client

  - It will buffer records to a partition up to a size or time limit (official client has a race condition related to partition selection here - investigate)
  - It lazily fetches metadata per-partition, because there can be thousands of topics/partitions, so it's not always feasible to use the scheduled metadata refresh for this
  - Offer interface for partition selection
  - Unanswered questions:
    - What happens if the partition leader changed since the last metadata refresh?

- Consumer

  - Figure out how groups actually work

- Respect the throttle time returned by the server.

- Other questions:

  - The Java NIO selector seems to just drop messages bound for a broker when the connection to the host closes. Is there a reason why?
    - This library will persist the channel for the broker id and only drop messages if the broker id is no longer part of the cluster.
      - Drawback is there's no immediate feedback that a broker might be failing, and requests should retry on a different broker if possible. Maybe there's something we can do about that.
  - What is the difference between `offset` and `sequence` in the context of a `ProduceRequest`?

- Benchmarking

  - See if producer can hit 800k records/s: https://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines

- More tracing

- More tests

- The node task and selector should be IO-agnostic. Make these generic over an `AsyncRead + AsyncWrite` type. Makes it far more testable.

- Think about switching off of kafka-protocol. It is not production-grade atm.
  - It uses `anyhow` for errors. This is not appropriate for a low-level protocol library.
  - It fails to encode instead of ignoring parameters not relevant to newer versions
  - It panicks in situations that should instead return an `Err`
  - It would be nice to decode into `Result<Response, ErrorCode>`
