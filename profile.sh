#!/bin/bash

mkdir -p reports

cargo b --release

cargo run --release --quiet -- -b=localhost:9092 admin delete-topics --topics profiling-topic
cargo run --release --quiet -- -b=localhost:9092 admin create-topics --name profiling-topic -p=6 -r=3

cargo flamegraph -o reports/flamegraph.svg --palette wakeup --flamechart -- -b=localhost:9092 producer random --topic profiling-topic

cargo run --release --quiet -- -b=localhost:9092 admin delete-topics --topics profiling-topic
