# Rust FFI port for AMPS message queue client
Supporting basic functionality with Rust FFI calling C client api via a wrapper class struct

## build
 - build a static library of AMPS C/C++ client without name mangling (a.k.a. doing extern "C") on amps.h and put into ./rust_amps/src/ directory
 - invoke ```run.sh``` for rustc manual build, or
 - cargo build/run (check build.rs for more details)

## compatability
developed on rustc 1.78.0 (9b00956e5 2024-04-29)

## Simple publish stress test on 16byte messages:
![image](https://github.com/DoubleSpicy/rust-amps/assets/26112431/4d32d380-ed93-4e28-a87c-92b476e04b12)

