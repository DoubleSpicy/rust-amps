extern crate core;

mod rustamps;
use rustamps::rustamps::AmpsClient;
// use libc::fwrite;

use std::{ffi::{c_char, c_void, CStr}, thread, time::Duration};
use std::slice::from_raw_parts;
use std::str::Utf8Error;

unsafe extern "C" fn recvCallback(msg: *mut c_void, _userdata: *mut c_void) {
    // supply this callback function when subscribing a topic
    let mut payload = rustamps::rustamps::get_payload(msg);
    println!("payload: {}", payload);
}


fn main() {
    println!("Rust FFI AMPS client!");
    let _pub_thread = thread::spawn(|| {
        let uri = "tcp://127.0.0.1:9007/amps/fix";
        let name = "rust_publisher";
        let client = AmpsClient::new(name, uri);
        let mut result = client.connect();
        println!("connection result: {}", result);
        let topic = "orders";
        let data = "35=D; 22=5; 55=NVDA.O";
        for _ in 0..1000{
            result = client.publish(topic, data);
            println!("thread1: send result{}", result);
            thread::sleep(Duration::from_millis(1000));
        }
    });

    let _sub_thread = thread::spawn(|| {
            let uri = "tcp://127.0.0.1:9007/amps/fix";
            let name = "rust_subscriber";
            let client = AmpsClient::new(name, uri);
            let mut result = client.connect();
            println!("connection result: {}", result);
            let topic = "orders";
            client.subscribe(topic, recvCallback);
            loop{
                thread::sleep(Duration::from_millis(100));
            }
    });

    // main thread loop
    loop {
        thread::sleep(Duration::from_millis(10000));
        println!("main thread running...");
    }
    



}
