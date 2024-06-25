extern crate core;

mod rustamps;
use rustamps::rustamps::AmpsClient;
// use libc::fwrite;

use std::{ffi::{c_char, c_void, CStr}, thread, time::Duration};
use std::slice::from_raw_parts;
use std::str::Utf8Error;

unsafe extern "C" fn recvCallback(msg: *mut c_void, _userdata: *mut c_void) {
    let mut data: *mut c_char = std::ptr::null_mut();
    let data_ptr: *mut *mut c_char = &mut data;
    let mut size: u64 = 0; 
    let size_ptr: *mut u64 = &mut size;
    unsafe {
        rustamps::rustamps::amps_message_get_data(msg, data_ptr, size_ptr);
        let mut buf: Vec<c_char> = Vec::new();
        for i in 0..size{
            let c_str = CStr::from_ptr(data);
            print!("{:?}", c_str);
            buf.push(*(data.add(i as usize)));
        }
        // println!("||||end||||");
        println!("\nOK: {:?}", buf);
    }
}

unsafe fn convert_double_pointer_to_vec(
    data: *mut *mut c_char,
    len: u64,
) -> Result<Vec<String>, Utf8Error> {
    from_raw_parts(data, len as usize)
        .iter()
        .map(|arg| CStr::from_ptr(*arg).to_str().map(ToString::to_string))
        .collect()
}

fn main() {
    println!("Rust FFI AMPS client!");
    let _pub_thread = thread::spawn(|| {
        let uri = "tcp://127.0.0.1:9007/amps/fix";
        let name = "rustClient";
        let client = AmpsClient::new(name, uri);
        let mut result = client.connect();
        println!("connection result: {}", result);
        let topic = "orders";
        let data = "0123456789";
        // client.subscribe(recvCallback);
        // println!("pub result: {}", result);
        for _ in 0..1000{
            result = client.publish(topic, data);
            // println!("thread1: send result{}", result);
        }
    });

    let _sub_thread = thread::spawn(|| {
            let uri = "tcp://127.0.0.1:9007/amps/fix";
            let name = "rustClient2";
            let client = AmpsClient::new(name, uri);
            let mut result = client.connect();
            println!("connection result: {}", result);
            let topic = "orders";
            let data = "35=D; 22=5; 55=NVDA.O";
            client.subscribe(recvCallback);
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
