extern crate core;

mod rustamps;

use rustamps::rustamps::AmpsClient;

fn main() {
    println!("Rust FFI AMPS client!");
    unsafe{
        let uri = "tcp://127.0.0.1:9007/amps/fix";
        let name = "rustClient";
        let client = AmpsClient::new(name, uri);
        let mut result = client.connect();
        println!("connection result: {}", result);
        let topic = "orders";
        let data = "35=D; 22=5; 55=NVDA.O";
        result = client.publish(topic, data);
        println!("pub result: {}", result);
        while(true){
            result = client.publish(topic, data);
        }
    }


}
