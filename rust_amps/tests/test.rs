use rust_amps::rustamps::rustamps::AmpsClient;
use std::time::Instant;

mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_add() {
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
            let now = Instant::now();
            for _ in 0..10000000{
                result = client.publish(topic, data);
            }
            let elapsed = now.elapsed();
            println!("Elapsed: {:.2?}", elapsed);
            assert!(elapsed.as_secs() <= 5);
        }
    }
}