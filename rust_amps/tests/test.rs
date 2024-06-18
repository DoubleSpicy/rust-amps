use rust_amps::rustamps::rustamps::AmpsClient;

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
            while(true){
                result = client.publish(topic, data);
            }
        }
    }

    // #[test]
    // fn test_bad_add() {
    //     // This assert would fire and test will fail.
    //     // Please note, that private functions can be tested too!
    //     assert_eq!(bad_add(1, 2), 3);
    // }
}