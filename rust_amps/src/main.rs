// extern crate cfunc;
// use cfunc::output;
extern crate core;
// extern crate cc;
use core::ffi::{c_char};
use core::panic;
use std::ffi::{c_void, CString, c_int};

// use std::os::raw::c_void;
use std::ptr::{null, null_mut};
use std::{ptr, thread, time};


extern "C" {
    fn amps_client_create(name: *const c_char) -> *mut c_void;
    fn amps_client_connect(client: *mut c_void, uri: *const c_char) -> i32;
    fn amps_message_create(client: *mut c_void) -> *mut c_void;
    fn amps_message_set_field_value_nts(msg: *mut c_void, field: i32, val: *const c_char) -> c_void;
    fn amps_message_assign_data(msg: *mut c_void, val: *const c_char, size: u64);
    fn amps_client_send(client:  *mut c_void, msg:  *mut c_void) -> i32;
    fn createClient(name: *const c_char, ptr: *mut c_void);
}

#[derive(Copy, Clone)]
enum FieldID{
    AMPS_Command
    , AMPS_Topic
    , AMPS_CommandId
    , AMPS_ClientName
    , AMPS_UserId
    , AMPS_Timestamp
    , AMPS_Filter
    , AMPS_MessageType
    , AMPS_AckType
    , AMPS_SubscriptionId
    , AMPS_Version
    , AMPS_Expiration
    , AMPS_Heartbeat
    , AMPS_TimeoutInterval
    , AMPS_LeasePeriod
    , AMPS_Status
    , AMPS_QueryID
    , AMPS_BatchSize
    , AMPS_TopNRecordsReturned
    , AMPS_OrderBy
    , AMPS_SowKeys
    , AMPS_CorrelationId
    , AMPS_Sequence
    , AMPS_Bookmark
    , AMPS_Password
    , AMPS_Options
    , AMPS_RecordsInserted
    , AMPS_RecordsUpdated
    , AMPS_SowDelete
    , AMPS_RecordsReturned
    , AMPS_TopicMatches
    , AMPS_Matches
    , AMPS_MessageLength
    , AMPS_SowKey
    , AMPS_GroupSequenceNumber
    , AMPS_SubscriptionIds
    , AMPS_Reason
    , AMPS_Unknown_Field = -1
}



fn cast(v: FieldID) -> i32 {
    v as i32
}



fn getPtr() -> *mut c_void {
    let mut val : c_int = 0;
    let p_val = &mut val as *mut c_int as *mut c_void;
    return p_val;
}

// wrapper class for amps client
pub struct AmpsClient {
    _client: *mut c_void, // pointer to raw client, do not touch
    name: CString,
    uri: CString,
    _logon_command: CString,
    _msg_type_json: CString,
    // _logon_command_ptr: 
}

impl AmpsClient {
    pub fn new(&mut self, name: &str, uri: &str) -> i32 {
        self.name = Self::cast(name);
        self.uri = Self::cast(uri);
        self._logon_command = Self::cast("logon");
        self._msg_type_json = Self::cast("json");
        unsafe{
            self._client = amps_client_create(self.name.as_ptr());
            return 0;
        }
    }
    pub fn connect(&self) -> i32 {
        unsafe{
            let mut result = amps_client_connect(self._client, self.uri.as_ptr());
            if(result != 0){
                result = amps_client_connect(self._client, self.uri.as_ptr());
                let logon_command = amps_message_create(self._client);
                amps_message_set_field_value_nts(logon_command, cast(FieldID::AMPS_Command), self._logon_command.as_ptr());
                amps_message_set_field_value_nts(logon_command, cast(FieldID::AMPS_ClientName), self.name.as_ptr());
                amps_message_set_field_value_nts(logon_command, cast(FieldID::AMPS_MessageType), self._msg_type_json.as_ptr());
                result = amps_client_send(self._client, logon_command);
            }
            return result;
        }
    }

    pub fn publish(&self) -> i32 {
        return 0;
    }

    pub fn subscribe(&self) -> i32{
        // TODO: add subscribe functionality, pass callback function to C code.
        return 0;
    }

    fn cast(x: &str) -> CString {
        return CString::new(x).expect("fail to cast String to CString!");
    }
}



fn main() {
    println!("Hello, world!");
    // cc::Build::new().cpp(true).
    let uri = CString::new("tcp://127.0.0.1:9007/amps/fix").expect("CString::new failed");
    let name = CString::new("myApp").expect("CString::new failed");
    unsafe{
        // let test = String::from("TEST123");
        // let testRaw = CString::new(test).expect("fail!");

        let client = amps_client_create(name.as_ptr());
        let mut result = amps_client_connect(client, uri.as_ptr());
        println!("connection result enum: {}", result);

        let logon_command = amps_message_create(client);
        amps_message_set_field_value_nts(logon_command, cast(FieldID::AMPS_Command), CString::new("logon").expect("new cstr failed").as_ptr());
        amps_message_set_field_value_nts(logon_command, cast(FieldID::AMPS_ClientName), CString::new("RustClient").expect("new cstr failed").as_ptr());
        amps_message_set_field_value_nts(logon_command, cast(FieldID::AMPS_MessageType), CString::new("json").expect("new cstr failed").as_ptr());
        result = amps_client_send(client, logon_command);
        println!("logon result: {}", result);

        while(true){
            let pubMsg = amps_message_create(client);
            let data = CString::new("dataTest").expect("new cstr failed");
            amps_message_set_field_value_nts(pubMsg, cast(FieldID::AMPS_Command), CString::new("publish").expect("new cstr failed").as_ptr());
            amps_message_set_field_value_nts(pubMsg, cast(FieldID::AMPS_Topic), CString::new("orders").expect("new cstr failed").as_ptr());
            amps_message_assign_data(pubMsg, data.as_ptr(), 8);
            result = amps_client_send(client, pubMsg);
            // println!("send result: {}", result);
            
            // let ten_millis = time::Duration::from_millis(100);
            // thread::sleep(ten_millis);
        }
    }


}
