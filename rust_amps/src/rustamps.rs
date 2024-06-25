pub mod rustamps{
    use core::ffi::{c_char};
    use std::ffi::{c_void, CString, c_int};
    
    

    extern "C" {
        pub fn amps_client_create(name: *const c_char) -> *mut c_void;
        pub fn amps_client_connect(client: *mut c_void, uri: *const c_char) -> i32;
        pub fn amps_message_create(client: *mut c_void) -> *mut c_void;
        pub fn amps_message_set_field_value_nts(msg: *mut c_void, field: i32, val: *const c_char) -> c_void;
        pub fn amps_message_assign_data(msg: *mut c_void, val: *const c_char, size: u64);
        pub fn amps_client_send(client:  *mut c_void, msg:  *mut c_void) -> i32;
        pub fn amps_message_get_data(msg: *mut c_void, data: *mut *mut c_char, size: *mut u64);
        pub fn amps_client_set_message_handler(client:  *mut c_void, callback: unsafe extern "C" fn(*mut c_void, *mut c_void), _userdata:  *mut c_void);
    }
    
    #[derive(Copy, Clone)]
    pub enum FieldID{
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
        _publish_command: CString,  
        _msg: *mut c_void,
        // _logon_command_ptr: 
    }
    
    impl AmpsClient {
        pub fn new(name: &str, uri: &str) -> Self {
            let name_cstr = Self::cast(name);
            unsafe{
                let client_ptr = amps_client_create(name_cstr.as_ptr());
                Self { _client: (client_ptr),
                     name: (name_cstr), uri: (Self::cast(uri)),
                      _logon_command: (Self::cast("logon")),
                       _msg_type_json: (Self::cast("json")),
                        _publish_command: (Self::cast("publish")),
                         _msg: (amps_message_create(client_ptr)) }
            }
            
        }
        pub fn connect(&self) -> i32 {
            unsafe{
                let mut result = amps_client_connect(self._client, self.uri.as_ptr());
                if result == 0 {
                    // amps_client_connect(self._client, CString::new("tcp://127.0.0.1:9007/amps/fix").expect("CString::new failed").as_ptr());
                    println!("fn connect result: {}", result);
                    let logon = amps_message_create(self._client);
                    amps_message_set_field_value_nts(logon, cast(FieldID::AMPS_Command), self._logon_command.as_ptr());
                    amps_message_set_field_value_nts(logon, cast(FieldID::AMPS_ClientName), self.name.as_ptr());
                    amps_message_set_field_value_nts(logon, cast(FieldID::AMPS_MessageType), self._msg_type_json.as_ptr());
                    result = amps_client_send(self._client, logon);
                    println!("fn logon result: {}", result);
                }
                return result;
            }
        }
    
        pub fn publish(&self, topic: &str, data: &str) -> i32 {
            unsafe {
                amps_message_set_field_value_nts(self._msg, cast(FieldID::AMPS_Command), self._publish_command.as_ptr());
                amps_message_set_field_value_nts(self._msg, cast(FieldID::AMPS_Topic), Self::cast(topic).as_ptr());
                amps_message_assign_data(self._msg, Self::cast(data).as_ptr() as *const c_char, data.chars().count() as u64); // TODO: fix val here
                let result = amps_client_send(self._client, self._msg);
                return result;
            }
        }
    
        pub fn publish2(&self) -> i32 {
            unsafe {
                let pubMsg = amps_message_create(self._client);
                let data = CString::new("dataTest").expect("new cstr failed");
                amps_message_set_field_value_nts(pubMsg, cast(FieldID::AMPS_Command), CString::new("publish").expect("new cstr failed").as_ptr());
                amps_message_set_field_value_nts(pubMsg, cast(FieldID::AMPS_Topic), CString::new("orders").expect("new cstr failed").as_ptr());
                amps_message_assign_data(pubMsg, data.as_ptr(), 8);
                let result = amps_client_send(self._client, pubMsg);
                return result;
            }
        }
    
        pub fn subscribe(&self, callback: unsafe extern "C" fn(*mut c_void, *mut c_void)) -> i32{
            // TODO: add subscribe functionality, pass callback function to C code.
            unsafe {
                amps_message_set_field_value_nts(self._msg, cast(FieldID::AMPS_QueryID), Self::cast("1").as_ptr());
                // amps_message_set_field_value_nts(self._msg, cast(FieldID::AMPS_CommandId), Self::cast("auto1").as_ptr());
                amps_message_set_field_value_nts(self._msg, cast(FieldID::AMPS_Command), Self::cast("subscribe").as_ptr());
                amps_message_set_field_value_nts(self._msg, cast(FieldID::AMPS_Topic), Self::cast("orders").as_ptr());
                amps_client_set_message_handler(self._client, callback, std::ptr::null_mut());
                let result = amps_client_send(self._client, self._msg);
                return result;
            }
        }
    
        fn cast(x: &str) -> CString {
            return CString::new(x).expect("fail to cast String to CString!");
        }
    }
    
}
