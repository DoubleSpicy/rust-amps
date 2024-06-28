pub mod rustamps{
    use core::ffi::{c_char};
    use std::ffi::{c_void, CStr, CString, c_int};
    
    

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
        _publish_command: CString,
        _msg_type: CString,  
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
                        _publish_command: (Self::cast("publish")),
                        _msg_type: (Self::cast("json")),
                         _msg: (amps_message_create(client_ptr)) }
            }
            
        }
        pub fn connect(&self) -> i32 {
            unsafe{
                let mut result = amps_client_connect(self._client, self.uri.as_ptr());
                if result == 0 {
                    println!("fn connect result: {}", result);
                    let logon = amps_message_create(self._client);
                    amps_message_set_field_value_nts(logon, cast(FieldID::AMPS_Command), self._logon_command.as_ptr());
                    amps_message_set_field_value_nts(logon, cast(FieldID::AMPS_ClientName), self.name.as_ptr());
                    amps_message_set_field_value_nts(logon, cast(FieldID::AMPS_MessageType), self._msg_type.as_ptr());
                    result = amps_client_send(self._client, logon);
                    println!("fn logon result: {}", result);
                }
                return result;
            }
        }
    
        pub fn publish(&self, topic: &str, data: &str) -> i32 {
            let payload: CString = Self::cast(data);
            let topic_cstr = Self::cast(topic);
            unsafe {
                amps_message_set_field_value_nts(self._msg, cast(FieldID::AMPS_Command), self._publish_command.as_ptr());
                amps_message_set_field_value_nts(self._msg, cast(FieldID::AMPS_Topic), Self::cast(topic).as_ptr());
                amps_message_assign_data(self._msg, payload.as_ptr() as *const c_char, data.len() as u64); // TODO: fix val here, wrong encoding?
                amps_message_set_field_value_nts(self._msg, cast(FieldID::AMPS_MessageType), self._msg_type.as_ptr());
                let result = amps_client_send(self._client, self._msg);
                return result;
            }
        }
    
        pub fn subscribe(&self, topic: &str, callback: unsafe extern "C" fn(*mut c_void, *mut c_void)) -> i32{
            // TODO: add subscribe functionality, pass callback function to C code.
            let sub = Self::cast("subscribe");
            let topic_cstr = Self::cast(topic);
            unsafe {
                amps_message_set_field_value_nts(self._msg, cast(FieldID::AMPS_Command), sub.as_ptr());
                amps_message_set_field_value_nts(self._msg, cast(FieldID::AMPS_Topic), topic_cstr.as_ptr());
                amps_client_set_message_handler(self._client, callback, std::ptr::null_mut());
                amps_message_set_field_value_nts(self._msg, cast(FieldID::AMPS_MessageType), self._msg_type.as_ptr());
                let result = amps_client_send(self._client, self._msg);
                return result;
            }
        }
    
        fn cast(x: &str) -> CString {
            return CString::new(x).expect("fail to cast String to CString!");
        }
    }
    
    pub fn get_payload(msg: *mut c_void) -> String {
        let mut data: *mut c_char = std::ptr::null_mut();
        let data_ptr: *mut *mut c_char = &mut data;
        let mut size: u64 = 0;
        let size_ptr: *mut u64 = &mut size;
        let mut str_buf: String = "".to_string();
        unsafe {
            amps_message_get_data(msg, data_ptr, size_ptr);
            let mut c_str: &CStr = CStr::from_ptr(data);
            str_buf = c_str.to_str().unwrap().to_owned();
        }
        let slice = str_buf[0..size as usize].to_string();
        return slice;
    }
}
