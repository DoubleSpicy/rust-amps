#include <amps.h>
#include <stdio.h>
     void displayError(amps_handle client)
     {
       char textBuffer[512];
       amps_client_get_error(client, textBuffer, sizeof(textBuffer));
       printf("error from AMPS: %s\n", textBuffer);
     }
 
     amps_result receiveMessage(amps_handle message, void* userdata)
     {
       amps_char* data;
       size_t len;
       amps_message_get_data(message, &data, &len);
       fwrite(data, 1, len, stdout);
       fwrite("\n", 1, 1, stdout);
       return AMPS_E_OK;
     }
 
     int main()
     {
       amps_handle client, m, logon;
       const char* uri="tcp://127.0.0.1:9007/amps/fix";
 
       client = amps_client_create("myApp");
       if(amps_client_connect(client, uri) != AMPS_E_OK) {
         displayError(client);
         return -1;
       }
        logon = amps_message_create(client);
        amps_message_set_field_value_nts(logon, AMPS_Command, "logon");
        amps_message_set_field_value_nts(logon, AMPS_ClientName, "C_Client");
        amps_message_set_field_value_nts(logon, AMPS_MessageType, "json");
        amps_client_send(client, logon);

       m = amps_message_create(client);
       amps_message_set_field_value_nts(m, AMPS_QueryID, "1");
       amps_message_set_field_value_nts(m, AMPS_CommandId, "1");
       amps_message_set_field_value_nts(m, AMPS_Command,
         "subscribe");
       amps_message_set_field_value_nts(m, AMPS_Topic, "orders");
    //    amps_message_set_field_value_nts(m, AMPS_Filter,
        //  "/symbol LIKE 'ABCD'");
       amps_client_set_message_handler(client, receiveMessage, NULL);
 
       amps_client_send(client, m);
 
       sleep(10);
       return 0;
     }