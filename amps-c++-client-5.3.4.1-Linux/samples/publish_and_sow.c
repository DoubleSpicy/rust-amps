/*//////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2010-2023 60East Technologies Inc., All Rights Reserved.
//
// This computer software is owned by 60East Technologies Inc. and is
// protected by U.S. copyright laws and other laws and by international
// treaties.  This computer software is furnished by 60East Technologies
// Inc. pursuant to a written license agreement and may be used, copied,
// transmitted, and stored only in accordance with the terms of such
// license agreement and with the inclusion of the above copyright notice.
// This computer software or any other copies thereof may not be provided
// or otherwise made available to any other person.
//
// U.S. Government Restricted Rights.  This computer software: (a) was
// developed at private expense and is in all respects the proprietary
// information of 60East Technologies Inc.; (b) was not developed with
// government funds; (c) is a trade secret of 60East Technologies Inc.
// for all purposes of the Freedom of Information Act; and (d) is a
// commercial item and thus, pursuant to Section 12.212 of the Federal
// Acquisition Regulations (FAR) and DFAR Supplement Section 227.7202,
// Government's use, duplication or disclosure of the computer software
// is subject to the restrictions set forth by 60East Technologies Inc..
//
////////////////////////////////////////////////////////////////////////////

///
/// publish_and_sow.c
///
/// AMPS C client sample that shows connection, message creation
/// subscription and disconnect handling.

// <amps.h> is the file to include for both C and C++.
*/
#include <amps/amps.h>
#ifndef AMPS_C_CLIENT_NAMESPACE
#include <string.h>
#include <time.h>
#include <stdio.h>
#ifndef _WIN32
  #include <unistd.h>
#endif

/*
///
/// Callback function for disconnects from the server.
/// \param userData is passed the URI of the server we wish to reconnect to.
///
*/
amps_result MyDisconnectHandler(amps_handle client, void* userData)
{
  amps_handle message;

  /* Keep trying to reconnect, forever, sleeping for a second
  // between attempts. */
  while (amps_client_connect(client, (char*)userData) != AMPS_E_OK)
  {
    AMPS_USLEEP(1000 * 1000);
  }

  /* Now that we've reconnected, issue a new SOW command.
  // Create a new message. */
  message = amps_message_create(client);
  /* set the ClientName to "joe bob" */
  amps_message_set_field_value_nts(message, AMPS_ClientName, "job bob");
  amps_message_set_field_value_nts(message, AMPS_CommandId, "1");
  amps_message_set_field_value_nts(message, AMPS_QueryID, "1");
  amps_message_set_field_value_nts(message, AMPS_Command, "sow");
  amps_message_set_field_value_nts(message, AMPS_Topic, "a");
  amps_message_set_field_value_nts(message, AMPS_BatchSize, "100");
  printf("resubscribing...\n");
  amps_client_send(client, message);
  amps_message_destroy(message);
  printf("sent new sow.\n");

  /* If we return other than AMPS_E_OK, AMPS decides we've given up
  // and will begin shutting down the client, stopping the reader thread
  // and forcing the other side of the socket down, if it's still open. */
  return AMPS_E_OK;
}

/*
///
/// MyMessageHandler is a callback function that receives messages as they
/// arrive from the network connection.
/// \param userData is passed a pointer to a long from main().
*/
amps_result MyMessageHandler(amps_handle message, void* userData)
{
  (*(volatile long*)userData)++;
  return AMPS_E_OK;
}


int main(int argc, char** argv)
{
  char buffer[128], errorBuffer[1024];
  amps_handle client, message, sendMessage;
  const int iters = 1000;
  int i;
  volatile long readMessages = 0;
  clock_t time;
  amps_result rc;

  if (argc < 2)
  {
    printf("usage: %s <amps-uri>\n", argv[0]);
    return -1;
  }

  /* Once a client is created, we immediately set a disconnect handler on it. */
  client = amps_client_create("sample-client");
  amps_client_set_disconnect_handler(client,
                                     (amps_handler)MyDisconnectHandler, argv[1]);

  /* Now, connect and write an error if we're unable to. */
  rc = amps_client_connect(client, argv[1]);
  if (rc != AMPS_E_OK)
  {
    amps_client_get_error(client, errorBuffer, sizeof(errorBuffer));
    printf("%s\n", errorBuffer);
    goto destroyclient;
  }

  /* Create a message for us to send.  This will be a simple publish. */
  sendMessage = amps_message_create(client);
  amps_message_set_field_value_nts(sendMessage, AMPS_ClientName, "shyronnie");
  amps_message_set_field_value_nts(sendMessage, AMPS_CommandId, "1234567890");
  amps_message_set_field_value_nts(sendMessage, AMPS_Command, "publish");
  amps_message_set_field_value_nts(sendMessage, AMPS_Topic, "pokes");
  memset(buffer, 'a', sizeof(buffer));
  amps_message_set_data(sendMessage, buffer, sizeof(buffer));

  /* We'll create another message to perform a SOW query */
  message = amps_message_create(client);
  amps_message_set_field_value_nts(message, AMPS_ClientName, "bigjoe");
  amps_message_set_field_value_nts(message, AMPS_CommandId, "joe1");
  amps_message_set_field_value_nts(message, AMPS_QueryID, "joe1");
  amps_message_set_field_value_nts(message, AMPS_Command, "sow");
  amps_message_set_field_value_nts(message, AMPS_Topic, "a");
  amps_message_set_field_value_nts(message, AMPS_BatchSize, "100");

  /* Unlike the C++ client, our message handler is called for *all*
  // messages received from the AMPS server.  It is our job to figure
  // out what subscription or query any given message corresponds to. */
  amps_client_set_message_handler(client, MyMessageHandler, (void*)&readMessages);

  /* In each iteration, we issue a new SOW query, wait for 500,000 messages
  // to arrive, and repeat.  While we wait for the SOW results to be
  // returned, we also publish a message as fast as we can. */
  for (i = 0; i < iters; i++)
  {
    readMessages = 0;
    /* begin the SOW. */
    rc = amps_client_send(client, message);
    if (rc != AMPS_E_OK)
    {
      amps_client_get_error(client, errorBuffer, sizeof(errorBuffer));
      printf("%s\n", errorBuffer);
      goto destroymessage;
    }

    time = clock();
    while (readMessages < 500000)
    {
      /* Stay busy by publishing messages back to AMPS. */
      if (amps_client_send(client, sendMessage) != AMPS_E_OK)
      {
        amps_client_get_error(client, errorBuffer, sizeof(errorBuffer));
        printf("%s\n", errorBuffer);
      }
    }
    time = clock() - time;
    printf("read %ld messages in %ld (%f/sec)\n",
           readMessages, time, ((double)readMessages / (double)time)*CLOCKS_PER_SEC);
  }


destroymessage:
  amps_message_destroy(message);

destroyclient:
  amps_client_disconnect(client);
  amps_client_destroy(client);

  return 0;
}
#else
int main(int argc, char** argv)
{
  printf("Program doesn't build when AMPS_C_CLIENT_NS is used to put a namespace on the client library.\n");
}
#endif

