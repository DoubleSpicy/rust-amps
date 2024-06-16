// amps_update_for_oof.cpp
//
// This sample publishes messages to a topic in AMPS that
// maintains a state-of-the-world (SOW) database.
//
// The program flow is simple:
//
// * Connect to AMPS
// * Logon
// * Publish a set of messages to the "messages-sow" topic:
//   - Publish a message that will expire.
//   - Publish a message that will later be deleted
//   - Publish a set of messages intended to match the subscriber filter.
// * Make changes to the messages so that OOF messages are generated:
//   - Update the set of messages so they no longer match the filter.
//   - Delete a message.
//
//  The "messages-sow" topic is configured in config/sample.xml
//  to maintain a SOW database, where each messageNumber is a
//  unique message.
//
// This sample doesn't include error handling, high availability, or
// connection retry logic.
//
// This file is a part of the AMPS C++ Client distribution samples.
////////////////////////////////////////////////////////////////////////////
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
// Government’s use, duplication or disclosure of the computer software
// is subject to the restrictions set forth by 60East Technologies Inc..
//
////////////////////////////////////////////////////////////////////////////
#include <amps/ampsplusplus.hpp>
#include <string>
#include <iostream>
#include <sstream>

#ifndef _WIN32
  #include <unistd.h>
#endif

void handleDeletedMessage(const AMPS::Message& /*message*/, void* /*user_data*/)
{
  // For sample purposes, do nothing.
}

// main
//
// Create a client, connect to the server, create simple messages,
// and publish those messages.
//
// Notice that this program produces no output on success.


int main(void)
{
  const char* uri = "tcp://127.0.0.1:9027/amps/json";

  // Construct a client with the name "exampleSOWPublisher".

  AMPS::Client ampsClient("exampleSOWPublisher");

  try
  {

    // connect to the server and log on
    ampsClient.connect(uri);
    ampsClient.logon();

    std::cerr << "logged on";

    //publish a message with expiration set.

    AMPS::Message m = AMPS::Message();
    m.setExpiration("1");
    m.setCommand("publish");
    m.newCommandId();
    m.setTopic("messages-sow");
    m.setData("{ \"messageNumber\" : 50000, "
              "\"message\" : \"Here and then gone...\" }");
    ampsClient.send(m);

    std::cerr << "sent expiring message";

    // publish a message to be deleted later on.
    ampsClient.publish("messages-sow",
                       "{ \"messageNumber\" : 500, "
                       "  \"message\" : \"I've got a bad feeling "
                       "about this...\" }");


    std::cerr << "sent message to be deleted";

    // publish two sets of messages, the first one to match the
    // subscriber filter, the next one to make messages no longer
    // match the subscriber filter.

    // the first set of messages is designed so that the
    // sample that uses OOF tracking receives updated messages.

    for (int i = 0; i < 10000 ; i += 1250)
    {
      std::ostringstream message;

      message << "{ \"message\" : \"Hello, World!\", "
              << "\"messageNumber\" : " << i << ", "
              << "\"OptionalField\"=\"true\"";

      ampsClient.publish("messages-sow", message.str());
    }
    std::cerr << "sent first set of messages";

    // the second set of messages will cause OOF messages for the
    // sample that uses OOF tracking.

    for (int i = 0; i < 10000 ; i += 1250)
    {
      std::ostringstream message;

      message << "{ \"message\" : \"Updated, World!\", "
              << "\"messageNumber\" : " << i <<  ", "
              << "\"OptionalField\" : \"ignore_me\" }";

      ampsClient.publish("messages-sow", message.str());
    }
    std::cerr << "sent second set of messages";

    // also delete a message to generate an OOF for the
    // subscriber

    ampsClient.sowDelete(
      AMPS::MessageHandler(handleDeletedMessage, NULL),
      "messages-sow",
      "/messageNumber == 500",
      0);

    // wait up to 2 seconds for all messages to be published
    std::cerr << "sent delete message";

    AMPS_USLEEP(2000000);

    // disconnect the client
    ampsClient.disconnect();

  }
  catch (const AMPS::AMPSException& e)
  {
    std::cerr << "Error!" << std::endl;
    std::cerr << e.toString() << std::endl;
    exit(1);
  }
  return 0;
}
