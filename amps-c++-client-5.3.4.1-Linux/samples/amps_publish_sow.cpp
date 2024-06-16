// amps_publish_sow.cpp
//
// This sample publishes messages to a topic in AMPS that
// maintains a state-of-the-world (SOW) database.
//
// The program flow is simple:
//
// * Connect to AMPS
// * Logon
// * Publish 10000 messages to the "messages-sow" topic. Each message
//   published has a unique messageNumber.
// * Update several messages by publishing messages with existing
//   messageNumbers. Add an optional field to the new messages.
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

    // Construct 10000 simple JSON messages.
    for (int i = 0 ; i < 10000 ; ++i)
    {

      std::ostringstream message;

      message << "{ \"message\" : \"Hello, World!\", "
              << "\"messageNumber\" : " << i << " } ";

      // publish the message
      ampsClient.publish("messages-sow", message.str());

    }

    // Update messages

    for (int i = 5; i < 10000 ; i += 777)
    {

      std::ostringstream message;

      message << "{ \"message\" : \"Updated, World!\", "
              << "\"messageNumber\" : " << i << ", "
              << "\"OptionalField\" : \"true\" } ";

      ampsClient.publish("messages-sow", message.str());
      std::cout << message.str() << std::endl;
    }
    std::cout << std::endl;

  }
  catch (const AMPS::AMPSException& e)
  {
    std::cerr << e.toString() << std::endl;
    exit(1);
  }
  return 0;
}
