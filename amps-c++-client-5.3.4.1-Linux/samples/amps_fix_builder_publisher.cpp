// amps_fix_builder_publisher.cpp
//
// This sample publishes messages to a topic in AMPS that
// maintains a state-of-the-world (SOW) database.
//
// The program flow is simple:
//
// * Connect to AMPS
// * Logon
// * Build a FIX message with some data.
// * Display the data to the console.
// * Publish the message to the "messages" topic.
//
//  The "messages" topic is configured in config/sample.xml
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

// main
//
// Create a client, create a simple fix message, display the message,
// connect to the server, and publish the message.
//
// Notice that this program produces no output on success.


int main(void)
{
  const char* uri = "tcp://127.0.0.1:9027/amps/fix";

  // Construct a client with the name "FIXPublisher".
  AMPS::Client ampsClient("FIXPublisher");

  // Construct a simple FIX message.
  AMPS::FIXBuilder builder;

  // Add data to the builder
  builder.append(0, "123");
  builder.append(1, "234");
  builder.append(2, "345");

  // Display the data
  std::cout << "Sending data: " << builder.getString() << std::endl;

  try
  {
    // Connect to the server and log on
    ampsClient.connect(uri);
    ampsClient.logon();

    // Publish message to the messages topic
    std::string topic("messages");
    ampsClient.publish(topic, builder.getString());

  }
  catch (const AMPS::AMPSException& e)
  {
    std::cerr << e.what() << std::endl;
    exit(1);
  }
  return 0;
}
