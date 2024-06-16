// amps_publish.cpp
//
// This is a minimalist way of publishing messages to a topic in AMPS.
//
// The program flow is simple:
//
// * Connect to AMPS
// * Logon
// * Publish a message to the "messages" topic.
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
// Create a client, create a simple message, connect to the server,
// and publish the message.
//
// Notice that this program produces no output on success.


int main(void)
{
  const char* uri = "tcp://127.0.0.1:9027/amps/json";

  // Construct a client with the name "examplePublisher".

  AMPS::Client ampsClient("examplePublisher");

  // Construct a simple JSON message.

  std::ostringstream message;

  message << "{ \"message\"  \"Hello, World!\" , \"client\" : 1 }";

  try
  {
    // connect to the server and log on
    ampsClient.connect(uri);
    ampsClient.logon();

    // publish messages
    ampsClient.publish("messages", message.str());

  }
  catch (const AMPS::AMPSException& e)
  {
    std::cerr << e.what() << std::endl;
    exit(1);
  }
  return 0;
}
