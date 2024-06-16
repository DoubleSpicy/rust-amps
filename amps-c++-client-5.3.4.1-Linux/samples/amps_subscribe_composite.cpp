// amps_subscribe_composite.cpp
//
// This program demonstrates a simple way to parse the composite messages
// published by amps_publish_composite.cpp
//
// Notice that the program assumes that received payload is in the expected
// format, and does not validate that assumption.
//
// * Connect to AMPS, using the transport configured for composite json-binary
//   messages
// * Logon
// * Subscribe to all messages published on the "messages" topic.
// * Parse the composite message body
// * Output the messages to the console
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
#include <iostream>
#include <string>
#include <vector>

#include <amps/CompositeMessageParser.hpp>

// main
//
// Create a client, connect to the server, subscribe to a topic,
// and then process the messages.

int main (void)
{

  // Address for the AMPS server.

  const char* uri = "tcp://127.0.0.1:9027/amps/composite-json-binary";

  // Create a client with the name "exampleCompositeSubscriber"

  AMPS::Client ampsClient("exampleCompositeSubscriber");

  try
  {
    // connect to the server and log on

    ampsClient.connect(uri);
    ampsClient.logon();

    // subscribe to the messages topic

    // This overload of the subscribe method returns a MessageStream
    // that can be iterated over.  When the MessageStream destructor
    // runs, the destructor unsubscribes.

    AMPS::CompositeMessageParser parser;

    AMPS::MessageStream stream = ampsClient.subscribe("messages");
    for (auto message = stream.begin(); message != stream.end(); ++message)
    {

      parser.parse(*message);
      std::string json_part(parser.getPart(0));
      AMPS::Field binary = parser.getPart(1);

      std::vector<double> vec;
      double* array_start = (double*)binary.data();
      double* array_end = array_start + (binary.len() / sizeof(double));

      vec.insert(vec.end(), array_start, array_end);

      std::cout << "Received message with " << parser.size() << " parts"
                << std::endl
                << json_part
                << std::endl;
      for (auto d = vec.begin(); d != vec.end(); ++d)
      {
        std::cout << *d << " ";
      }
      std::cout << std::endl;
    }

  }
  catch (const AMPS::AMPSException& e)
  {
    std::cerr << e.what() << std::endl;
    exit(1);
  }

  return 0;
}


