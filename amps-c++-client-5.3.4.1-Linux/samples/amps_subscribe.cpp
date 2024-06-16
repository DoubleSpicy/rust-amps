// amps_subscribe.cpp
//
// This program shows a simple, minimalistic subscriber for receiving AMPS
// messages. The program flow is simple:
//
// * Connect to AMPS
// * Logon
// * Subscribe to all messages published on the "messages" topic.
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

// main
//
// Create a client, connect to the server, subscribe to a topic,
// and then sleep forever while the callback function processes
// the messages.

int main (void)
{

  // Address for the AMPS server.

  const char* uri = "tcp://127.0.0.1:9027/amps/json";

  // Create a client with the name "exampleSubscriber"

  AMPS::Client ampsClient("exampleSubscriber");

  try
  {
    // connect to the server and log on

    ampsClient.connect(uri);
    ampsClient.logon();

    // subscribe to the messages topic

    // This overload of the subscribe method returns a MessageStream
    // that can be iterated over.  When the MessageStream destructor
    // runs, the destructor unsubscribes.


    AMPS::MessageStream stream = ampsClient.subscribe("messages");
    for (auto message = stream.begin(); message != stream.end(); ++message)
    {
      std::cout << "Received message: " << message->getData() << std::endl;
    }

  }
  catch (const AMPS::AMPSException& e)
  {
    std::cerr << e.what() << std::endl;
    exit(1);
  }

  return 0;
}


