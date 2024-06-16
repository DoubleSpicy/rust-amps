// amps_query_sow.cpp
//
// This program shows two simple queries of a SOW database.
// The program flow is simple:
//
// * Connect to AMPS
// * Logon
// * Query messages from the SOW topic with a simple filter.
// * Output the messages to the console, while the main thread waits
//   for the query to finish.
// * Query messages from the SOW topic with a different filter.
// * Output the messages to the console, while the main thread waits
//   for the query to finish.
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
#include <amps/Message.hpp>

void printResults(const AMPS::Message& message)
{

  if (message.getCommandEnum() == AMPS::Message::Command::GroupBegin)
  {
    std::cout << "Receiving messages from the SOW." << std::endl;
    return;
  }
  else if (message.getCommandEnum() == AMPS::Message::Command::GroupEnd)
  {
    std::cout << "Done receiving messages from SOW." << std::endl;
    return;
  }
  else
  {
    std::cout << "Received message: " << message.getData() << std::endl;
  }

}

// main
//
// Create a client, connect to the server, query the SOW,
// and then wait for the callback function to process
// the messages.

int main (void)
{

  // Address for the AMPS server.

  const char* uri = "tcp://127.0.0.1:9027/amps/json";

  // Create a client with the name "exampleSowQuery"

  AMPS::Client ampsClient("exampleSowQuery");

  try
  {
    // connect to the server and log on

    ampsClient.connect(uri);
    ampsClient.logon();


    std::cout << std::endl << "-----" << std::endl
              << "Submitting query for messageNumber values that are "
              << "multiples of 10."
              << std::endl << std::endl;


    // query for messages from the messages-sow topic

    AMPS::MessageStream stream = ampsClient.sow("messages-sow",
                                                "/messageNumber % 10 == 0");
    for (auto message = stream.begin(); message != stream.end(); ++message)
    {
      printResults(*message);
    }

    std::cout << std::endl << "-----" << std::endl
              << "Submitting query for messages where "
              << "OptionalField is true."
              << std::endl << std::endl;


    stream = ampsClient.sow("messages-sow", "/OptionalField = 'true'");
    for (auto message = stream.begin(); message != stream.end(); ++message)
    {
      printResults(*message);
    }

    std::cout << std::endl << "-----" << std::endl
              << "Submitting query for both conditions."
              << std::endl << std::endl;

    stream = ampsClient.sow("messages-sow",
                            "/OptionalField = 'true' AND /messageNumber % 10 == 0");
    for (auto message = stream.begin(); message != stream.end(); ++message)
    {
      printResults(*message);
    }
  }
  catch (const AMPS::AMPSException& e)
  {
    std::cerr << e.what() << std::endl;
    exit(1);
  }

  ampsClient.disconnect();
  return 0;
}


