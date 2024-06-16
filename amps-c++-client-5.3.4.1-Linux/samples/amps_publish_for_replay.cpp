// amps_publish_for_replay.cpp
//
// This sample publishes messages to a topic in AMPS that
// maintains a transaction log.
//
// The program flow is simple:
//
// * Connect to AMPS
// * Logon
// * Publish 1000 messages at a time to the "messages-history" topic. Each
//   message published has a unique orderId. The program waits one second
//   between sets of 1000 messages.
//
//  The "messages-history" topic is configured in config/sample.xml
//  to maintain a transaction log.
//
//  This sample doesn't include error handling, high availability, or
//  connection retry logic.
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
// Notice that this program produces no output on success, and runs
// in the background until the program is stopped.


int main(int argc_, char** argv_)
{
  const std::string defaultURI = "tcp://127.0.0.1:9027/amps/json";
  try
  {
    // Address for the AMPS server.
    const std::string uri = (argc_ == 2) ? std::string(argv_[1]) : defaultURI;

    AMPS::Client ampsClient("replay_publisher");

    // connect to the server and log on
    ampsClient.connect(uri);
    ampsClient.logon();

    // Construct 1000 simple JSON messages.
    size_t orderId = 1;
    const std::string topic = "messages-history";

    // Publish 1000 orders a second.

    while (1)
    {
      std::ostringstream message;
      for (int i = 0 ; i < 1000 ; ++i)
      {
        message.str("");
        message << R"({ "orderId" : )" << orderId << ", "
                << R"(  "symbol" : "IBM" , )"
                << R"(  "size"  : 1000 , )"
                << R"(  "price"  : 190.01 } )" ;

        ampsClient.publish(topic, message.str());
        ++orderId;
      }
      AMPS_USLEEP(1000000);
    }
  }
  catch (const AMPS::AMPSException& e)
  {
    std::cerr << e.toString() << std::endl;
    exit(1);
  }
  return 0;
}
