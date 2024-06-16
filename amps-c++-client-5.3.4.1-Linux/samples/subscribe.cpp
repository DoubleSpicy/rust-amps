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
// Government's use, duplication or disclosure of the computer software
// is subject to the restrictions set forth by 60East Technologies Inc..
//
////////////////////////////////////////////////////////////////////////////

///
/// subscribe.cpp
///
/// Illustrates the basics of subscribing to an AMPS topic.
///
#include <amps/ampsplusplus.hpp>
#include <iostream>

// AMPS define for std::atomic<long> unless unsupported
AMPS_ATOMIC_TYPE global_count(0);

void myHandler(const AMPS::Message& message, void* userData)
{
  global_count++;

  if (message.getTopic() != (char*)userData)
  {
    std::cout << "got message " << message.getTopic() << " expected "
              << (char*)userData << std::endl;
  }
}


int main(int argc, char** argv)
{
  if (argc < 2)
  {
    std::cerr << "usage: " << argv[0] << " <amps-uri>" << std::endl;
    return -1;
  }
  try
  {
    const int total = 20000000;

    AMPS::Client c("subscribe");
    const char* data1 = "pokes", *data2 = "jokes";

    std::string url = argv[1];
    c.connect(url);
    c.logon();
    bool xml = url.find("/xml") != std::string::npos;

    c.subscribe(AMPS::MessageHandler(myHandler, (void*)data1), "pokes",
                10000);
    c.subscribe(AMPS::MessageHandler(myHandler, (void*)data2), "jokes",
                10000);
    AMPS_ATOMIC_BASE_TYPE lastprint  = 0;

    // now load us up with messages
    for (int i = 0; i < total / 2; i++)
    {
      AMPS_USLEEP(10);
      c.publish("pokes", xml ? "<data>poke</data>" : "1=here's a poke");
      c.publish("jokes", xml ? "<data>that's funny</data>" : "1=that's funny.");
      if (global_count - lastprint > 1000)
      {
        lastprint = global_count;
        std::cout << "received " << lastprint << std::endl;
      }
    }
    while (global_count < total)
    {
      AMPS_USLEEP(10000000);
      if (global_count - lastprint > 1000)
      {
        lastprint = global_count;
        std::cout << "got " << lastprint << std::endl;
      }
    }
    std::cout << "finished" << std::endl;

  }
  catch (AMPS::AMPSException& e)
  {
    std::cerr << "an error occurred: " << e.toString() << std::endl;
  }


  return 0;
}

