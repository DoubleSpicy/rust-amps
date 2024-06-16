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
/// functional.cpp
///
/// An example of using C++ function objects and lambda functions
/// with the AMPS C++ client.
///

#include <amps/ampsplusplus.hpp>
#include <iostream>

#ifndef AMPS_USE_FUNCTIONAL
#warning AMPS_USE_FUNCTIONAL undefined.  Use a newer compiler to build this sample.
int main()
{
  std::cerr << "This sample is unavailable; AMPS_USE_FUNCTIONAL is not defined.\n"
            << "Try rebuilding with a newer compiler." << std::endl;
  return -1;
}
#else

///
/// myMessageHandler is a global function that we'll use with std::bind()
///
void myMessageHandler(const AMPS::Message& message, AMPS_ATOMIC_TYPE& count,
                      const std::string& expectedTopic)
{
  count++;
  if (message.getTopic() != expectedTopic)
  {
    std::cout << "myMessageHandler got message " <<
              message.getTopic() << " expected " << expectedTopic << std::endl;
  }
}

///
/// MyMessageHandlerClass is a regular C++ class that we'll bind into
/// an AMPS message handler
///
class MyMessageHandlerClass
{
  std::string _expected;
  // AMPS define for std::atomic<long> unless unsupported
  AMPS_ATOMIC_TYPE& _count;
public:
  MyMessageHandlerClass(const std::string& expected, AMPS_ATOMIC_TYPE& count) :
    _expected(expected), _count(count)
  {
  }

  void VerifyExpected(const AMPS::Message& message)
  {
    _count++;
    if (message.getTopic() != _expected)
    {
      std::cout << "myMessageHandlerClass got message " << message.getTopic()
                << " expected " << _expected << std::endl;
    }
  }
};


int main(int argc, char** argv)
{
  if (argc < 2)
  {
    std::cerr << "usage: " << argv[0] << " <amps-uri>" << std::endl;
    return -1;
  }
  try
  {
    const int total = 20;
    AMPS_ATOMIC_TYPE count(0);
    const int timeout = 100000;
    std::string url = argv[1];

    AMPS::Client c("functional");
    c.connect(url);
    c.logon();

    // subscribe global "myMessageHandler" to the "pokes" topic.
    // we pass in a reference to count, so we have to use std::ref,
    // otherwise count is passed by value.  Placeholder _1 is filled
    // by AMPS with the received Message
    AMPS::Message message;
    message.setTopic("pokes");
    message.setCommand("subscribe");
    c.send(std::bind(myMessageHandler, std::placeholders::_1, std::ref(count), "pokes"),
           message, timeout);

    // We bind the member function VerifyExpected to an instance of
    // MyMessageHandlerClass.  The placeholder _1 is filled with the
    // received message, again
    MyMessageHandlerClass mh("jokes", count);
    c.subscribe(
      std::bind(&MyMessageHandlerClass::VerifyExpected, std::ref(mh), std::placeholders::_1),
      "jokes", timeout);

    // Bind the "oaks" topic to a lambda function, if they are available.
    // Macro AMPS_USE_LAMBDAS is set in amps.h, based on compiler version
#ifdef AMPS_USE_LAMBDAS
    auto lambda = [&](const AMPS::Message & m)
    {
      std::cout << "lambda saw some mighty " << m.getTopic() << std::endl;

      // count gets automatically bound in from our parent scope,
      // and by-ref because of [&]
      ++count;
    };

    c.subscribe(lambda, "oaks", timeout);
#endif
    bool xml = url.find("/xml") != std::string::npos;

    // now load us up with messages
    for (int i = 0; i < total / 2; i++)
    {
      c.publish("pokes", xml ? "<data>here's a poke</data>" : "1=here's a poke!");
      c.publish("jokes", xml ? "<data>that's funny.</data>" : "1=that's funny.");
    }
    // This publish ought to hit our lambda function, if
    // it is registered.
    c.publish("oaks", xml ? "<data>A beautiful specimen.</data>" : "1=a beautiful specimen.");
    while (count < total)
    {
      AMPS_USLEEP(10000);
    }
    std::cout << "got " << count << " messages." << std::endl;
    std::cout << "finished" << std::endl;

  }
  catch (AMPS::AMPSException& e)
  {
    std::cerr << "an error occurred: " << e.toString() << std::endl;
  }

  return 0;
}
#endif // AMPS_USE_FUNCTIONAL
