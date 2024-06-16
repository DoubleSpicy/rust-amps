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
/// disconnect.cpp
///
/// Illustrates publishing and disconnect handling with the AMPS C++ client.
///

#include <amps/ampsplusplus.hpp>
#include <iostream>

///
/// On disconnect, attempts to reconnect \param client with the uri
/// passed in \param uri.
///
class MyDisconnectHandler
{
  std::string _uri;
public:
  MyDisconnectHandler(const std::string& uri_) : _uri(uri_)
  {
  }

#ifdef AMPS_USE_FUNCTIONAL
  void operator()(AMPS::Client& client_)
  {
    handlerFunction(client_, this);
  }
  void registerMe(AMPS::Client& c_)
  {
    c_.setDisconnectHandler(*this);
  }
#else
  void registerMe(AMPS::Client& c_)
  {
    c_.setDisconnectHandler(AMPS::DisconnectHandler(MyDisconnectHandler::handlerFunction, this));
  }
#endif

private:
  static void handlerFunction(AMPS::Client& client_, void* userData_)
  {
    MyDisconnectHandler* me = (MyDisconnectHandler*)userData_;
    std::cerr << "! Connection failed, reconnecting" << std::endl;

    while (true)
    {
      try
      {
        client_.connect(me->_uri);
        return;
      }
      catch (AMPS::AMPSException&)
      {
      }
    }
  }
};

int main(int argc, char** argv)
{
  if (argc < 2)
  {
    std::cerr << "usage: " << argv[0] << " <amps-uri>" << std::endl;
    return 0;
  }

  AMPS::Client c("sample-client");

  try
  {
    // Set the disconnect handler on our client
    std::string url = argv[1];
    MyDisconnectHandler handler(url);
    handler.registerMe(c);
    c.connect(url);
    c.logon();
    url = argv[1];

    // For performance, we'll construct a single message and send it
    // repeatedly, instead of calling Client.publish() each time.
    AMPS::Message message;
    message.setCommand("publish");
    message.setTopic("pokes");
    message.newCommandId();
    if (url.find("/xml") == std::string::npos)
    {
      message.setData("/1=here's a poke");
    }
    else
    {
      message.setData("<data>here's a poke</data>");
    }

    // If a disconnect occurs, c.send() will call our disconnect handler,
    // which in turn will cause our program to keep re-attemping a
    // connection.  If it success, this loop will continue on as if
    // nothing ever happened.
    for (int i = 0; i < 1000000000; i++)
    {
      if (i % 1000000 == 0)
      {
        std::cout << i << std::endl;
      }
      c.send(message);
    }
  }
  catch (AMPS::AMPSException& ampsException)
  {
    std::cerr << "an AMPS exception occurred: " <<
              ampsException.toString() << std::endl;
  }
  return 0;
}


