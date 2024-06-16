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
/// exceptions.cpp
///
/// Illustrates use of many of the exception types in AMPS, for
/// finer-grained control over exception processing.
///

#include <amps/ampsplusplus.hpp>
#include <iostream>

class Application : public AMPS::ExceptionListener
{
  AMPS::Client client_;
  static void handleMessage(const AMPS::Message& message, void* userData);
public:
  Application(const std::string& uri);
  void run(const std::string& topic, const std::string& filter);
  void exceptionThrown(const std::exception& ex) const;
};

Application::Application(const std::string& uri)
  : client_("My application")
{
  // Go ahead and connect up to the thing.
  client_.setExceptionListener(*this);
  client_.connect(uri);
  client_.logon();
}

void
Application::run(const std::string& topic, const std::string& filter)
{
  // This is a simple app -- we'll set up a subscription to the topic
  // and provide a filter if asked for it.  Then, we'll just sit there
  // for a few seconds while messages come in, and then be done.
  client_.subscribe(
    AMPS::MessageHandler(&Application::handleMessage, this),
    topic,
    10000, // timeout
    filter);

  // Now just wait it out.
  AMPS_USLEEP( 5 * 1000 * 1000 );
}

void
Application::handleMessage(const AMPS::Message& message, void* /*unused*/)
{
  std::cout << "-- received " << message.getTopic() << ": " <<
            message.getData() << std::endl;
}

void
Application::exceptionThrown(const std::exception& ex) const
{
  std::cout << "AMPS caught an exception: " << ex.what() << std::endl;
}

int main(int /*argc*/, char** /*argv*/)
{
  // we'll set this to true if we want to give the progam another shot.
  std::string uri, topic, filter;
  bool askForFilter = true;

  while (true)
  {
    try
    {
      // Ask for a URI, then try connecting.
      if (uri.empty())
      {
        std::cout << "Enter an AMPS uri: " << std::flush;
        if (!std::getline(std::cin, uri))
        {
          return 0;
        }
      }
      Application myApp(uri);

      // Ask for a topic and filter
      if (topic.empty())
      {
        std::cout << "Enter a topic name: " << std::flush;
        if (!std::getline(std::cin, topic))
        {
          return 0;
        }
      }
      if (askForFilter)
      {
        std::cout << "Enter a filter, or <RETURN> for none: " << std::flush;
        if (!std::getline(std::cin, filter))
        {
          return 0;
        }
        askForFilter = false;
      }

      myApp.run(topic, filter);
      std::cout << "finished." << std::endl;
      return 0;
    }
    catch (AMPS::InvalidURIException& e)
    {
      std::cout << "uri '" << uri << "' is an invalid uri.  " << e.toString() << std::endl;
      uri.erase();
    }
    catch (AMPS::TransportTypeException& e)
    {
      std::cout << e.toString() << std::endl;
      uri.erase();
    }
    catch (AMPS::ConnectionRefusedException& e)
    {
      std::cout << "Connection refused (error was: " << e.toString() << ").  " << std::endl;
      uri.erase();
    }
    catch (AMPS::BadRegexTopicException& e)
    {
      std::cout << e.toString() << std::endl;
      topic.erase();
    }
    catch (AMPS::InvalidTopicException& e)
    {
      std::cout << e.toString() << std::endl;
      topic.erase();
    }
    catch (AMPS::BadFilterException& e)
    {
      std::cout << e.toString() << std::endl;
      filter.erase();
      askForFilter = true;
    }
    catch (AMPS::AMPSException& e)
    {
      std::cout << e.toString() << std::endl;
      break;
    }
  }

  return -1;

}




