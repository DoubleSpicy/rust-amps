// amps_subscribe_with_replay.cpp
//
// This sample subscribes to a topic in AMPS that maintains
// a transaction log, and requests replay from the transaction log.
//
// The program flow is simple:
//
// * Connect to AMPS
// * Logon
// * Request replay from the "messages-history" topic from the last
//   message received.
//
// To understand this program, start with the subscribe method of the
// bookmark_subscriber.
//
////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2012-2023 60East Technologies Inc., All Rights Reserved.
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
// is subject to the restrictions set forth by 60East Technologies Inc.
//
////////////////////////////////////////////////////////////////////////////

#include <amps/ampsplusplus.hpp>
#include <amps/HAClient.hpp>
#include <amps/DefaultServerChooser.hpp>
#include <amps/RingBookmarkStore.hpp>
#include <iostream>
#include <string>
#ifndef _WIN32
  #include <unistd.h>
#endif
#include <limits.h>
#include <map>

// Callback function for handling messages from AMPS.
//
// * message is the message received from AMPS
//
// * user_data is provided by the subscriber as a way to
//   pass extra information into the message handler.

void handleMessage(const AMPS::Message& message_, void* userData_);

// bookmark_subscriber
//
// This class encapsulates the subscriber in the program, and handles
// the details of tracking the subscription ID and resubscribing at the
// correct point in the transaction log when the program restarts.

class bookmark_subscriber
{
public:


  // bookmark_subscriber
  //
  // Construct an order subscriber with the provided client name.
  //
  // The order subscriber creates a subscription ID to provide to the server,
  // and uses the RingBookmarkStore implementation to keep track of processed
  // messages.

  bookmark_subscriber(const std::string& clientName_ = "orderSubscriber")
    : _clientName(clientName_),
      _subId(clientName_ + "_sub"),
      _bookmarkStore(
        new AMPS::RingBookmarkStore(
          std::string(clientName_ + ".bookmark").c_str())),
      _client(clientName_)
  {
    _client.setBookmarkStore(_bookmarkStore);
  }


  // ~bookmark_subscriber
  //
  // Disconnect the client. Notice that there is no need to explicitly
  // manage the bookmark store.

  ~bookmark_subscriber(void)
  {
    _client.disconnect();
  }


  // connect
  //
  // Wrapper around AMPS connect. This method creates a DefaultServerchooser
  // with the provided URI, so the client will reconnect if it is disconnected.

  void connect(const std::string& uri_)
  {
    // Create a server chooser with the provided URI
    AMPS::ServerChooser chooser(new AMPS::DefaultServerChooser());
    chooser.add(uri_);

    // connect to the server and log on
    _client.setServerChooser(chooser);
    _client.connectAndLogon();
  }

  // subscribe
  //
  // This method handles the details of subscribing to AMPS and requesting
  // replay from the last processed message.

  void subscribe(void)
  {
    const std::string filter;
    const std::string options;

    // Create a Field with the saved subscription ID.
    AMPS::Message::Field subId(_subId.c_str(), _subId.length());

    // subscribe using the last bookmark that was processed
    _client.bookmarkSubscribe(AMPS::MessageHandler(handleMessage, (void*)this),
                              "messages-history",
                              5000,
                              AMPS::Client::BOOKMARK_RECENT(),
                              filter,
                              options,
                              _subId);
  }

  // process
  //
  // Process received messages.

  void process(const AMPS::Message& message_)
  {

    const std::string& data = message_.getData();

    // For production purposes, use a full JSON parser. For sample
    // purposes, we simply find the OrderId.

    size_t order_tag = data.find("\"orderId\"");
    if (order_tag == std::string::npos)
    {
      return;
    }

    size_t order_colon = data.find(':', order_tag + 1);
    if (order_colon == std::string::npos)
    {
      return;
    }

    size_t order_comma = data.find(',', order_colon + 1);
    if (order_comma == std::string::npos)
    {
      return;
    }

    // The value for orderId is between order_colon+1 and order_comma

    int orderId = std::stoi(data.substr(order_colon + 1,
                                        order_comma - (order_colon + 1)));



    std::cout << "Process Order with OrderId = " << orderId << std::endl;

    // Discard the message. Until this method call, the bookmark store
    // doesn't consider the message processed, and will request the
    // message again if the client restarts.

    _bookmarkStore.discard(message_);

  }
protected:
  // Stored client name
  std::string         _clientName;

  // Saved subscription ID, to be used for resuming the subscription.
  std::string         _subId;

  // Bookmark store, used for tracking which messages the program
  // has processed.
  AMPS::BookmarkStore _bookmarkStore;
  AMPS::HAClient      _client;
};

// handleMessage
//
// Wrapper method for processing a message. The program passes in the
// bookmark_subscriber in the userData parameter. This method casts the
// userData back to an bookmark_subscriber, then calls the process method
// on the order subscriber.
//
// This is a standard technique in AMPS message handling.

void handleMessage(const AMPS::Message& message_, void* userData_)
{
  bookmark_subscriber* pOrderSubscriber = reinterpret_cast<bookmark_subscriber*>(userData_);
  pOrderSubscriber->process(message_);
}

// main
//
// Create an bookmark_subscriber, connect to the server, subscribe to a topic,
// and then sleep forever while the callback function processes
// the messages.

int main(int argc_, char** argv_)
{
  const std::string defaultURI = "tcp://127.0.0.1:9027/amps/json";
  try
  {
    const std::string uri = (argc_ == 2) ? std::string(argv_[1]) : defaultURI;

    // create the bookmark_subscriber, connect, and subscribe.

    bookmark_subscriber orderProcessor("order_processor");
    orderProcessor.connect(uri);
    orderProcessor.subscribe();

    // do nothing, all work takes place in the message handler
    volatile int awake = 0;
    while (!awake)
    {
      AMPS_USLEEP(UINT_MAX);
    }
  }
  catch (const AMPS::AMPSException& x_)
  {
    std::cerr << x_.what() << std::endl;
    exit(1);
  }
  return 0;
}

