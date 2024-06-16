// amps_publish_composite.cpp
//
// Simple example to demonstrate publishing a composite message
// to a topic in AMPS.
//
// The program flow is simple:
//
// * Connect to AMPS, using the transport configured for composite json-binary
//   messages
// * Logon
// * Construct a composite message payload.
// * Publish the message to the "messages" topic.
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
#include <amps/CompositeMessageBuilder.hpp>

#include <vector>

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
  const char* uri = "tcp://127.0.0.1:9027/amps/composite-json-binary";

  // Construct binary data.

  std::vector<double> data;

  for (size_t dIndex = 0; dIndex < 50; ++dIndex)
  {
    if (dIndex <= 1)
    {
      data.push_back(1.0);
      std::cout << dIndex << "=" << data[dIndex] << " ";
      continue;
    }
    std::cout << dIndex << "=" << data[dIndex - 2] + (double)dIndex << " " ;
    data.push_back(data[dIndex - 2] + (double)dIndex);
  }
  std::cout << std::endl;
  std::cout << "initial array : " << std::endl;
  for (auto d = data.begin(); d != data.end(); ++d)
  {
    std::cout << *d << " ";
  }
  std::cout << std::endl;

  // Construct a simple JSON message.

  std::ostringstream json_part;

  json_part << "{ \"binary_type\" :  \"double\""
            << ",\"size\" :" << data.size()
            << ", \"message\": \"Hi, world!\""
            << "}";

  // Create the payload for the composite message.
  AMPS::CompositeMessageBuilder builder;
  builder.append(json_part.str());

  // copy the double array
  builder.append(reinterpret_cast<const char*>(data.data()),
                 data.size() * sizeof(double));

  // Construct a client with the name "exampleCompositePublisher".

  AMPS::Client ampsClient("examplePublisher");

  try
  {
    // connect to the server and log on
    ampsClient.connect(uri);
    ampsClient.logon();

    // publish the message message
    std::string topic("messages");

    // avoid an extra copy by using the pointer / length overload
    ampsClient.publish(topic.c_str(), topic.length(),
                       builder.data(), builder.length());

  }
  catch (const AMPS::AMPSException& e)
  {
    std::cerr << e.what() << std::endl;
    exit(1);
  }
  return 0;
}
