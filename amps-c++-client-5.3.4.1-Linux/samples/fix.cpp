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

#include <amps/ampsplusplus.hpp>
#include <iostream>

void receiver(const AMPS::Message& message, void* /*userData*/)
{
  // A FIX parser for the message body.
  AMPS::FIX fix(message.getData());

  // Iterate through the values in the message.
  // first is the tag, second is the value
  for (AMPS::FIX::iterator i = fix.begin(); i != fix.end(); ++i)
  {
    std::cout << '|' << (*i).first << '|' << (*i).second << '$' << std::endl;
  }

  // Or, just populate a map from the message data, and use it.
  AMPS::FIXShredder::map_type mappy =
    AMPS::FIXShredder().toMap(message.getData());

  std::cout << mappy["key"] << std::endl;
}

int main(int argc, char** argv)
{
  if (argc < 2)
  {
    std::cerr << "usage: " << argv[0] << " <amps-uri>" << std::endl;
    return -1;
  }
  std::string url = argv[1];

  if (url.find("/xml") != std::string::npos)
  {
    std::cerr << "The URI specifies an XML message type; please use this example with a FIX or NVFIX message type." << std::endl;
    return -1;
  }

  try
  {
    // Construct the client.  "fixer" is the client name
    // that will be used to uniquely identify this client to the server.
    AMPS::Client client("fixer");

    // If the server is unavailable, an exception is thrown here.
    client.connect(url);
    client.logon();
    std::cout << "successfully connected to " << url << std::endl;

    client.subscribe(AMPS::MessageHandler(receiver, NULL), "baz", 0);

    AMPS::NVFIXBuilder builder;
    builder.append("baz", "100");
    builder.append("bar", "999");
    builder.append("key", "value");
    builder.append("abc", "0123456", 2, 3);

    for (int i = 0; i < 10; i++)
    {
      client.publish("baz", builder);
    }
    AMPS_USLEEP(5 * 1000 * 1000);

  }
  catch (AMPS::AMPSException& e)
  {
    std::cerr << "An error occured connecting to " << url <<
              ". " << e.toString() << std::endl;
    return -1;
  }

  return 0;
}

