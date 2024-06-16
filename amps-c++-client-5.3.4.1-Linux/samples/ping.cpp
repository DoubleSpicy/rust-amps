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
/// ping.cpp
///
/// A C++ client that attempts to connect to a server.  Useful for
/// testing connectivity from your computer to another AMPS server.
///

#include <amps/ampsplusplus.hpp>
#include <iostream>

int main(int argc, char** argv)
{
  if (argc < 2)
  {
    std::cerr << "usage: " << argv[0] << " <amps-uri>" << std::endl;
    return -1;
  }

  try
  {
    // Construct the client.  "pinger" is the client name
    // that will be used to uniquely identify this client to the server.
    AMPS::Client client("pinger");

    // If the server is unavailable, an exception is thrown here.
    client.connect(argv[1]);
    client.logon();
    std::cout << "successfully connected to " << argv[1] << std::endl;
  }
  catch (AMPS::AMPSException& e)
  {
    std::cerr << "An error occurred connecting to " << argv[1] <<
              ". " << e.toString() << std::endl;
    return -1;
  }

  return 0;
}

