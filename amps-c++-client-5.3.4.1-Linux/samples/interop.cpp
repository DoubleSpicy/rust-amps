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
/// interop.cpp
///
/// A C++ example demonstrating use of the C and C++ APIs together.
///

#include <amps/ampsplusplus.hpp>
#include <iostream>

// for getpeername() and inet_ntoa, below.
#ifdef _WIN32
  #include <winsock2.h>
#else
  #include <sys/socket.h>
  #include <arpa/inet.h>
#endif

int main(int argc, char** argv)
{
  if (argc < 2)
  {
    std::cerr << "usage: " << argv[0] << " <amps-uri>" << std::endl;
    return -1;
  }
  try
  {
    AMPS::Client client("interop");
    client.connect(argv[1]);
    client.logon();

    // fetch the amps_handle for the client.
    amps_handle clientAmpsHandle = client.getHandle();

    if (clientAmpsHandle != NULL)
    {
      amps_handle transportAmpsHandle = amps_client_get_transport(clientAmpsHandle);

      if (transportAmpsHandle != NULL && std::string(argv[1]).find("tcp") == 0)
      {
        AMPS_SOCKET socketFd = amps_tcp_get_socket(transportAmpsHandle);

        // now we can do stuff with the underlying socket
        struct sockaddr_in saddr;
        socklen_t addressLength = sizeof(struct sockaddr_in);
        memset(&saddr, 0, sizeof(struct sockaddr));
        if (!getpeername(socketFd, (struct sockaddr*)&saddr, &addressLength))
        {
          std::cout << "connected to " <<  inet_ntoa(saddr.sin_addr) << std::endl;
        }
        else
        {
          std::cerr << "getpeername() failed." << std::endl;
        }
      }
      else
      {
        std::cerr << "not connected." << std::endl;
      }
    }
    else
    {
      std::cerr << "could not obtain amps_handle." << std::endl;
    }
  }
  catch (AMPS::AMPSException& ex)
  {
    std::cerr << ex.toString() << std::endl;
  }
  return 0;
}
