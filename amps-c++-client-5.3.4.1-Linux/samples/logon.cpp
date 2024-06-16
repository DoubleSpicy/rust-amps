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
/// logon.cpp
///
/// A C++ example of connecting and logging on to an AMPS server.
///

#include <amps/ampsplusplus.hpp>
#include <iostream>

class MyAuthenticator : public AMPS::DefaultAuthenticator
{
public:
  std::string authenticate(const std::string& username, const std::string& password)
  {
    std::cout << "authenticating " << username << std::endl;
    return password;
  }

  std::string retry(const std::string& username, const std::string& password)
  {
    std::cout << "retry received " << username << " " << password << std::endl;
    return password;
  }

  void completed(const std::string& username, const std::string& password, const std::string& reason)
  {
    std::cout << "success!  the server returned " << username << " and " << password << ", with reason " << reason << "." << std::endl;
  }
};

int main(int argc, char** argv)
{
  if (argc < 3)
  {
    std::cerr << "usage: " << argv[0] << " <amps-uri> <client-name>" << std::endl;
    std::cerr << "where amps-uri is: tcp://[user[:password]@]server:port/protocol" << std::endl;
    std::cerr << std::endl << "example: " << argv[0] << " tcp://me:Secret@localhost:9007/amps logon-sample-client" << std::endl;
    return -1;
  }
  try
  {
    AMPS::Client client(argv[2]);
    for (int i = 0; i < 10000; i++)
    {
      MyAuthenticator authenticator;
      client.connect(argv[1]);
      client.logon(50000, authenticator);
      std::cout << client.getName() << " logged on." << std::endl;
      client.disconnect();
    }
  }
  catch (AMPS::AMPSException& ex)
  {
    std::cout << ex.toString() << std::endl;
  }
  return 0;
}
