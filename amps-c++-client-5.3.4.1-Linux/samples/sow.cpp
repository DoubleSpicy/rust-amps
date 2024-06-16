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
/// sow.cpp
///
/// Illustrates a SOW query using the AMPS C++ client.
///

#include <amps/ampsplusplus.hpp>
#include <iostream>

// AMPS define for std::atomic<long> unless unsupported
AMPS_ATOMIC_TYPE global_count(0);
AMPS_ATOMIC_TYPE global_elapsed;
#if __cplusplus >= 201100L
std::atomic<bool> global_finished(false);
#else
volatile bool global_finished = false;
#endif

#ifndef WIN32
  #define _InterlockedExchange(ptr, val) __sync_lock_test_and_set(ptr, val);
#endif

void mySowReceiver(const AMPS::Message& message, void*)
{
  static clock_t initTime, finishTime;
  static std::string foo;

  // If we see the last message in our query, record the finish time
  // and tell the main thread we're done.
  if (message.getCommand() == "group_end")
  {
    finishTime = clock();
    global_elapsed = (long)(finishTime - initTime);
    global_finished = true;
    initTime = clock();
  }
  else if (message.getCommand() == "group_begin")
  {
    foo = message.getCommand();
    global_finished = false;
    initTime = clock();
  }
  global_count++;
}

#ifdef _WIN32
  #define usleep(x) Sleep((x)/1000)
#endif

int main(int argc, char** argv)
{
  const char* TOPIC = "a";
  const int SUBSCRIBE_TIMEOUT = 1000;
  const int BATCHSIZE = 100;

  if (argc < 2)
  {
    std::cerr << "usage: " << argv[0] << " <amps-uri>" << std::endl;
    return -1;
  }
  std::cout.precision(2);
  try
  {
    AMPS::Client c("sow client");

    c.connect(argv[1]);
    c.logon();
    for (int i = 0; i < 1000; i++)
    {
      global_finished = false;
      global_count = 0;
      c.sow(AMPS::MessageHandler(mySowReceiver, NULL), TOPIC,
            SUBSCRIBE_TIMEOUT, "", BATCHSIZE);
      while (!global_finished)
      {
        usleep(1000 * 1000);
      }
      double recordsPerSec = ((double)global_count) / ((double)global_elapsed / CLOCKS_PER_SEC);
      std::cout << std::fixed << recordsPerSec <<
                " rps (" << (long)global_count << " records in "
                << global_elapsed << " ticks.)" << std::endl;
    }

  }
  catch (AMPS::AMPSException& ex)
  {
    std::cerr << "an exception occurred: " << ex.toString() << std::endl;
    if (strstr(ex.what(), "Topic") != NULL)
    {
      std::cerr << "The topic " << TOPIC << " does not appear to be a valid SOW topic on this server.\n"
                << "Please review the AMPS User Guide to configure this topic as a SOW topic, and re-run." << std::endl;
    }

  }
  return 0;
}
