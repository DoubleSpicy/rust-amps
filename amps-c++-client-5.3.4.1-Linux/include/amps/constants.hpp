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
#ifndef __AMPS_CONSTANTS_HPP__
#define __AMPS_CONSTANTS_HPP__
namespace AMPS
{
  template <int _>
  struct CommandConstants
  {
    static const char* Values [];
    static unsigned Lengths [];
  };
  template <int _>
  struct AckTypeConstants
  {
    static const unsigned Entries;
    static const char* Values [];
    static unsigned Lengths [];
  };

  template <int _>
  const char* CommandConstants<_>::Values [] =
  {
    "", "publish", "subscribe", "unsubscribe", "sow", "heartbeat",
    "sow_delete", "delta_publish", "logon", "sow_and_subscribe", "delta_subscribe",
    "sow_and_delta_subscribe", "start_timer", "stop_timer", "group_begin", "group_end",
    "oof", "ack", "flush"
  };
  template <int _>
  unsigned CommandConstants<_>::Lengths [] =
  {
    0, 7, 9, 11, 3, 9, 10, 13, 5, 17, 15, 23, 11, 10, 11, 9, 3, 3, 5
  };
  template <int _> const unsigned AckTypeConstants<_>::Entries = 64;
  template <int _>
  const char* AckTypeConstants<_>::Values [] =
  {
    "",                                                    // 000000
    "received",                                            // 000001
    "parsed",                                              // 000010
    "received,parsed",                                     // 000011
    "processed",                                           // 000100
    "received,processed",                                  // 000101
    "parsed,processed",                                    // 000110
    "received,parsed,processed",                           // 000111
    "persisted",                                           // 001000
    "received,persisted",                                  // 001001
    "parsed,persisted",                                    // 001010
    "received,parsed,persisted",                           // 001011
    "processed,persisted",                                 // 001100
    "received,processed,persisted",                        // 001101
    "parsed,processed,persisted",                          // 001110
    "received,parsed,processed,persisted",                 // 001111
    "completed",                                           // 010000
    "received,completed",                                  // 010001
    "parsed,completed",                                    // 010010
    "received,parsed,completed",                           // 010011
    "processed,completed",                                 // 010100
    "received,processed,completed",                        // 010101
    "parsed,processed,completed",                          // 010110
    "received,parsed,processed,completed",                 // 010111
    "persisted,completed",                                 // 011000
    "received,persisted,completed",                        // 011001
    "parsed,persisted,completed",                          // 011010
    "received,parsed,persisted,completed",                 // 011011
    "processed,persisted,completed",                       // 011100
    "received,processed,persisted,completed",              // 011101
    "parsed,processed,persisted,completed",                // 011110
    "received,parsed,processed,persisted,completed",       // 011111
    "stats",                                               // 100000
    "received,stats",                                      // 100001
    "parsed,stats",                                        // 100010
    "received,parsed,stats",                               // 100011
    "processed,stats",                                     // 100100
    "received,processed,stats",                            // 100101
    "parsed,processed,stats",                              // 100110
    "received,parsed,processed,stats",                     // 100111
    "persisted,stats",                                     // 101000
    "received,persisted,stats",                            // 101001
    "parsed,persisted,stats",                              // 101010
    "received,parsed,persisted,stats",                     // 101011
    "processed,persisted,stats",                           // 101100
    "received,processed,persisted,stats",                  // 101101
    "parsed,processed,persisted,stats",                    // 101110
    "received,parsed,processed,persisted,stats",           // 101111
    "completed,stats",                                     // 110000
    "received,completed,stats",                            // 110001
    "parsed,completed,stats",                              // 110010
    "received,parsed,completed,stats",                     // 110011
    "processed,completed,stats",                           // 110100
    "received,processed,completed,stats",                  // 110101
    "parsed,processed,completed,stats",                    // 110110
    "received,parsed,processed,completed,stats",           // 110111
    "persisted,completed,stats",                           // 111000
    "received,persisted,completed,stats",                  // 111001
    "parsed,persisted,completed,stats",                    // 111010
    "received,parsed,persisted,completed,stats",           // 111011
    "processed,persisted,completed,stats",                 // 111100
    "received,processed,persisted,completed,stats",        // 111101
    "parsed,processed,persisted,completed,stats",          // 111110
    "received,parsed,processed,persisted,completed,stats"  // 111111
  };
  template <int _>
  unsigned AckTypeConstants<_>::Lengths [] =
  {
    0,
    8,
    6,
    15,
    9,
    18,
    16,
    25,
    9,
    18,
    16,
    25,
    19,
    28,
    26,
    35,
    9,
    18,
    16,
    25,
    19,
    28,
    26,
    35,
    19,
    28,
    26,
    35,
    29,
    38,
    36,
    45,
    5,
    14,
    12,
    21,
    15,
    24,
    22,
    31,
    15,
    24,
    22,
    31,
    25,
    34,
    32,
    41,
    15,
    24,
    22,
    31,
    25,
    34,
    32,
    41,
    25,
    34,
    32,
    41,
    35,
    44,
    42,
    51
  };
}
#endif
