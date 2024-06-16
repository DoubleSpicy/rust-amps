////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2012-2024 60East Technologies Inc., All Rights Reserved.
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

#ifndef _MODULE_AMPS_MODULE_H
#define _MODULE_AMPS_MODULE_H

enum AMPSReturnValues { AMPS_SUCCESS = 0x00, AMPS_FAILURE = 0xFF, AMPS_FATAL = 0xFE };

struct amps_module_option
{
  const char* key;
  const char* value;
};

typedef struct amps_module_option* amps_module_options;

enum amps_module_log_level { amps_module_log_level_none      = 0,
                             amps_module_log_level_developer = 1,
                             amps_module_log_level_trace     = 2,
                             amps_module_log_level_stats     = 4,
                             amps_module_log_level_debug     = 8,
                             amps_module_log_level_info      = 8,
                             amps_module_log_level_warning   = 16,
                             amps_module_log_level_error     = 32,
                             amps_module_log_level_critical  = 64,
                             amps_module_log_level_emergency = 128
                           };

typedef void  (*amps_module_logger)(int level, const char* message);
typedef void* (*amps_module_allocator)(unsigned long);

#endif

