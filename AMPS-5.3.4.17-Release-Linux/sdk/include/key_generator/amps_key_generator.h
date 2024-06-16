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

#ifndef _AMPS_KEY_GENERATOR_H
#define _AMPS_KEY_GENERATOR_H

#include <module/amps_module.h>
#include <message_type/amps_message_type.h>
#include <amps_api.h>

///
/// @file actions/amps_key_generator.h
/// @brief Core type and function declarations for the AMPS Key Generator Plugin Module API.
///


typedef void* amps_key_generator_context;

typedef void* amps_key_generator_sow_key;

typedef void (*amps_sow_hash_function)(const char*, size_t, amps_key_generator_sow_key);

#endif

