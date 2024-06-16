/** ////////////////////////////////////////////////////////////////////////
 *
 * Copyright (c) 2010-2023 60East Technologies Inc., All Rights Reserved.
 *
 * This computer software is owned by 60East Technologies Inc. and is
 * protected by U.S. copyright laws and other laws and by international
 * treaties.  This computer software is furnished by 60East Technologies
 * Inc. pursuant to a written license agreement and may be used, copied,
 * transmitted, and stored only in accordance with the terms of such
 * license agreement and with the inclusion of the above copyright notice.
 * This computer software or any other copies thereof may not be provided
 * or otherwise made available to any other person.
 *
 * U.S. Government Restricted Rights.  This computer software: (a) was
 * developed at private expense and is in all respects the proprietary
 * information of 60East Technologies Inc.; (b) was not developed with
 * government funds; (c) is a trade secret of 60East Technologies Inc.
 * for all purposes of the Freedom of Information Act; and (d) is a
 * commercial item and thus, pursuant to Section 12.212 of the Federal
 * Acquisition Regulations (FAR) and DFAR Supplement Section 227.7202,
 * Government's use, duplication or disclosure of the computer software
 * is subject to the restrictions set forth by 60East Technologies Inc..
 *
 * ////////////////////////////////////////////////////////////////////// */
#ifndef _AMPS_AMPSURI_H_
#define _AMPS_AMPSURI_H_
#include <stdlib.h>
typedef enum
{
  AMPS_URI_START,
  AMPS_URI_TRANSPORT,
  AMPS_URI_USER,
  AMPS_URI_PASSWORD,
  AMPS_URI_HOST,
  AMPS_URI_PORT,
  AMPS_URI_PROTOCOL,
  AMPS_URI_MESSAGE_TYPE,
  AMPS_URI_OPTION_KEY,
  AMPS_URI_OPTION_VALUE,
  AMPS_URI_ERROR,
  AMPS_URI_END
} amps_uri_part;

/*
*/

typedef struct
{
  const char*   part;
  size_t        part_length;
  amps_uri_part part_id;
} amps_uri_state;

#if defined(_WIN32) && defined(AMPS_SHARED)
  #ifdef AMPS_BUILD
    #define AMPSDLL __declspec(dllexport)
  #else
    #define AMPSDLL __declspec(dllimport)
  #endif
#else
  #define AMPSDLL
#endif
/**
 * Parses an AMPS URI. Not a general-purpose URI parser; only supported
 * for AMPS client uri formats.
 * To use this function call with the URI and uri length, and an amps_uri_state
 * instance that has been memset to 0. On each call, amps_uri_parse will set
 * the part, part_length, and part_id members to the next token found in the
 * uri. part_id is set to AMPS_URI_END when the end of the uri is found,
 * or AMPS_URI_ERROR when an error is encountered.
 * \param pUri_      A pointer to the URI to be parsed.
 * \param uriLength_ The length of the URI string at pUri_.
 * \param pUriState_ A pointer to the amps_uri_state instance which will be
 *                   set to the next token in the URI.
 **/
#ifdef __cplusplus
  extern "C"
#endif
AMPSDLL void amps_uri_parse(const char*     pUri_,
                            size_t          uriLength_,
                            amps_uri_state* pUriState_);

#endif

