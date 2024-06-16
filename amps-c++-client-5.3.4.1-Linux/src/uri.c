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
#include <amps/ampsuri.h>
#include <assert.h>

AMPSDLL void
amps_uri_parse(const char* pUri_, size_t uriLength_, amps_uri_state* pUriState_)
{
  const char* ch = NULL;
  const char* pEnd = pUri_ + uriLength_;
  if (pUriState_->part_id == AMPS_URI_START)
  {
    pUriState_->part = pUri_;
    pUriState_->part_length = 0;
  }
  ch = pUriState_->part + pUriState_->part_length;
  switch (pUriState_->part_id)
  {
  case AMPS_URI_START:
    while (ch < pEnd)
    {
      if (*ch == ':')
      {
        pUriState_->part_length = (size_t)(ch - pUri_);
        pUriState_->part_id     = AMPS_URI_TRANSPORT;
        return;
      }
      ++ch;
    }
    pUriState_->part_id = AMPS_URI_ERROR;
    break;
  case AMPS_URI_TRANSPORT:
    ++ch;
    while (ch < pEnd && *++ch == '/');
    pUriState_->part = ch;
    while (ch < pEnd)
    {
      if (*ch == '[')
      {
        pUriState_->part = ++ch;
        while (ch < pEnd && *++ch != ']');
        pUriState_->part_id = ch >= pEnd ? AMPS_URI_ERROR : AMPS_URI_HOST;
        pUriState_->part_length = (size_t)(ch - pUriState_->part);
        return;
      }
      else if (*ch == ':')
      {
        const char* next = NULL;
        pUriState_->part_length = (size_t)(ch - pUriState_->part);
        next = ch + 1;
        while (next < pEnd)
        {
          if (*next == '@')
          {
            pUriState_->part_id = AMPS_URI_USER;
            return;
          }
          if (*next == '/')
          {
            pUriState_->part_id = AMPS_URI_HOST;
            return;
          }
          ++next;
        }
      }
      else if (*ch == '@')
      {
        pUriState_->part_length = (size_t)(ch - pUriState_->part);
        pUriState_->part_id = AMPS_URI_USER;
        return;
      }
      ++ch;
    }
    pUriState_->part_id = AMPS_URI_ERROR;
    break;
  case AMPS_URI_USER:
    if (*ch == '@')
    {
      ++ch;
      if (*ch == '[')
      {
        pUriState_->part = ++ch;
        while (ch < pEnd && *++ch != ']');
      }
      else
      {
        pUriState_->part = ch;
        while (ch < pEnd && *++ch != ':');
      }
      pUriState_->part_id = ch >= pEnd ? AMPS_URI_ERROR : AMPS_URI_HOST;
      pUriState_->part_length = (size_t)(ch - pUriState_->part);
    }
    else
    {
      pUriState_->part_id = AMPS_URI_PASSWORD;
      pUriState_->part = ++ch;
      while (ch < pEnd && *ch != '@')
      {
        ++ch;
      }
      pUriState_->part_length = (size_t)(ch - pUriState_->part);
    }
    break;
  case AMPS_URI_PASSWORD:
    assert(*ch == '@');
    ++ch;
    if (*ch == '[')
    {
      pUriState_->part = ++ch;
      while (ch < pEnd && *++ch != ']');
    }
    else
    {
      pUriState_->part = ch;
      while (ch < pEnd && *++ch != ':');
    }
    pUriState_->part_id = ch >= pEnd ? AMPS_URI_ERROR : AMPS_URI_HOST;
    pUriState_->part_length = (size_t)(ch - pUriState_->part);
    break;
  case AMPS_URI_HOST:
    if (*ch == ']')
    {
      ++ch;
    }
    assert(*ch == ':');
    ++ch;
    pUriState_->part = ch;
    while (ch < pEnd)
    {
      if (*ch == '/')
      {
        pUriState_->part_id = AMPS_URI_PORT;
        pUriState_->part_length = (size_t)(ch - pUriState_->part);
        return;
      }
      ++ch;
    }
    pUriState_->part_id = AMPS_URI_ERROR;
    break;
  case AMPS_URI_PORT:
    assert(*ch == '/');
    ++ch;
    pUriState_->part = ch;
    while (ch < pEnd)
    {
      if (*ch == '/' || *ch == '?')
      {
        break;
      } ++ch;
    }
    pUriState_->part_id = AMPS_URI_PROTOCOL;
    pUriState_->part_length = (size_t)(ch - pUriState_->part);
    break;
  case AMPS_URI_PROTOCOL:
    if (ch == pEnd)
    {
      pUriState_->part_id = AMPS_URI_END;
    }
    else if (*ch == '?')
    {
      ++ch;
      pUriState_->part = ch;
      pUriState_->part_id = AMPS_URI_OPTION_KEY;
      while (ch < pEnd)
      {
        if (*ch == '=')
        {
          pUriState_->part_length = (size_t)(ch - pUriState_->part);
          return;
        }
        ++ch;
      }
      pUriState_->part_id = AMPS_URI_ERROR;
    }
    else
    {
      assert(*ch == '/');
      ++ch;
      pUriState_->part = ch;
      while (ch < pEnd)
      {
        if (*ch == '/' || *ch == '?')
        {
          break;
        } ++ch;
      }
      pUriState_->part_id = AMPS_URI_MESSAGE_TYPE;
      pUriState_->part_length = (size_t)(ch - pUriState_->part);
    }
    break;
  case AMPS_URI_MESSAGE_TYPE:
    if (ch + 1 >= pEnd)
    {
      pUriState_->part_id = AMPS_URI_END;
    }
    else
    {
      if (*ch == '/')
      {
        ++ch;
      }
      if (*ch++ != '?')
      {
        pUriState_->part_id = AMPS_URI_ERROR;
        return;
      }
      pUriState_->part = ch;
      pUriState_->part_id = AMPS_URI_OPTION_KEY;
      while (ch < pEnd)
      {
        if (*ch == '=')
        {
          pUriState_->part_length = (size_t)(ch - pUriState_->part);
          return;
        }
        ++ch;
      }
      pUriState_->part_id = ch > pUriState_->part ? AMPS_URI_ERROR : AMPS_URI_END;
    }
    break;
  case AMPS_URI_OPTION_KEY:
    assert(*ch == '=');
    pUriState_->part = ++ch;
    while (ch < pEnd && *ch != '&')
    {
      ++ch;
    }
    pUriState_->part_id = AMPS_URI_OPTION_VALUE;
    pUriState_->part_length = (size_t)(ch - pUriState_->part);
    break;
  case AMPS_URI_OPTION_VALUE:
    if (ch + 1 >= pEnd)
    {
      pUriState_->part_id = AMPS_URI_END;
    }
    else
    {
      pUriState_->part = ++ch;
      while (ch < pEnd && *++ch != '=');
      pUriState_->part_length = (size_t)(ch - pUriState_->part);
      pUriState_->part_id = ch >= pEnd ? AMPS_URI_ERROR : AMPS_URI_OPTION_KEY;
    }
    break;
  default:
    pUriState_->part_id = AMPS_URI_ERROR;
    break;
  }
  return;
}
/*
static const char* amps_uri_part_names[] = {
  "start",
  "transport",
  "user",
  "password",
  "host",
  "port",
  "protocol",
  "message type",
  "option key",
  "option value",
  "ERROR",
  "end"
};
int main(int argc, char**argv)
{
  amps_uri_state state;
  memset(&state, 0, sizeof(amps_uri_state));

  while(state.part_id < AMPS_URI_ERROR)
  {
    amps_uri_parse(argv[1],strlen(argv[1]),&state);
    printf("%12s [%.*s]\n", amps_uri_part_names[state.part_id],
        (int)state.part_length,
        state.part);
  }
  return 0;
}
*/
