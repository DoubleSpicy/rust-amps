/*//////////////////////////////////////////////////////////////////////////
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
// Government’s use, duplication or disclosure of the computer software
// is subject to the restrictions set forth by 60East Technologies Inc..
//
///////////////////////////////////////////////////////////////////////// */
#include <amps/amps_impl.h>
#include "amps_protocol_generated.h"

#ifdef _WIN32
  #define amps_inline_func __inline
#else
  #define amps_inline_func inline
#endif

/* get field id optimized for the single char cases */
static amps_inline_func
int get_header_field_id(const char* pTag_, size_t len_)
{
  /*fprintf(stderr,"HEADER ATTR %lu %6s\n",len_,pTag_); */
  switch (len_)
  {
  case 1:
    switch (pTag_[0])
    {
    case 'c': return AMPS_Command;
    case 't': return AMPS_Topic;
    case 's': return AMPS_Sequence;
    case 'a': return AMPS_AckType;
    case 'e': return AMPS_Expiration;
    case 'o': return AMPS_Options;
    case 'l': return AMPS_MessageLength;
    case 'k': return AMPS_SowKey;
    case 'x': return AMPS_CorrelationId;
    }
    break;
  case 2:
    switch (pTag_[1])
    {
    case 'w': return AMPS_Password;   /* pw (same as auth_key) */
    case 's': return (pTag_[0] == 'b') ? AMPS_BatchSize : AMPS_Timestamp; /* bs (same as batch_size) or ts */
    case 'm': return AMPS_Bookmark;   /* bm (same as bookmark) */
    case 'p': return AMPS_LeasePeriod;
    case 't': return AMPS_MessageType;
    }
    break;
  case 3:
    switch (pTag_[1])
    {
    case 'm': return AMPS_Command;   /* cmd */
    case 'i': return AMPS_CommandId; /* cid */
    case 'c': return AMPS_AckType;   /* ack */
    case 'e': return AMPS_Sequence;  /* seq */
    }
    break;
  case 4:
    switch (pTag_[0])
    {
    case 'o': return AMPS_Options;             /* opts */
    case 's': return AMPS_SubscriptionIds;     /* sids - same as sub_ids */
    case 'g': return AMPS_GroupSequenceNumber; /* gseq - same as group_seq_num */
    }
    break;
  case 5:
    switch (pTag_[3])
    {
    case 'i': return AMPS_Topic;               /* topic */
    case '_': return AMPS_TopNRecordsReturned; /* top_n */
    }
    break;
  case 6:
    switch (pTag_[1])
    {
    case 'm': return AMPS_CommandId;      /* cmd_id */
    case 'i': return AMPS_Filter;         /* filter */
    case 't': return AMPS_Status;         /* status */
    case 'e': return AMPS_Reason;         /* reason */
    case 'u': return AMPS_SubscriptionId; /* sub_id */
    }
    break;
  case 7:
    switch (pTag_[0])
    {
    case 'm': return AMPS_Matches;         /* matches */
    case 'u': return AMPS_UserId;          /* user_id */
    case 's': return AMPS_SubscriptionIds; /* sub_ids */
    case 'v': return AMPS_Version;         /* version */
    }
    break;
  case 8:
    switch (pTag_[6])
    {
    case 'p': return AMPS_AckType;         /* ack_type */
    case 'e': return AMPS_Password;        /* auth_key */
    case 'r': return AMPS_Bookmark;        /* bookmark */
    case 'i': return AMPS_QueryID;         /* query_id */
    case 'y': return AMPS_SowKeys;         /* sow_keys */
    }
    break;
  case 9:
    switch (pTag_[0])
    {
    case 'h': return AMPS_Heartbeat; /* heartbeat */
    }
    break;
  case 10:
    switch (pTag_[0])
    {
    case 'e': return AMPS_Expiration; /* expiration */
    case 'b': return AMPS_BatchSize;  /* batch_size */
    }
    break;
  case 11:
    switch (pTag_[0])
    {
    case 'c': return AMPS_ClientName; /* client_name */
    }
    break;
  case 13:
    switch (pTag_[0])
    {
    case 't': return AMPS_TopicMatches;         /* topic_matches */
    case 'g': return AMPS_GroupSequenceNumber;  /* group_seq_num */
    }
    break;
  case 15:
    switch (pTag_[8])
    {
    case 'd': return AMPS_SowDelete;      /* records_deleted */
    case 'u': return AMPS_RecordsUpdated; /* records_updated */
    }
    break;
  case 16:
    switch (pTag_[8])
    {
    case 'i': return AMPS_RecordsInserted; /* records_inserted */
    case 'r': return AMPS_RecordsReturned; /* records_returned */
    }
    break;
  }
  return -1;
}

/* matches
 * topic_matches
 * records_returned
 * records_deleted */

amps_result amps_protocol_pre_deserialize(amps_handle message, const amps_char* buffer, size_t length)
{
  amps_message_t* me = (amps_message_t*)message;
  amps_message_reset(message);
  me->rawBuffer = buffer;
  me->length = length;
  return AMPS_E_OK;
}

static const char quote        = '"';
static const char comma        = ',';
static const char colon        = ':';
static const char open_brace   = '{';
static const char close_brace  = '}';

/* string data inside double quotes (anything but ")
 * return 0 for "
 * return 1 for all but \\ \/ \b \f \n \r \t which return 2
 * \\     5C      Back Slash
 * \b     08      Backspace
 * \t     09      Horizontal Tab
 * \n     0A      Line feed
 * \f     0C      Form feed
 * \r     0D      Carriage return */
const char lookup_string_data[256] =
{
  /* 0   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F */
  1,  1,  1,  1,  1,  1,  1,  1,  2,  2,  2,  1,  2,  2,  1,  1,  /* 0 */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  /* 1 */
  1,  1,  0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  /* 2 */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  /* 3 */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  /* 4 */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  2,  1,  1,  1,  /* 5 */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  /* 6 */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  /* 7 */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  /* 8 */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  /* 9 */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  /* A */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  /* B */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  /* C */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  /* D */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  /* E */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1   /* F */
};

/* all characters that could constitute a number
 / 0 1 2 3 4 5 6 7 8 9 . + - e E */
const char lookup_number[256] =
{
  /* 0   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F */
  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  1,  0,  0,  1,  0,  0,  /* 0 */
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 1 */
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  0,  1,  1,  0,  /* 2 */
  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  0,  0,  0,  0,  0,  /* 3 */
  0,  0,  0,  0,  0,  1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 4 */
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 5 */
  0,  0,  0,  0,  0,  1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 6 */
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 7 */
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 8 */
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* 9 */
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* A */
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* B */
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* C */
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* D */
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  /* E */
  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0   /* F */
};

typedef struct
{
  const char* _p;
  size_t      _len;

} cstring;

static size_t get_string(const char* p_, const char* pEnd_)
{
  const char* p = p_;
  while (p_ < pEnd_)
  {
    /* lookup will return 0, 1 or 2 (2 is returned for escaped chars */
    char rc = lookup_string_data[(unsigned char)p_[0]];
    if (rc)
    {
      p_ += (size_t)rc;
    }
    else
    {
      if (p_[0] == quote)
      {
        size_t len = (size_t)(p_ - p);
        /* skip over '"' */
        ++p_;
        return len;
      }
      assert(0); // Indicates parse failure, i.e. server or network error
      return 0;
    }
  }
  assert(0); // Indicates parse failure, i.e. server or network error
  return 0;
}

static size_t get_number(const char* p_, const char* pEnd_)
{
  const char* p = p_;
  while (p_ < pEnd_)
  {
    if (lookup_number[(unsigned)p_[0]])
    {
      ++p_;
    }
    else
    {
      return (size_t)(p_ - p);
    }
  }
  assert(0); // Indicates parse failure, i.e. server or network error
  return 0;
}

static const char* get_value(const char* p_, const char* pEnd_, cstring* pValue_)
{
  /* '[' == 0x5B, '{' == 0x7B */
  char c = p_[0];
  switch (c)
  {
  case '"':
    /* start of string data
     * skip over '"' */
    ++p_;
    pValue_->_p = (const char*)p_;
    pValue_->_len = get_string(p_, pEnd_);
    /* skip over terminating '"' */
    return p_ + pValue_->_len + 1;
  case '0':
  case '1':
  case '2':
  case '3':
  case '4':
  case '5':
  case '6':
  case '7':
  case '8':
  case '9':
  case '-':
    /* number */
    pValue_->_p = (const char*)p_;
    pValue_->_len = get_number(p_, pEnd_);
    return p_ + pValue_->_len;
  case 't':
    /* true */
    if (p_[1] == 'r' && p_[2] == 'u' && p_[3] == 'e')
    {
      pValue_->_p = (const char*)p_;
      pValue_->_len = 4;
      return p_ + 4;
    }
    return 0;
  case 'f':
    /* false */
    if (p_[1] == 'a' && p_[2] == 'l' && p_[3] == 's' && p_[4] == 'e')
    {
      pValue_->_p = (const char*)p_;
      pValue_->_len = 5;
      return p_ + 5;
    }
    return 0;
  case 'n':
    /* null */
    if (p_[1] == 'u' && p_[2] == 'l' && p_[3] == 'l')
    {
      pValue_->_p = (const char*)p_;
      pValue_->_len = 4;
      return p_ + 4;
    }
    return 0;
  default:
    return 0;
  }
  return 0;
}

static const int read_header          = 0;
static const int read_sow_data_header = 1;

amps_result amps_protocol_deserialize(amps_handle message, size_t startingPosition, unsigned long* bytesRead)
{
  static const char* PUBLISH = "publish";
  amps_message_t* me = (amps_message_t*)message;
  size_t length = me->length - startingPosition;
  const char* pStart = me->rawBuffer + startingPosition;
  const char* pEnd = pStart + length;
  const char* p = pStart;
  const char* pTag = 0;
  char ch;
  cstring value = { 0, 0 };
  int fieldId = 0;
  int readState = (startingPosition == 0) ? read_header : read_sow_data_header;
  size_t msgLen = 0;

  //fprintf(stderr,"\nPARSE AMPS 1 len = %lu data = %s\n",length,pStart);
scan_start:
  if (p[0] != open_brace)
  {
    while (p < pEnd)
    {
      if (p[0] == open_brace)
      {
        break;
      }
      ++p;
    }
    if (p == pEnd)
    {
      return AMPS_E_CONNECTION;
    }
  }

  /* skip open_brace */
  ++p;

  /*fprintf(stderr,"PARSE AMPS 2\n"); */
  while (p < pEnd)
  {
    if (p[0] == close_brace)
    {
      /* skip closing brace */
      ++p;
      break;
    }
    if (p[0] == quote)
    {
      /* skip starting quote */
      ++p;
      pTag = p;
      for (ch = *p; ch != quote && p < pEnd;)
      {
        ++p;
        ch = *p;
      }
      fieldId = get_header_field_id(pTag, (size_t)(p - pTag));
      if (p[0] != quote || fieldId == -1)
      {
        return AMPS_E_CONNECTION;
      }
      /* skip ending quote */
      ++p;
      /* find colon */
      if (p[0] != colon)
      {
        while (p < pEnd)
        {
          if (p[0] == colon)
          {
            break;
          }
          ++p;
        }
        if (p[0] != colon)
        {
          return AMPS_E_CONNECTION;
        }
      }
      /* skip colon */
      ++p;
      //fprintf(stderr,"\nPARSE AMPS 2 field id = %d\n",fieldId);
      if ((fieldId == AMPS_Command) && (p[0] == '"') && (p[1] == 'p') && (p[2] == '"'))
      {
        //fprintf(stderr,"PARSE AMPS 3 COMMAND = p\n");
        amps_message_assign_field_value(me, AMPS_Command, PUBLISH, 7);
        p += 3;
      }
      else
      {
        p = get_value(p, pEnd, &value);
        amps_message_assign_field_value(me, (FieldId)fieldId, value._p, value._len);
        //fprintf(stderr,"PARSE AMPS 3 %d len = %lu\n",fieldId,value._len);
      }
      if (p == 0)
      {
        return AMPS_E_CONNECTION;
      }
      //fprintf(stderr,"PARSE AMPS 3 %d len = %lu\n",fieldId,value._len);
      /* find comma or closing brace */
      if (p[0] == comma)
      {
        /* skip comma and get next value */
        ++p;
        continue;
      }
      else if (p[0] == close_brace)
      {
        /* skip ending '}' */
        ++p;
        /*fprintf(stderr,"PARSE AMPS 4\n"); */
        if (readState == read_header)
        {
          /*fprintf(stderr,"PARSE AMPS 5\n");
           * check for sow
           * sow messages are {msg header}{sow header}{data}{sow header}{data} .... */
          amps_field_t* theField = &(me->fields[AMPS_Command]);
          if (theField->begin[0] == 's' && theField->length == 3)
          {
            /* transition state and read the data header */
            readState = read_sow_data_header;
            /*fprintf(stderr,"PARSE AMPS 6\n"); */
            goto scan_start;
          }
          /* done, the data is the remaining buffer */
          me->data.owner = 0;
          me->data.begin = (char*)p;
          me->data.length = (size_t)(pEnd - p);
          *bytesRead = (unsigned long)(pEnd - pStart);
          /*fprintf(stderr,"PARSE AMPS 7 bytes read = %lu\n",(size_t)(pEnd - pStart)); */
          return AMPS_E_OK;
        }
        else
        {
          /*fprintf(stderr,"PARSE AMPS 8\n");
           * sow data header */
          msgLen = amps_message_get_field_long(me, AMPS_MessageLength);
          me->data.owner = 0;
          me->data.begin = (char*)p;
          me->data.length = msgLen;
          *bytesRead = (unsigned long)((size_t)(p - pStart) + msgLen);
          return AMPS_E_OK;
        }
        break;
      }
      else
      {
        while (p < pEnd && p[0] != comma && p[0] != close_brace)
        {
          ++p;
        }
        /*fprintf(stderr,"PARSE AMPS 9\n"); */
        if (p[0] != colon && p[0] != close_brace)
        {
          return AMPS_E_CONNECTION;
        }
      }
    }
  }
  /*fprintf(stderr,"PARSE AMPS 10\n"); */
  return AMPS_E_CONNECTION;
}

static unsigned long long s_is_string_mask = (unsigned long long)
                                             ~(0x1 << AMPS_Sequence
                                               | 0x1 << AMPS_TimeoutInterval
                                               | 0x1 << AMPS_LeasePeriod
                                               | 0x1 << AMPS_BatchSize
                                               | 0x1 << AMPS_TopNRecordsReturned);

static unsigned long long s_is_expansion_mask = (unsigned long long)
                                                ( 0x1 << AMPS_ClientName
                                                  | 0x1 << AMPS_Topic
                                                  | 0x1 << AMPS_Filter
                                                  | 0x1 << AMPS_UserId
                                                  | 0x1 << AMPS_Password
                                                  | 0x1 << AMPS_SubscriptionId);

static amps_inline_func
int is_string_field(int fieldId_)
{
  return ((0x1ULL << fieldId_) & s_is_string_mask) != 0ULL;
}

static amps_inline_func
int is_expansion_field(int fieldId_)
{
  return ((0x1ULL << fieldId_) & s_is_expansion_mask) != 0ULL;
}

static int amps_protocol_field_serialize_command(amps_field_t* pField_, amps_char* buffer_, size_t length_)
{
  amps_char* p = buffer_;

  if (length_ < (6 + pField_->length))
  {
    return 0;
  }

  *buffer_++ = '"';
  *buffer_++ = 'c';
  *buffer_++ = '"';
  *buffer_++ = ':';
  *buffer_++ = '"';
  if (pField_->begin[0] == 'p')
  {
    *buffer_++ = 'p';
  }
  else
  {
    memcpy(buffer_, pField_->begin, pField_->length);
    buffer_ += pField_->length;
  }
  *buffer_++ = '"';
  return (int)(buffer_ - p);
}

static int amps_protocol_field_serialize(amps_field_t* field, FieldId fieldId, amps_char* buffer, size_t length, int count)
{
  amps_char* p = buffer;
  const size_t messageIdLen = g_ampsProtocolNameLengths[fieldId];
  const int isString = is_string_field(fieldId);
  size_t expansionFactor = is_expansion_field(fieldId) ? 2 : 1;
  unsigned i;
  char ch;

  /* ,"field":data */
  if (length < (messageIdLen + 4 + (expansionFactor * field->length)))
  {
    return 0;
  }
  if (count > 0)
  {
    *buffer++ = ',';
  }
  *buffer++ = '"';
  memcpy(buffer, g_ampsProtocolNames[fieldId], messageIdLen);
  buffer += messageIdLen;
  *buffer++ = '"';
  *buffer++ = ':';
  if (isString)
  {
    *buffer++ = '"';
  }
  if (2 == expansionFactor)
  {
    for (i = 0; i < field->length; ++i)
    {
      ch = field->begin[i];
      switch (field->begin[i])
      {
      case '\\':
        *buffer++ = '\\';
        *buffer++ = '\\';
        break;
      case '\"':
        *buffer++ = '\\';
        *buffer++ = '\"';
        break;
      default:
        *buffer++ = ch;
      }
    }
  }
  else
  {
    memcpy(buffer, field->begin, field->length);
    buffer += field->length;
  }
  if (isString)
  {
    *buffer++ = '"';
  }
  return (int)(buffer - p);
}

int amps_protocol_serialize(amps_handle message, amps_char* buffer, size_t length)
{
  amps_message_t* me = (amps_message_t*)message;
  size_t spaceRemaining = length;
  int i = 0, bytesWritten = 0;
  int count = 0;
  unsigned long long bitmask = me->bitmask;

  if (spaceRemaining < 2)
  {
    return -1;
  }
  *buffer++ = '{';
  spaceRemaining -= 1;
  // command should be first bit set
  if (bitmask & 1)
  {
    bytesWritten = amps_protocol_field_serialize_command(&(me->fields[i]), buffer, spaceRemaining);
    if (!bytesWritten)
    {
      return -1;
    }
    else
    {
      ++count;
      buffer += bytesWritten;
      spaceRemaining -= (size_t)bytesWritten;
    }
  }
  bitmask >>= 1;
  for (i = 1; bitmask; ++i, bitmask >>= 1)
  {
    if (bitmask & 1)
    {
      /* try serializing to the buffer. */
      bytesWritten = amps_protocol_field_serialize(&(me->fields[i]), (FieldId)i, buffer, spaceRemaining, count);
      if (!bytesWritten)
      {
        return -1;
      }
      else
      {
        ++count;
        buffer += bytesWritten;
        spaceRemaining -= (size_t)bytesWritten;
      }
    }
  }
  *buffer++ = '}';
  --spaceRemaining;

  /* now ensure and write the data portion */
  if (spaceRemaining < me->data.length)
  {
    return -1;
  }
  memcpy(buffer, me->data.begin, me->data.length);
  spaceRemaining -= me->data.length;
  return (int)(length - spaceRemaining);
}

