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
// Government's use, duplication or disclosure of the computer software
// is subject to the restrictions set forth by 60East Technologies Inc..
//
///////////////////////////////////////////////////////////////////////// */
#include <amps/amps_impl.h>
#include "message_generated.h"

static size_t g_message_protocols_count = 5;
/* xml names */
static char g_envelope[] = "<SOAP-ENV:Envelope>";
static char g_envelopeEnd[] = "</SOAP-ENV:Envelope>";
static char g_soapHeader[] = "<SOAP-ENV:Header>";
static char g_soapEndHeader[] = "</SOAP-ENV:Header>";
static char g_soapBody[] = "<SOAP-ENV:Body>";
static char g_soapEndBody[] = "</SOAP-ENV:Body>";
static const size_t g_bodyEndLength = sizeof(g_soapEndBody) + sizeof(g_soapEndHeader);

amps_int64_t
amps_message_get_protocol(const amps_char* protocolname)
{
  size_t i;
  for (i = 0; i < g_message_protocols_count; i++)
  {
    if (strcmp(protocolname, g_message_protocols[i].name) == 0)
    {
      /* found the dude */
      return (amps_int64_t)i;
    }
  }
  return (amps_int64_t)-1;
}

AMPSDLL amps_handle amps_message_create(amps_handle client)
{
  amps_handle m = (amps_handle)malloc(sizeof(amps_message_t));
  if (m != NULL)
  {
    memset(m, 0, sizeof(amps_message_t));
  }
  return m;
}

AMPSDLL amps_handle amps_message_copy(amps_handle message)
{
  amps_message_t* orig = (amps_message_t*)message;
  amps_handle res = amps_message_create(NULL);
  unsigned long long bitmask;
  size_t i;
  if (message == NULL)
  {
    return res;
  }

  bitmask = orig->bitmask;
  for (i = 0; bitmask; ++i, bitmask >>= 1)
  {
    if (bitmask & 1ULL)
    {
      amps_message_set_field_value(res, (FieldId)i, orig->fields[i].begin,
                                   orig->fields[i].length);
    }
  }
  if (orig->data.length > 0)
  {
    amps_message_set_data(res, orig->data.begin, orig->data.length);
  }
  return res;
}

AMPSDLL void amps_message_destroy(amps_handle message)
{
  unsigned i;
  amps_message_t* me = (amps_message_t*)message;
  if (message == NULL)
  {
    return;
  }

  for (i = 0; i < MESSAGEFIELDS; i++)
  {
    if (me->fields[i].owner)
    {
      free(me->fields[i].begin);
    }
  }

  if (me->data.owner)
  {
    free(me->data.begin);
  }
  free(me);
}

AMPSDLL void amps_message_reset(amps_handle message)
{
  amps_message_t* me = (amps_message_t*)message;
  if (message == NULL)
  {
    return;
  }
  /* set every field's length to 0.  But don't free any memory they allocate yet; no reason to. */
  me->data.length = 0;
  me->bitmask = 0ULL;
}

AMPSDLL void amps_message_get_field_value(
  amps_handle message,
  FieldId field,
  const amps_char**  value_ptr,
  size_t* length_ptr)
{
  amps_message_t* me = (amps_message_t*)message;
  if (me->bitmask & (1ULL << field))
  {
    *value_ptr = me->fields[(size_t)field].begin;
    *length_ptr = me->fields[(size_t)field].length;
  }
  else
  {
    *value_ptr = 0L;
    *length_ptr = 0L;
  }
}

AMPSDLL void amps_message_get_data(
  amps_handle message,
  amps_char** value_ptr,
  size_t* length_ptr)
{
  amps_message_t* me = (amps_message_t*)message;;
  *value_ptr = me->data.begin;
  *length_ptr = me->data.length;
}

AMPSDLL void amps_field_set(amps_field_t* theField,
                            const amps_char* value,
                            size_t length)
{
  if (!length) /* makes it 'NULL' */
  {
    theField->length = 0;
    return;
  }

  if (!theField->owner || theField->capacity < length)
  {
    if (theField->owner)
    {
      theField->owner = 0;
      theField->length = 0;
      theField->capacity = 0;
      free(theField->begin);
    }

    theField->begin = (char*) malloc(length); /* should we have an expansion factor?*/
    if (!theField->begin)
    {
      return;
    }
    theField->owner = 1;
    theField->capacity = length;
  }
  memcpy(theField->begin, value, length);
  theField->length = length;
}

AMPSDLL void amps_message_set_field_value(
  amps_handle message,
  FieldId field,
  const amps_char* value,
  size_t   length)
{
  amps_message_t* me = (amps_message_t*)message;
  amps_field_t* theField = &(me->fields[field]);
  if (length == 0)
  {
    me->bitmask &= ~(1ULL << field);
  }
  else
  {
    me->bitmask |= (1ULL << field);
  }
  amps_field_set(theField, value, length);
}

AMPSDLL void amps_field_assign(amps_field_t* theField,
                               const amps_char* value,
                               size_t length)
{
  if (!length) /* makes it 'NULL'*/
  {
    theField->length = 0;
    return;
  }
  if (theField->owner)
  {
    theField->length = 0;
    theField->capacity = 0;
    free(theField->begin);
  }
  theField->begin = (char*)value;
  theField->length = length;
  theField->owner = 0;
}

AMPSDLL void amps_message_assign_field_value(
  amps_handle message,
  FieldId field,
  const amps_char* value,
  size_t   length)
{
  amps_message_t* me = (amps_message_t*)message;
  amps_field_t* theField = &(me->fields[field]);
  if (length == 0)
  {
    me->bitmask &= ~(1ULL << field);
  }
  else
  {
    me->bitmask |= (1ULL << field);
  }
  amps_field_assign(theField, value, length);
}
AMPSDLL void amps_message_set_field_value_nts(
  amps_handle message,
  FieldId field,
  const amps_char* value)
{
  amps_message_set_field_value(message, field, value, strlen(value));
}

AMPSDLL void amps_message_set_data(
  amps_handle message,
  const amps_char* value,
  size_t   length)
{
  amps_message_t* me = (amps_message_t*)message;
  amps_field_set(&(me->data), value, length);
}

AMPSDLL void amps_message_assign_data(
  amps_handle message,
  const amps_char* value,
  size_t   length)
{
  amps_message_t* me = (amps_message_t*)message;
  amps_field_assign(&(me->data), value, length);
}

AMPSDLL void amps_message_set_data_nts(
  amps_handle message,
  const amps_char* value)
{
  amps_message_set_data(message, value, strlen(value));
}

int amps_field_serialize(
  amps_field_t* field,
  FieldId fieldId,
  amps_char* buffer,
  size_t length)
{
  const size_t messageIdLen = 6;
  /* id +data+fieldsep */
  if (length < (messageIdLen + field->length + 1  ))
  {
    return 0;
  }
  memcpy(buffer, g_FieldIdNames[fieldId], messageIdLen);
  buffer += messageIdLen;
  memcpy(buffer, field->begin, field->length);
  buffer += field->length;
  *buffer = 0x01;  /* field separator! */

  return (int)(messageIdLen + field->length + 1);
}

AMPSDLL void amps_message_assign_field(
  amps_message_t* me,
  size_t fieldCode,
  const amps_char* data,
  size_t length)
{
  if (fieldCode >= 20000)
  {
    fieldCode -= 20000;
  }
  if (fieldCode < sizeof(g_decoder) / sizeof(int)
      && g_decoder[fieldCode] != -1)
  {
    /* found a tag we want, drop it in. */
    int field = g_decoder[fieldCode];
    if (me->fields[field].owner)
    {
      free(me->fields[field].begin);
      me->fields[field].owner = 0;
      me->fields[field].capacity = 0;
    }
    me->fields[field].begin = (char*)data;
    me->fields[field].length = length;
    if (length == 0)
    {
      me->bitmask &= ~(1ULL << field);
    }
    else
    {
      me->bitmask |= (1ULL << field);
    }
  }
}

AMPSDLL unsigned long amps_message_get_field_long(
  amps_handle message,
  FieldId field)
{
  amps_message_t* me = (amps_message_t*)message;
  unsigned long result = 0;
  size_t i;
  if (!(me->bitmask & (1ULL << field)))
  {
    return 0;
  }
  for (i = 0; i < me->fields[field].length; i++)
  {
    result *= 10;
    result += (unsigned long)(*(me->fields[field].begin + i) - '0');
  }
  return result;
}

AMPSDLL amps_uint64_t amps_message_get_field_uint64(
  amps_handle message,
  FieldId field)
{
  amps_message_t* me = (amps_message_t*)message;
  amps_uint64_t result = (amps_uint64_t)0;
  size_t i;
  if (!(me->bitmask & (1ULL << field)))
  {
    return result;
  }
  for (i = 0; i < me->fields[field].length; i++)
  {
    result *= (amps_uint64_t)10;
    result += (amps_uint64_t)(*(me->fields[field].begin + i) - '0');
  }
  return result;
}

amps_result amps_fix_pre_deserialize(
  amps_handle message,
  const amps_char* buffer,
  size_t length)
{
  amps_message_t* me = (amps_message_t*)message;
  amps_message_reset(message);
  me->rawBuffer = buffer;
  me->length = length;
  return AMPS_E_OK;
}
amps_result amps_fix_deserialize(
  amps_handle message,
  size_t startingPosition,
  unsigned long* bytesRead)
{
  amps_message_t* me = (amps_message_t*)message;
  size_t i;
  size_t accumulator = 0;
  short readingHeader = 1;
  size_t firstDataCharacter = 0;
  size_t length = me->length - startingPosition;
  const char* buffer = me->rawBuffer + startingPosition;
  char thisCharacter;

  for (i = 0; i < length; i++)
  {
    thisCharacter = buffer[i];
    if (readingHeader)
    {
      if (thisCharacter >= '0' && thisCharacter <= '9')
      {
        accumulator *= 10;
        accumulator += (size_t)(thisCharacter - '0');
      }
      else
      {
        if (thisCharacter == '=')
        {
          readingHeader = 0;
          firstDataCharacter = i + 1;
          continue;
        }
        else if (thisCharacter == 0x02)    /*header sep */
        {
          /* if this is a SOW, Publish or OOF, we need to keep going
           * until we have a MessageLength */
          char command = me->fields[AMPS_Command].begin[0];
          char command2 = me->fields[AMPS_Command].begin[1];
          if (me->bitmask & (1ULL << AMPS_MessageLength) ||
              (command != 'p' && !(command == 's' && command2 == 'o') && command != 'o'))
          {
            i++;
            break;
          }
        }
        else
        {
          return AMPS_E_CONNECTION;
        }
      }
    }
    else
    {
      /* field sep or header sep */
      if (thisCharacter == 0x01 || thisCharacter == 0x02)
      {
        amps_message_assign_field(me, accumulator, buffer + firstDataCharacter, i - firstDataCharacter);
        if (thisCharacter == 0x02)
        {
          i++;
          break;
        }
        accumulator = 0;
        readingHeader = 1;
      }
    } /* if(readingHeader) */
  }/* for */
  /* in case a message terminated in the middle of the data of a header. */
  if (!readingHeader)
  {
    amps_message_assign_field(me, accumulator, buffer + firstDataCharacter, i - firstDataCharacter);
  }

  /* now the rest is data. */
  me->data.owner = 0;
  me->data.begin = (char*)buffer + i;
  me->data.length = amps_message_get_field_long(me, AMPS_MessageLength);
  /* stop_timer does not include a message length on its data. */
  if (me->data.length == 0 && me->fields[AMPS_Command].begin[1] == 't')
  {
    me->data.length = length - i;
  }
  *bytesRead = (unsigned long)(i + me->data.length + 1);
  return AMPS_E_OK;
}

int amps_fix_serialize(
  amps_handle message,
  amps_char* buffer,
  size_t length)
{
  amps_message_t* me = (amps_message_t*)message;
  size_t spaceRemaining = length;
  int i = 0, bytesWritten = 0;
  unsigned long long bitmask = me->bitmask;
  for (i = 0; bitmask; ++i, bitmask >>= 1)
  {
    if (bitmask & 1ULL)
    {
      /* try serializing to the buffer. */
      bytesWritten = amps_field_serialize(&(me->fields[i]), (FieldId)i, buffer, spaceRemaining);

      if (!bytesWritten)
      {
        return -1;
      }
      else
      {
        buffer += bytesWritten;
        spaceRemaining -= (size_t)bytesWritten;
      }
    }
  }

  /* now ensure and write the data portion */
  if (spaceRemaining < me->data.length + 1)
  {
    return -1;
  }
  *(buffer++) = (char)0x02; /* header sep */
  spaceRemaining -= 1;
  memcpy(buffer, me->data.begin, me->data.length);
  spaceRemaining -= me->data.length;
  return (int)(length - spaceRemaining);
}

amps_result amps_message_pre_deserialize(
  amps_handle message,
  amps_int64_t serializer,
  const amps_char* buffer,
  size_t length)
{
  return g_message_protocols[serializer].preDeserializeFunc(
           message, buffer, length);
}

amps_result amps_message_deserialize(
  amps_handle message,
  amps_int64_t serializer,
  size_t startingPosition,
  unsigned long* bytesRead)
{
  return g_message_protocols[serializer].deserializeFunc(
           message, startingPosition, bytesRead);
}

int amps_message_serialize(
  amps_handle message,
  amps_int64_t serializer,
  amps_char* buffer,
  size_t length)
{
  return g_message_protocols[serializer].serializeFunc(
           message, buffer, length);
}

/* XML parsing functions */
#define WRITE(x, len) if(len>bytesRemaining) return -1; memcpy(buffer,x,len); buffer+=len; bytesRemaining-=len
#define WRITE_A(x) WRITE(x,sizeof(x)-1)
#define WRITE_START(x) WRITE(g_xmlNames[(x)*2], g_xmlNameLengthsBegin[x] + 2)
#define WRITE_END(x)   WRITE(g_xmlNames[((x)*2)+1], g_xmlNameLengthsEnd[x] + 3)
#define WRITE_F(i) bytesWritten = amps_field_serialize(&(me->fields[i]), (FieldId)i, buffer, bytesRemaining); if(!bytesWritten) return -1; buffer+=bytesWritten; bytesRemaining-=bytesWritten

int
amps_xml_serialize(
  amps_handle message,
  amps_char* buffer,
  size_t length)
{
  amps_message_t* me = (amps_message_t*)message;
  size_t bytesRemaining = length;
  int i;
  unsigned long long bitmask;

  WRITE_A(g_envelope);
  WRITE_A(g_soapHeader);
  bitmask = me->bitmask;
  for (i = 0; bitmask; ++i, bitmask >>= 1)
  {
    if (bitmask & 1ULL)
    {
      WRITE_START(i);
      WRITE(me->fields[i].begin, me->fields[i].length);
      WRITE_END(i);
    }
  }
  WRITE_A(g_soapEndHeader);
  WRITE_A(g_soapBody);
  WRITE(me->data.begin, me->data.length);
  WRITE_A(g_soapEndBody);
  WRITE_A(g_envelopeEnd);

  return (int)(length - bytesRemaining);
}

char
unescape(char a, char b)
{
  switch (a)
  {
  case 'a':
    switch (b)
    {
    case 'm':  return '&';
    case 'p':  return '\'';
    }
    break;
  case 'l':  return '<';
  case 'g':  return '>';
  case 'q':  return '\"';
  }
  return AMPS_E_STREAM;
}

amps_result
amps_xml_deserialize(
  amps_handle message,
  size_t startingPosition,
  unsigned long* bytesRead)
{
  amps_message_t* me = (amps_message_t*)message;
  size_t i = 0;
  size_t firstDataCharacter = 0, firstTagCharacter = 0;
  size_t length = me->length - startingPosition;
  char* buffer = (char*)(me->rawBuffer + startingPosition);
  enum { Outside, OpeningTag, ClosingTag, InsideData, PastHeaders } state;
  /* if startingPosition != 0, we can assume we're in the middle
   * of a batch. */
  FieldId currentField = AMPS_Unknown_Field;
  const char* tag;
  size_t tagLength;
  /* first set all of our message headers, unless we're no longer
   * in the message headers, in the middle of a batch */
  state = startingPosition ? PastHeaders : Outside;
  for (; state != PastHeaders && i < length; i++)
  {
    switch (buffer[i])
    {
    case '<':
      /* if we're inside a tag, the XML is malformed. */
      if (state == Outside)
      {
        state = OpeningTag;
      }
      else if (state == InsideData)
      {
        state = ClosingTag;
      }
      else
      {
        return AMPS_E_STREAM;
      }
      firstTagCharacter = i + 1;
      break;

    case '>':
      if (state == OpeningTag)
      {
        /* new tag.  try to decode it. */
        currentField = (FieldId)amps_xml_decode(buffer + firstTagCharacter, i - firstTagCharacter);
        if (currentField != AMPS_Unknown_Field)
        {
          /* This is a header field we know and love. */
          state = InsideData;
          firstDataCharacter = i + 1;

          //these fields could have escaped values that need to be unescaped
          if (currentField == AMPS_ClientName || currentField == AMPS_Topic ||
              currentField == AMPS_UserId || currentField == AMPS_Password  ||
              currentField == AMPS_SubscriptionId)
          {
            size_t reader = firstDataCharacter;
            size_t writer = firstDataCharacter;
            while (reader < length && buffer[reader] != '<')
            {
              if (buffer[reader] == '&')
              {
                buffer[writer] = unescape(buffer[reader + 1], buffer[reader + 2]);
                while (reader < length && buffer[reader] != ';')
                {
                  ++reader;
                }
              }
              else
              {
                buffer[writer] = buffer[reader];
              }
              ++reader;
              ++writer;
            }
            amps_message_set_field_value(message, currentField, buffer + firstDataCharacter, writer - firstDataCharacter);
            state = Outside;

            //scan to past the closing tag
            while (reader < length && buffer[reader] != '>')
            {
              ++reader;
            }
            i = reader;
          }
        }
        else
        {
          /* the first time we see a closing tag,
           * we know we're past the headers.  true story. */
          if (buffer[firstTagCharacter] == '/')
          {
            state = PastHeaders;
          }
          else
          {
            /* descend down if we don't recognize. */
            state = Outside;
          }
        }
      }
      else if (state == ClosingTag)
      {
        /* For well formed-ness this tag ought to be the same
         * as the opening one, but instead we'll just check that
         * there's a closing tag where we expect. */
        if (buffer[firstTagCharacter] != '/')
        {
          return AMPS_E_STREAM;
        } /* hierarchy in the fields is not OK
                 * great, it's data.  it begins at firstDataCharacter and
                 * ends at firstTagCharacter - 2. */
        amps_message_set_field_value(message, currentField,
                                     buffer + firstDataCharacter,
                                     firstTagCharacter - (firstDataCharacter + 1));
        state = Outside;
      }
      else
      {
        /* spurious '>' not inside a tag.  could recover, but this is illegal
         * by golly. */
        return AMPS_E_STREAM;
      } /* else if */
      break;
    } /* switch */
  } /* for */

  if (!startingPosition)
  {
    /* skip past the <SOAP:Body>, if we're not in the middle of a message */
    while (i < length && buffer[i++] != '>');
  }

  amps_message_get_field_value(message, AMPS_Command, &tag, &tagLength);
  if (tagLength == 3 && tag[0] == 's' && tag[1] == 'o' && tag[2] == 'w')
  {
    /* it's a sow.  Advance past <Msg Key="  and read the key, being careful
     * in the face of message corruption. */
    i += 10;
    firstDataCharacter = i; /* buffer[firstDataCharacter] now the character after the " */
    while (i < length && buffer[i] != '"')
    {
      i++;    /* advance to the next one */
    }
    if (i > firstDataCharacter) /* catch unexpected end */
    {
      amps_message_set_field_value(message, AMPS_SowKey, &buffer[firstDataCharacter], i - firstDataCharacter);
    }
    else
    {
      return AMPS_E_STREAM;
    }

    /* now extract the length from len=".*" */
    i += 7;
    firstDataCharacter = i;
    while (i < length && buffer[i] != '"')
    {
      i++;
    }
    if (i > firstDataCharacter)
    {
      amps_message_set_field_value(message, AMPS_MessageLength, &buffer[firstDataCharacter], i - firstDataCharacter);
    }
    else
    {
      return AMPS_E_STREAM;
    }
    if (buffer[i + 1] != '>')
    {
      /* skip the last '"' */
      ++i;
      /* now extract the length from ts=".*" */
      i += 6;
      firstDataCharacter = i;
      while (i < length && buffer[i] != '"')
      {
        i++;
      }
      if (i > firstDataCharacter)
      {
        amps_message_set_field_value(message, AMPS_Timestamp, &buffer[firstDataCharacter], i - firstDataCharacter);
      }
      else
      {
        return AMPS_E_STREAM;
      }
    }

    /* buffer[i] should look like "><somedata>...  (including the double quote)
     * use the length we just retrieved to get the data length, and set it. */
    tagLength = amps_message_get_field_long(message, AMPS_MessageLength);
    /* make sure the messagelength isn't lying to us. */
    if (i + 2 + tagLength < length)
    {
      amps_message_set_data(message, &buffer[i + 2], tagLength);
    }
    else
    {
      return AMPS_E_STREAM; /* The MessageLength we think we read would take us past the end. */
    }
    /* trick: if all that's left in the message is the
     * size of the </Msg></SOAP-ENV:Body></SOAP-ENV:Envelope>,
     * then consume all of it and be done. */
    if ( tagLength + i + g_bodyEndLength + 8 >= length )
    {
      *bytesRead = (unsigned long)length;
    }
    else
    {
      *bytesRead = (unsigned long)(i + tagLength + 8);
    }
  }
  else if (buffer[i - 2] == 'y') /* SOAP:Body> -- non empty body */
  {
    firstDataCharacter = i;
    if (length > i + g_bodyEndLength)
    {
      amps_message_set_data(message, &buffer[firstDataCharacter],
                            length - (i + g_bodyEndLength));
    }
    *bytesRead = (unsigned long)length;
  }
  else
  {
    *bytesRead = (unsigned long)length;
  }
  /* empty body, otherwise. */

  return AMPS_E_OK;
}

protocol_entry_t g_message_protocols[] =
{
  { "fix", &amps_fix_serialize, &amps_fix_pre_deserialize, &amps_fix_deserialize },
  { "nvfix", &amps_fix_serialize, &amps_fix_pre_deserialize, &amps_fix_deserialize },
  { "xml", &amps_xml_serialize, &amps_fix_pre_deserialize, &amps_xml_deserialize },
  { "json", &amps_protocol_serialize, &amps_protocol_pre_deserialize, &amps_protocol_deserialize },
  { "amps", &amps_protocol_serialize, &amps_protocol_pre_deserialize, &amps_protocol_deserialize },
};


