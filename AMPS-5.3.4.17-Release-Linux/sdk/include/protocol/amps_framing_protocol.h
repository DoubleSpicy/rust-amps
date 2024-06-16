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
// is subject to the restrictions set forth by 60East Technologies Inc.
//
////////////////////////////////////////////////////////////////////////////

#ifndef _AMPS_SDK_PROTOCOL_AMPS_FRAMING_PROTOCOL_H_
#define _AMPS_SDK_PROTOCOL_AMPS_FRAMING_PROTOCOL_H_

#include <protocol/amps_protocol.h>
#include <arpa/inet.h>

///
/// AMPS String Framing Protocol
///
/// The string framing is a simple protocol where a 4 byte network
/// encoded value contains the number of bytes in the message. The
/// total number of bytes to be consumed will be 4 bytes plus the
/// number of bytes specified in the value.
/// Example:
/// [header][data] where the number of bytes in data is network encoded in header
///
extern "C"
int amps_string_framing_protocol_allocate_segment(amps_message_buffer_allocate_function allocate_,
                                                  amps_message_segment* pSegment_, size_t size_);
extern "C"
int amps_string_framing_protocol_finalize_segment(amps_message_segment* pSegment_, size_t messageSize_);
extern "C"
int amps_string_framing_protocol_chunk_message(const char* pBuffer_, size_t length_, amps_protocol_chunk* pMessage_);

///
/// AMPS WebSocket Protocol
///
/// The following functions implement the WebSocket Protocol
/// (http://tools.ietf.org/html/rfc6455).
///
extern "C"
int amps_websocket_framing_protocol_allocate_segment(amps_message_buffer_allocate_function allocate_,
                                                     amps_message_segment* pSegment_, size_t size_);
extern "C"
int amps_websocket_framing_protocol_finalize_segment(amps_message_segment* pSegment_, size_t messageSize_);
extern "C"
int amps_websocket_framing_protocol_chunk_message(const char* pBuffer_, size_t length_, amps_protocol_chunk* pMessage_);
extern "C"
int amps_websocket_framing_protocol_handshake(const char* pBuffer_, size_t length_,
                                              amps_protocol_handshake_response* pResponse_);

///
/// Inline implementation of AMPS String Framing Protocol
///

static const uint32_t amps_default_protocol_header_size    = 4;
static const size_t amps_default_protocol_max_message_size = 209715200; // 200MB

inline int amps_protocol_default_allocate_segment(amps_message_buffer_allocate_function allocate_,
                                                  amps_message_segment* pSegment_, size_t size_)
{
  uint32_t size = size_ + amps_default_protocol_header_size;

  amps_message_buffer buffer;
  allocate_(&buffer, size);

  amps_protocol_buffer* pProtocolBuffer = &pSegment_->protocol_buffer;
  uint32_t allocated = buffer.allocated;
  char* p = buffer.data;

  pProtocolBuffer->buffer_handle = buffer.handle;
  pProtocolBuffer->deallocate = buffer.deallocate;
  pProtocolBuffer->protocol_offset = 0;
  pProtocolBuffer->data_offset = amps_default_protocol_header_size;
  pProtocolBuffer->allocated = (allocated - amps_default_protocol_header_size);
  pProtocolBuffer->flags = 0;

  pSegment_->data = p + amps_default_protocol_header_size;
  pSegment_->offset = 0;
  pSegment_->length = 0;
  return AMPS_SUCCESS;
}

inline int amps_protocol_default_finalize_segment(amps_message_segment* pSegment_, size_t messageSize_)
{
  uint32_t* pui = reinterpret_cast<uint32_t*>(pSegment_->data - pSegment_->protocol_buffer.data_offset);
  *pui = (uint32_t)(htonl((uint32_t)messageSize_));
  return AMPS_SUCCESS;
}

inline int amps_protocol_default_chunk_message(const char* pBuffer_, size_t length_, amps_protocol_chunk* pMessage_)
{
  int status = AMPS_PROTOCOL_INCOMPLETE_MESSAGE;
  if (length_ >= amps_default_protocol_header_size)
  {
    // pointers can be cast to int regardless of alignment on x86
    uint32_t header = *(reinterpret_cast<const uint32_t*>(pBuffer_));
    size_t bytesToProcess = ntohl(header);
    pMessage_->length = bytesToProcess;
    if (bytesToProcess <= amps_default_protocol_max_message_size)
    {
      if ((length_ - amps_default_protocol_header_size) >= bytesToProcess)
      {
        pMessage_->data = pBuffer_ + amps_default_protocol_header_size;
        pMessage_->consumed_count = bytesToProcess + amps_default_protocol_header_size;
        // there are enough bytes for a message
        status = AMPS_PROTOCOL_COMPLETE_MESSAGE;
      }
      else
      {
        // incomplete but enough bytes to point to the actual data
        pMessage_->data = pBuffer_ + amps_default_protocol_header_size;
      }
    }
    else
    {
      pMessage_->data = 0;
      pMessage_->length = 0;
      pMessage_->consumed_count = 0;
      status = AMPS_PROTOCOL_ERROR;
    }
  }
  return status;
}

#endif
