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

#ifndef _AMPS_SDK_AMPS_COMMON_H_
#define _AMPS_SDK_AMPS_COMMON_H_

#include <module/amps_module.h>
#include <sys/types.h>
#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

///
/// @file amps_common.h
/// @brief Core type and function declarations for the AMPS Server SDK.
///

#define AMPS_BUFFER_TOO_SMALL    -1
#define AMPS_NULL_SOW_KEY        ~(0UL)

#define AMPS_BUFFER_IS_REF_COUNTED_MASK 0x1

///
/// All module functions provide access to closure data in order to
/// allow module developers to associate contextual information in
/// their module implementations with functions that are invoked
/// within AMPS. The various module API's implement this 'closure data'
/// approach via a create context API where the module creates a
/// context that is returned to AMPS as an opaque value. AMPS will
/// arrange to pass the opaque context value to the module when
/// invoking the functions provided by the module.
///

///
/// Opaque handle type set and used by AMPS.
/// The size of a pointer.
///
typedef void* amps_context;

///
/// Opaque handle returned from amps_message_type_create_context.
/// AMPS will store the value and pass it to message type module
/// functions. AMPS does not dereference this handle.
/// The size of a pointer.
///
typedef void* amps_message_type_context;

///
/// Opaque handle returned from amps_protocol_create_context.  AMPS
/// will store the value and pass it to protocol module functions.
/// AMPS does not dereference this handle.
///
typedef void* amps_protocol_context;

///
/// An opaque handle returned from amps_protocol_create_client. The
/// transport is required to call (if set) the create_client and
/// destroy_client functions when a new client is created and
/// destroyed. The opaque handle is passed to all the protocol
/// functions so that the protocol implementation can maintain per
/// client protocol level state.
///
typedef void* amps_protocol_client_handle;

/// an opaque id used by a protocol to identify commands, passed to
/// AMPS and returned to the protocol serialization functions
typedef void* amps_protocol_command_id;

/// an opaque handle to a protocol implementation structure that is
/// passed from AMPS into the protocol serialization functions.
typedef uint64_t amps_protocol_impl_handle;

/// Opaque handle returned from amps_transport_create_context.
/// AMPS does not dereference this handle.
typedef void* amps_transport_context;

/// A unique identifier returned by AMPS to identify an AMPS client
typedef void* amps_transport_client_id;

/// A unique identifier passed to AMPS by transport to identify a transport client
typedef void* amps_transport_client_handle;

/// A unique identifier passed to AMPS by transport to identify an inet_address
typedef void* amps_inet_address_handle;

/// Opaque handle passed by AMPS to indicate that inline select processing should occur.
typedef void* amps_select_list_handle;

struct amps_message_select_data
{
  amps_message_type_context  message_type_context;
  const char*                input_message;
  size_t                     input_byte_count;
  char*                      output_message;
  size_t                     output_capacity;
  size_t                     output_byte_count;
  amps_select_list_handle    select_list_handle;

};

typedef int (*amps_message_type_select_function)(struct amps_message_select_data*);

typedef uint64_t amps_message_buffer_handle;

struct amps_message_buffer;

typedef void (*amps_message_buffer_allocate_function)(struct amps_message_buffer*, size_t);
typedef void (*amps_message_buffer_deallocate_function)(amps_message_buffer_handle);
typedef void (*amps_message_buffer_reserve_function)(struct amps_message_buffer*, size_t);
typedef amps_message_buffer_handle (*amps_message_buffer_copy_function)(amps_message_buffer_handle);

// Structure to hold a byte array and a length
struct amps_byte_array
{
  void*   data;
  size_t  length;
};

struct amps_message_buffer
{
  amps_message_buffer_handle              handle;
  amps_message_buffer_deallocate_function deallocate;
  amps_message_buffer_copy_function       copy;
  size_t                                  allocated_byte_count;
  char*                                   data;
};

void amps_message_buffer_allocate(struct amps_message_buffer*, size_t);
void amps_message_buffer_deallocate(amps_message_buffer_handle);
void amps_message_buffer_reserve(struct amps_message_buffer*, size_t);
amps_message_buffer_handle amps_message_buffer_copy(amps_message_buffer_handle);

struct amps_message_segment;

/// prototype for a segment allocation function
typedef void (*amps_message_segment_allocate_function)(struct amps_message_segment*, size_t);

/// prototype for a segment deallocation function
typedef void (*amps_message_segment_deallocate_function)(struct amps_message_segment*);

/// prototype for a segment reserve function that will expand the
/// incoming segment to ensure that it can hold at least the number of
/// bytes specified
typedef void (*amps_message_segment_reserve_function)(amps_context, struct amps_message_segment*, size_t);

///
/// amps_message_segment is the basic unit of storage that the
/// serialization functions will use to form an outbound message. An
/// outbound message can consist of one or more segments and a
/// segment. In addition, a segment can be dynamic or static. When a
/// segment is allocated using a segment allocation function, the
/// 'deallocator' member will be set so that the segment can be
/// returned to the allocator that allocated it. When deallocator is
/// null then it is assumed that the handle is null and the data
/// points to data that has been statically allocated and whose
/// lifetime is guaranteed to exist past the lifetime of the segment.
///
/// A segment is limited to a length of 4GB. However, it is possible
/// to use a single allocated segment for multiple segments within a
/// message. All segments would have the same 'handle' and 'data' but
/// different 'offset' and 'length' values.
///
/// the data for a segment is:    const chart *p = segment.data + segment.offset;
/// the length of the segment is: uint32_t len = segment.length;
///
/// Example:
/// the "amps" protocol consists of a 4 byte network encoded length value, followed by JSON header
/// [4 byte length][amps protocol header][data]
/// protocol_start_offset == 0
/// protocol_byte_offset  == 0
/// data_byte_offset      == 4
/// data_byte_count       == length of the protocol header

/// Example with protocol_start_offset

/// the protocol_start_offset can be used to avoid shifting the data
/// after the serialization process is complete.

/// [8 bytes that are not used][4 byte length][amps protocol header][data]
///                            ^
///                            |
///                           pointer where send buffer should start
/// protocol_start_offset == 8
/// protocol_byte_offset  == 0
/// data_byte_offset      == 4
/// data_byte_count       == length of the protocol header

/// AMPS will init a send buffer such that the "data" to be sent
/// starts at data + protocol_start_offset

struct amps_protocol_buffer
{
  /// opaque handle set when a buffer is dynamically allocated
  amps_message_buffer_handle              buffer_handle;        // 8
  /// set by the AMPS buffer allocator so that is possible for the protocol or message type library to release a buffer
  amps_message_buffer_deallocate_function deallocate;           // 16
  /// set by the AMPS buffer allocator so that is possible for the protocol or message type library to copy a buffer
  amps_message_buffer_copy_function       copy;                 // 24
  /// the start of the protocol header wrt the allocated data
  uint32_t                                protocol_start_offset; // 28 the start of the protocol header inside the allocted data
  /// offset to where to the start of the protocol header
  uint32_t                                protocol_byte_offset; // 28 the start of the protocol header
  /// offset to the payload (protocol header size = (data_byte_offset - protocol_byte_offset))
  uint32_t                                data_byte_offset;     // 32
  /// data byte count represents the byte count starting at the data byte offset (see example above)
  uint32_t                                data_byte_count; // 40
  /// amount of memory allocated in the buffer available to be used
  size_t                                  allocated_byte_count; // 48
  /// flags used by the AMPS buffer allocator
  uint64_t                                flags;                // 56
  uint64_t                                is_direct;            // 64
};

struct amps_message_segment
{
  /// the protocol buffer (can be dynamic or static)
  struct amps_protocol_buffer  protocol_buffer;  // 56
  /// pointer to where the data portion of the segment begins
  char*                        data;             // 64
  /// byte offset to the data
  size_t                       byte_offset;      // 72 offset from data to the segment
  /// byte count of the data
  uint32_t                     byte_count;       // 76 the length of the segment
  uint32_t                     reserved1;        // 80
};

inline void amps_message_segment_init(struct amps_message_segment* pSegment_)
{
  memset((void*)pSegment_, 0, sizeof(struct amps_message_segment));
}

inline void amps_message_segment_copy(struct amps_message_segment* pDst_, struct amps_message_segment* pSrc_)
{
  if (pSrc_->protocol_buffer.flags)
  {
    pSrc_->protocol_buffer.copy(pSrc_->protocol_buffer.buffer_handle);
  }
  // indicate that the buffer has been placed on the segment list
  ++(pSrc_->protocol_buffer.flags);
  memcpy((void*)pDst_, (const void*)pSrc_, sizeof(struct amps_message_segment));
}

/// the initial number of segments in a segment list
#define AMPS_SEGMENT_LIST_STATIC_SIZE 3

/// prototype for a function to dynamically allocate a segment list of a specified size
typedef struct amps_message_segment* (*amps_message_segment_list_allocate_function)(uint32_t);
/// prototype for a function to deallocate a segment list
typedef void (*amps_message_segment_list_deallocate_function)(struct amps_message_segment*);

/// a list of segments that form an output message. the output message
/// length is the sum of all segment lengths in the list.
struct amps_message_segment_list
{
  /// function used to allocate a segment list when the existing list is exhausted
  amps_message_segment_list_allocate_function   allocate;                // 8
  /// function used to deallocate a segment list when a larger list is needed
  amps_message_segment_list_deallocate_function deallocate;              // 16
  /// the number of segments in the list
  uint32_t                                      segment_count;           // 20
  /// the number of segments available for push_back, e.g. size <= allocated
  uint32_t                                      allocated_segment_count; // 24
  /// a pointer to to start of the array list
  struct amps_message_segment*                   array;                   // 32
  /// an embedded segment list used to avoid dynamic allocation in the simple cases (< 3 segments)
  struct amps_message_segment                   static_array[AMPS_SEGMENT_LIST_STATIC_SIZE];
};

/// a structure that represents an input message arriving from a
/// transport
struct amps_input_message
{
  /// set by AMPS when an input message is passed to message_type library functions (parse_message etc)
  amps_message_type_context   message_type_context;
  /// set by AMPS when an input message is passed to protocol library functions (parse_header etc)
  amps_protocol_context       protocol_context;
  /// the protocol client handle created via the create_client function
  amps_protocol_client_handle protocol_client_handle;
  /// set by AMPS when an input message is passed to protocol library functions (original protocol_command_id set inside protocol chunk function)
  amps_protocol_command_id    protocol_command_id;
  /// opaque handle set and used by AMPS
  amps_context                context;
  /// pointer to the start of the input message
  const char*                 data;
  /// length of the input message
  size_t                      byte_count;
};


/// a structure that represents a part of an input or output message
/// that should be formatted for tracing.
struct amps_trace_buffer
{
  /// set by AMPS to the beginning of the data to be formatted by the
  /// message type or protocol module.
  const char* data;
  /// set by AMPS to indicate the length of the data to be formatted.
  size_t      length;
};

/// a structure that represents a location to write formatted trace output to.
/// output and capacity and length are filled by the caller. The callee
/// should begin writing trace data at output+length and then update "length"
/// by adding the number of additional bytes consumed.
struct amps_trace_output_buffer
{
  /// The beginning of output data; set by AMPS.
  char*  output;
  /// The available size for writing output; set by AMPS.
  size_t capacity;
  /// The number of bytes written to output; set by AMPS, protocol and
  ///  message type modules.
  size_t length;
};

/// Function signature for a message type tracing function; defined here
/// because it is needed by a structure in amps_protocol.h.
typedef int(*amps_message_type_trace_function)(amps_message_type_context,
                                               const struct amps_trace_buffer*,
                                               struct amps_trace_output_buffer*);

inline uint32_t amps_message_segment_available_capacity(struct amps_message_segment* pSegment_)
{
  return (pSegment_->protocol_buffer.allocated_byte_count - (pSegment_->byte_offset + pSegment_->byte_count));
}

/// AMPS convenience function to ensure that a segment list has at
/// least the specified segment count
///
/// \param pList_               the segment list to reserve space for
/// \param reserveSegmentCount_ the segment count to reserve
inline void amps_message_segment_list_reserve(struct amps_message_segment_list* pList_,
                                              uint32_t reserveSegmentCount_)
{
  if (reserveSegmentCount_ >= pList_->allocated_segment_count)
  {
    uint32_t i = 0;
    struct amps_message_segment* tmp = pList_->allocate(reserveSegmentCount_);
    for (; i != pList_->segment_count; ++i)
    {
      tmp[i] = pList_->array[i];
    }
    if (pList_->array != &pList_->static_array[0])
    {
      pList_->deallocate(pList_->array);
    }
    pList_->allocated_segment_count = reserveSegmentCount_;
    pList_->array = tmp;
  }
}

/// AMPS convenience function to push a static data block onto a
/// segment list. The data pushed must be guaranteed to outlive the
/// lifetime of the segment, i.e. it will not be copied.
///
/// \param pList_     the segment list where the static data will be pushed onto
/// \param pData_     pointer to the data
/// \param byteCount_ the byte count of the data segment
inline void amps_message_segment_list_push_back_static_segment(struct amps_message_segment_list* pList_,
                                                               const char* pData_,
                                                               uint32_t byteCount_)
{
  if (pList_->segment_count < pList_->allocated_segment_count)
  {
    struct amps_message_segment* pSegment = &pList_->array[pList_->segment_count];

    pSegment->data = (char*)pData_;
    pSegment->byte_offset = 0;
    pSegment->byte_count = byteCount_;
    ++pList_->segment_count;
  }
  else
  {
    amps_message_segment_list_reserve(pList_, pList_->allocated_segment_count << 1);
    amps_message_segment_list_push_back_static_segment(pList_, pData_, byteCount_);
  }
}

/// AMPS convenience function to push a segment onto a segment list.
///
/// \param pList_    the segment list where the segment will be pushed onto
/// \param pSegment_ the segment to push onto the list
inline void amps_message_segment_list_push_back_segment(struct amps_message_segment_list* pList_,
                                                        struct amps_message_segment* pSegment_)
{
  if (pList_->segment_count < pList_->allocated_segment_count)
  {
    amps_message_segment_copy(&pList_->array[pList_->segment_count], pSegment_);
    ++pList_->segment_count;
  }
  else
  {
    amps_message_segment_list_reserve(pList_, pList_->allocated_segment_count << 1);
    amps_message_segment_list_push_back_segment(pList_, pSegment_);
  }
}

/// AMPS convenience function to push a segment onto a segment list
/// with the specified length (the length overrides the length set on
/// the specified segment)
///
/// \param pList_     the segment list where the segment will be pushed onto
/// \param pSegment_  the segment to push onto the list
/// \param byteCount_ the byte count of the segment
inline void amps_message_segment_list_push_back_segment_with_length(struct amps_message_segment_list* pList_,
                                                                    struct amps_message_segment* pSegment_,
                                                                    uint32_t byteCount_)
{
  if (pList_->segment_count < pList_->allocated_segment_count)
  {
    struct amps_message_segment* pSegment = &pList_->array[pList_->segment_count];
    amps_message_segment_copy(&pList_->array[pList_->segment_count], pSegment_);
    pSegment->byte_count = byteCount_;
    ++pList_->segment_count;
  }
  else
  {
    amps_message_segment_list_reserve(pList_, pList_->allocated_segment_count << 1);
    amps_message_segment_list_push_back_segment_with_length(pList_, pSegment_, byteCount_);
  }
}

/// AMPS convenience function to push a segment onto a segment list
/// with the specified length and offset (these values override the
/// length and offset set on the specified segment).
///
/// \param pList_     the segment list where the segment will be pushed onto
/// \param pSegment_  the segment to push onto the list
/// \param byteCount_ the byte count of the segment
inline void amps_message_segment_list_push_back_segment_with_offset(struct amps_message_segment_list* pList_,
                                                                    struct amps_message_segment* pSegment_,
                                                                    size_t offset_,
                                                                    uint32_t byteCount_)
{
  if (pList_->segment_count < pList_->allocated_segment_count)
  {
    struct amps_message_segment* pSegment = &pList_->array[pList_->segment_count];
    amps_message_segment_copy(&pList_->array[pList_->segment_count], pSegment_);
    pSegment->byte_offset = offset_;
    pSegment->byte_count = byteCount_;
    ++pList_->segment_count;
  }
  else
  {
    amps_message_segment_list_reserve(pList_, pList_->allocated_segment_count << 1);
    amps_message_segment_list_push_back_segment_with_offset(pList_, pSegment_, offset_, byteCount_);
  }
}

/// @addtogroup command_identifiers
/// @{
/// Constant for a subscribe command
#define AMPS_COMMAND_SUBSCRIBE               0
/// Constant for an unsubscribe command
#define AMPS_COMMAND_UNSUBSCRIBE             1
/// Constant for a publish command
#define AMPS_COMMAND_PUBLISH                 2
/// Constant for a SOW command
#define AMPS_COMMAND_SOW                     3
/// Constant for a heartbeat command
#define AMPS_COMMAND_HEARTBEAT               4
/// Constant for a logon command
#define AMPS_COMMAND_LOGON                   5
/// Constant for a start timer command
#define AMPS_COMMAND_START_TIMER             6
/// Constant for a stop timer command
#define AMPS_COMMAND_STOP_TIMER              7
/// Constant for a SOW and subscribe command
#define AMPS_COMMAND_SOW_AND_SUBSCRIBE       8
/// Constant for a delta publish command
#define AMPS_COMMAND_DELTA_PUBLISH           9
/// Constant for a create view command
#define AMPS_COMMAND_CREATE_VIEW             10
/// Constant for a delta subscribe
#define AMPS_COMMAND_DELTA_SUBSCRIBE         11
/// Constant for a SOW and delta subscribe
#define AMPS_COMMAND_SOW_AND_DELTA_SUBSCRIBE 12
/// Constant for a SOW delete
#define AMPS_COMMAND_SOW_DELETE              13
/// Constant for a create replica command
#define AMPS_COMMAND_CREATE_REPLICA          14
/// Constant for a group begin command
#define AMPS_COMMAND_GROUP_BEGIN             15
/// Constant for a group end command
#define AMPS_COMMAND_GROUP_END               16
/// Constant for an OOF message
#define AMPS_COMMAND_OOF                     17
/// Constant for an ACK message
#define AMPS_COMMAND_ACK                     18
/// Constant for a flush message
#define AMPS_COMMAND_FLUSH                   19
/// Constant for a unknown command
#define AMPS_COMMAND_INTERNAL                63
/// @}

/// @addtogroup ack_identifiers
/// @{
/// Constant for a none acknowledgement type
#define AMPS_ACK_NONE      0x0
/// Constant for a received acknowledgement type
#define AMPS_ACK_RECEIVED  0x1
/// Constant for a parsed acknowledgement type
#define AMPS_ACK_PARSED    0x2
/// Constant for a persisted acknowledgement type
#define AMPS_ACK_PERSISTED 0x4
/// Constant for a processed acknowledgement type
#define AMPS_ACK_PROCESSED 0x8
/// Constant for a completed acknowledgement type
#define AMPS_ACK_COMPLETED 0x10
/// Constant for a stats acknowledgement type
#define AMPS_ACK_STATS     0x20
/// @}

/**
 * Indicator for the type of an AMPS command.
 */
enum amps_command_type
{
  amps_ct_publish                 = AMPS_COMMAND_PUBLISH,
  amps_ct_delta_publish           = AMPS_COMMAND_DELTA_PUBLISH,
  amps_ct_subscribe               = AMPS_COMMAND_SUBSCRIBE,
  amps_ct_sow_delete              = AMPS_COMMAND_SOW_DELETE,
  amps_ct_sow                     = AMPS_COMMAND_SOW,
  amps_ct_sow_and_subscribe       = AMPS_COMMAND_SOW_AND_SUBSCRIBE,
  amps_ct_delta_subscribe         = AMPS_COMMAND_DELTA_SUBSCRIBE,
  amps_ct_sow_and_delta_subscribe = AMPS_COMMAND_SOW_AND_DELTA_SUBSCRIBE,
  amps_ct_oof                     = AMPS_COMMAND_OOF,
  amps_ct_group_begin             = AMPS_COMMAND_GROUP_BEGIN,
  amps_ct_group_end               = AMPS_COMMAND_GROUP_END,
  amps_ct_ack                     = AMPS_COMMAND_ACK,
  amps_ct_internal                = AMPS_COMMAND_INTERNAL,
  amps_ct_logon                   = AMPS_COMMAND_LOGON
};

#ifdef __cplusplus
}
#endif

#endif
