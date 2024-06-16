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

#ifndef _AMPS_SDK_PROTOCOL_AMPS_PROTOCOL_H_
#define _AMPS_SDK_PROTOCOL_AMPS_PROTOCOL_H_

#include <amps_common.h>
#include <arpa/inet.h>
#include <string.h>

///
/// @file protocol/amps_protocol.h
/// @brief Core type and function declarations for the AMPS SDK Protocol Plugin API.
///


/// @defgroup command_identifiers Command identifier constants
/// AMPS 'command' identifiers passed from protocol header parsing
/// function to AMPS.
///


/*
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
#define AMPS_COMMAND_FLUSH                   20
/// @}
*/

#ifndef AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_CMD_ID

  ///
  /// AMPS 'header' field identifiers passed from protocol header
  /// parsing function to AMPS.
  ///
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_CMD_ID            0
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_TOPIC             1
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_SOURCE_NAME       2
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_SOW_KEY           3
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_SOW_KEYS          4
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_CORRELATION_ID    5
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_AUTH_ID           6
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_USER_ID           7
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_CLIENT_NAME       8
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_OPTIONS           9
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_FILTER            10
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_BOOKMARK          11
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_SUB_ID            12
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_QUERY_ID          13
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_ORDER_BY          14
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_VERSION           15
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_PASSWORD          16
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_GROUP_NAME        17
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_MSG_TYPE          18

  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_LAST_STRING_FIELD AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_MSG_TYPE
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_MAX_STRING_FIELDS AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_LAST_STRING_FIELD + 1

  /// numeric or bool header fields
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_CMD               19
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_CLIENT_SEQ        20
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_ACK_TYPE          21
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_EXPIRATION        22
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_BATCH_SIZE        23
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_QUERY_INTERVAL    24
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_TOP_N             25
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_HEARTBEAT         26
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_TIMEOUT_INTERVAL  27
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_LEASE_PERIOD      28
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_SKIP_N            29

  /// deprecated numeric or bool header fields (subsumed into OPTIONS)
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_MAX_MSGS          30  // DEPRECATED
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_SEND_MATCHING_IDS 31  // DEPRECATED
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_SEND_OOF          32  // DEPRECATED
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_SEND_KEYS         33  // DEPRECATED
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_SEND_EMPTY        34  // DEPRECATED
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_DATA_ONLY         35  // DEPRECATED
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_USE_NS            36  // DEPRECATED
  #define AMPS_INPUT_MESSAGE_HEADER_FIELD_ID_MAX_FIELDS        37  // DEPRECATED

#endif // AMPS_INPUT_MESSAGE_HEADER_FIELD_ID

///
/// AMPS heartbeat types.
///
#define AMPS_HEARTBEAT_START   0x0
#define AMPS_HEARTBEAT_STOP    0x1
#define AMPS_HEARTBEAT_BEAT    0x2
#define AMPS_HEARTBEAT_UNKNOWN 0x3

#define AMPS_FLAG_IS_REPLICATION   0x1
#define AMPS_FLAG_IS_TOPIC_ESCAPED 0x2

///
/// AMPS 'header' field type identifiers passed from AMPS to protocol
/// serialization functions to identify the type data in the header
/// field (see amps_output_message_header_field)
///
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_TYPE_STRING    0
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_TYPE_INT       1
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_TYPE_UINT      2
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_TYPE_DOUBLE    3
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_TYPE_BOOL      4
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_TYPE_DATE_TIME 5
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_TYPE_SOW_KEY   6

///
/// AMPS 'header' field identifiers passed from AMPS to protocol
/// serialization functions (see amps_output_message_header_field)
///
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CMD               0
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CMD_ID            1
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_USER_ID           2
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_TOPIC             3
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_SUB_IDS           4
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CORRELATION_ID    5
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_SOW_KEY           6
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_MSG_LEN           7
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CLIENT_SEQ        8
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_BOOKMARK          9
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_TIMESTAMP         10
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_LEASE_PERIOD      11
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CLIENT_NAME       12
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_ACK_TYPE          13
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_SUB_ID            14
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_STATUS            15
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_REASON            16
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_QUERY_ID          17
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_BATCH_SIZE        18
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_TOPIC_MATCHES     19
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_MATCHES           20
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_GROUP_SEQ_NUM     21
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_PASSWORD          22
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_OPTIONS           23
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_RECORDS_DELETED   24
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_RECORDS_RETURNED  25
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_RECORDS_INSERTED  26
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_RECORDS_UPDATED   27
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_VERSION           28
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_MAX_FIELDS        29

#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CMD_MASK               (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CMD)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CMD_ID_MASK            (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CMD_ID)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_USER_ID_MASK           (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_USER_ID)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_TOPIC_MASK             (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_TOPIC)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_SUB_IDS_MASK           (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_SUB_IDS)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CORRELATION_ID_MASK    (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CORRELATION_ID)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_SOW_KEY_MASK           (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_SOW_KEY)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_MSG_LEN_MASK           (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_MSG_LEN)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CLIENT_SEQ_MASK        (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CLIENT_SEQ)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_BOOKMARK_MASK          (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_BOOKMARK)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_TIMESTAMP_MASK         (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_TIMESTAMP)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_LEASE_PERIOD_MASK      (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_LEASE_PERIOD)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CLIENT_NAME_MASK       (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_CLIENT_NAME)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_ACK_TYPE_MASK          (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_ACK_TYPE)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_SUB_ID_MASK            (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_SUB_ID)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_STATUS_MASK            (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_STATUS)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_REASON_MASK            (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_REASON)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_QUERY_ID_MASK          (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_QUERY_ID)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_BATCH_SIZE_MASK        (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_BATCH_SIZE)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_TOPIC_MATCHES_MASK     (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_TOPIC_MATCHES)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_MATCHES_MASK           (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_MATCHES)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_GROUP_SEQ_NUM_MASK     (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_GROUP_SEQ_NUM)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_PASSWORD_MASK          (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_PASSWORD)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_OPTIONS_MASK           (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_OPTIONS)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_RECORDS_DELETED_MASK   (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_RECORDS_DELETED)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_RECORDS_RETURNED_MASK  (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_RECORDS_RETURNED)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_RECORDS_INSERTED_MASK  (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_RECORDS_INSERTED)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_RECORDS_UPDATED_MASK   (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_RECORDS_UPDATED)
#define AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_VERSION_MASK           (0x1UL << AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_VERSION)

inline uint64_t amps_output_message_header_field_mask(uint64_t fieldId_)
{
  return 0x1UL << fieldId_;
}

inline void amps_output_message_header_field_mask_set(uint64_t& bitmask_, uint64_t mask_)
{
  bitmask_ |= mask_;
}

inline void amps_output_message_header_field_mask_unset(uint64_t& bitmask_, uint64_t mask_)
{
  bitmask_ &= ~mask_;
}

///
/// AMPS protocol 'chunk message' return codes
///

/// indicates that a complete message is avaible for delivery into AMPS
#define AMPS_PROTOCOL_COMPLETE_MESSAGE    1
/// indicates that there are not enough bytes to form a complete message
#define AMPS_PROTOCOL_INCOMPLETE_MESSAGE  0
/// a protocol error has been detected when trying to form a message
#define AMPS_PROTOCOL_ERROR              -1
#define AMPS_PROTOCOL_ERROR_HTTP_POST 0x504F5354
#define AMPS_PROTOCOL_ERROR_HTTP_GET 0x47455420
#define AMPS_PROTOCOL_ERROR_XML 0x3C3F786D
#define AMPS_PROTOCOL_ERROR_KERBEROS 0x4D415445
#define AMPS_PROTOCOL_ERROR_TELNET_CLOSE 0xFFF4FFFD
// TODO Define websocket protocol error

struct amps_protocol_chunk;

/// reserve count_ messages inside amps_protocol_chunk. if a
/// reallocation is required then existing array will be memcpy'd
typedef int (*amps_protocol_chunk_message_reserve_function)(size_t count_, amps_protocol_chunk*);

struct amps_protocol_output_buffer
{
  void*   handle;
  char*   data;
  size_t  byte_count;
  size_t  allocated_byte_count;
};

typedef void (*amps_protocol_output_buffer_reserve_function)(amps_protocol_output_buffer*, size_t);

struct amps_protocol_deflate_request
{
  /// the protocol context as returned by the protocol create_context function
  amps_protocol_context                         protocol_context;
  /// the opaque protocol impl handle that can be casted to a protocol implemention pointer
  amps_protocol_impl_handle                     protocol_handle;
  /// the protocol client handle created via the create_client function
  amps_protocol_client_handle                   protocol_client_handle;
  size_t                                        input_byte_count;     // byte count for all input_data segments
  size_t                                        input_segment_count;  // the number of 'input' segments
  const amps_byte_array*                        input_data;           // the segments of data for all messages
  amps_protocol_output_buffer_reserve_function  output_buffer_reserve;
  amps_protocol_output_buffer                   output_buffer;
  size_t                                        consumed_byte_count;
  size_t                                        consumed_segment_count; // must consume whole segments
};

/// deflate function that can be specified in the handshake
typedef int (*amps_protocol_deflate_function)(amps_protocol_deflate_request*);

// a protocol message is produced inside the protocol 'chunk function'
// and indicates the bounaries of a complete message that will be
// delivered into AMPS.
struct amps_protocol_message
{
  /// pointer to the start of the message bytes
  const char*               data;
  /// length of the message
  size_t                    byte_count;
  amps_protocol_command_id  command_id;
};

/// prototype for a function used by the transport to create options
/// that are passed to the transport logon function.  the protocol
/// will use the allocation function to allocate the bytes to hold the
/// options string. the options string will be in 'json' format.

typedef void* (*amps_protocol_options_allocate_function)(size_t byteCount_, size_t* allocatedByteCount_);

struct amps_protocol_options
{
  /// allocator to be used to allocate bytes to hold the options
  amps_protocol_options_allocate_function   allocate;
  /// pointer to the start of the options
  const char*                               data;
  /// the length of the options
  size_t                                    byte_count;
  /// the number of bytes allocated for the options
  size_t                                    allocated_byte_count;
};

/// prototype for a function used by the transport to create a
/// response message. if a response is required then allocator is used
/// to allocate the bytes for the response message.

typedef void* (*amps_protocol_response_allocate_function)(size_t byteCount_, size_t* allocatedByteCount_);

struct amps_protocol_response
{
  /// allocator to be used to allocate bytes to hold a response message
  amps_protocol_response_allocate_function  allocate;
  /// pointer to the start of the response message
  const char*                               data;
  /// the length of the response message
  size_t                                    byte_count;
  /// the number of bytes allocated for the response
  size_t                                    allocated_byte_count;
};

/// a amps_protocol_create_client_params struct is passed to the
/// protocol client create function and contains information regarding
/// the client. the connect_preamble can be used by the protocol
/// create_client function to convey a response that can be set back
/// to the client.
struct amps_protocol_create_client_params
{
  /// the protocol context as returned by the protocol create_context function
  amps_protocol_context   protocol_context;
  /// description of the client, i.e. "peername -> localname"
  const char*             client_description;
  /// length of the description
  size_t                  client_description_length;
  /// the client name as set by AMPS
  const char*             client_name;
  /// the length the client name
  size_t                  client_name_length;
  /// AMPS_TRANSPORT_INBOUND_CLIENT or AMPS_TRANSPORT_OUTBOUND_CLIENT
  uint64_t                direction;
  // can be set by AMPS via protocol to indicate that upon connect a string should be sent to client
  amps_protocol_response  connect_preamble;
  /// client's transport address
  const char*             transport_address;
  /// length of the transport address host
  size_t                  transport_address_length;
  /// client's transport address host
  const char*             transport_address_host;
  /// length of the transport address host
  size_t                  transport_address_host_length;
  /// client's transport address port
  const char*             transport_address_port;
  /// length of the transport address port
  size_t                  transport_address_port_length;
  const char*             client_local_name;
  size_t                  client_local_name_length;
  const char*             client_peer_name;
  size_t                  client_peer_name_length;
  /// whether or not the transport is using SSL/TLS
  bool                    is_ssl_enabled;
};

/// a protocol chunk is produced inside the protocol 'chunk function'
/// and includes 1 or more complete messages that will be delivered
/// into AMPS.
struct amps_protocol_chunk
{
  /// the protocol context as returned by the protocol create_context function
  amps_protocol_context                         protocol_context;
  /// the opaque protocol impl handle that can be casted to a protocol implemention pointer
  amps_protocol_impl_handle                     protocol_handle;
  /// the protocol client handle created via the create_client function
  amps_protocol_client_handle                   protocol_client_handle;
  /// pointer to the start of the input data
  const char*                                   input_data;
  /// the length of the input data
  size_t                                        input_byte_count;

  /// response message to be delivered (and freed) if allocated and set
  amps_protocol_response                        response;

  /// reserve function to be called when 'allocated' is not large
  /// enough to hold the number of messages produced in a chunk
  amps_protocol_chunk_message_reserve_function  reserve;
  /// allocated_count is the max number of messages available to be
  /// filled in during the 'chunk' function. reserve can be called to
  /// get additional messages.
  size_t                                        allocated_count;
  /// number of bytes that were consumed to form the messages
  size_t                                        consumed_byte_count;
  /// if the segment_count is non-zero then message_count will be 1
  /// and the messages should be considered a 'gather list' that forms
  /// a single message
  size_t                                        segment_count;
  /// if the segment_count is non-zero then segments_byte_count is the
  /// number of bytes for all segments
  size_t                                        segments_byte_count;
  /// number of messages to be processed
  size_t                                        message_count;
  /// byte count for various protocol errors
  size_t                                        protocol_error;
  /// an array of messages to be processed
  amps_protocol_message*                        messages;
};

/// a structure that represents a protocol handshake. a handshake
/// structure is passed into the protocol handshake function and it
/// may or may not fill in the response fields.
struct amps_protocol_handshake
{
  /// the protocol context as returned by the protocol create_context function
  amps_protocol_context                     protocol_context;
  /// the opaque protocol impl handle that can be casted to a protocol implemention pointer
  amps_protocol_impl_handle                 protocol_handle;
  /// the protocol client handle created via the create_client function
  amps_protocol_client_handle               protocol_client_handle;
  /// pointer to the start of the input data
  const char*                               input_data;
  /// the length of the input data
  size_t                                    input_byte_count;
  /// the number of bytes consumed processing the handshake
  size_t                                    consumed_byte_count;
  /// response message to be delivered (and freed) if allocated and set
  amps_protocol_response                    response;
  /// options to be passed to the transport logon function (allocated and set by the handshake function)
  amps_protocol_options                     options;
  /// set by the handshake when deflate is requested at the protocol level
  amps_protocol_deflate_function            deflate;
};

/// a structure that represents a protocol upgrade. a upgrade
/// structure is passed into the protocol upgrade function and it
/// may or may not fill in the response fields.
struct amps_protocol_upgrade
{
  /// the protocol context as returned by the protocol create_context function
  amps_protocol_context                     protocol_context;
  /// the opaque protocol impl handle that can be casted to a protocol implemention pointer
  amps_protocol_impl_handle                 protocol_handle;
  /// the protocol client handle created via the create_client function
  amps_protocol_client_handle               protocol_client_handle;
  /// pointer to the start of the input data
  const char*                               input_data;
  /// the length of the input data
  size_t                                    input_byte_count;
  /// the number of bytes consumed processing the upgrade
  size_t                                    consumed_byte_count;
  /// response message to be delivered (and freed) if allocated and set
  amps_protocol_response                    response;
};

/// a structure that represents a protocol logon_complete. a logon
/// complete structure is passed into the protocol logon_complete
/// function after the transport executes the logon requested in the
/// handshake and it may or may not fill in the response fields.
struct amps_protocol_logon_completion
{
  /// the protocol context as returned by the protocol create_context function
  amps_protocol_context                     protocol_context;
  /// the opaque protocol impl handle that can be casted to a protocol implemention pointer
  amps_protocol_impl_handle                 protocol_handle;
  /// the protocol client handle created via the create_client function
  amps_protocol_client_handle               protocol_client_handle;
  /// the return code from the transport logon call
  int                                       logon_status;
  /// logon options created in the transport logon function and passed to the logon_completion function
  amps_protocol_options*                    logon_options;
  /// response message to be delivered (and freed) if allocated and set
  amps_protocol_response                    response;
  amps_protocol_handshake*                  handshake;
};

/// a structure that represents a protocol header field. each header
/// field shall have a type, a field id and a value. the value union
/// will be set in accordance with the type of field. for strings, the
/// length data member will contain the length of the string.
struct amps_output_message_header_field
{
  /// the type of the header field
  uint8_t  type;
  uint8_t  reserved;
  /// the header field identifier
  uint16_t field_id;
  /// the length when the type is 'string' limited to 4GB
  uint32_t length;
  union
  {
    /// the start of the data for a string value
    const char* s;
    /// a double value
    double      f;
    /// an signed 64 bit integer value
    int64_t     i;
    /// an unsigned 64 bit integer value
    uint64_t    u;
    /// an unsigned 8 bit integer value used for booleans
    uint8_t     b;
  } value;
};

/// a structure to hold an array of header fields passed to the
/// protocol serialization functions.
struct amps_output_message_header
{
  /// the protocol context as returned by the protocol create_context function
  amps_protocol_context            protocol_context;           // 8
  /// the opaque protocol impl handle that can be casted to a protocol implemention pointer
  amps_protocol_impl_handle        protocol_handle;            // 16
  /// the protocol client handle created via the create_client function
  amps_protocol_client_handle      protocol_client_handle;     // 24
  /// the protocol_command_id for the command otherwise 0 (used for ack and group serialization)
  amps_protocol_command_id         protocol_command_id;        // 32
  /// a bit mask where 0x1<<field_id is set for each field that is valid
  uint64_t                         field_mask;                 // 40
  /// the number of fields in field_mask that are set
  uint64_t                         field_count;                // 48
  /// the max byte count for field values
  uint32_t                         field_max_byte_count;       // 52
  /// set by the protocol 'compute_serialize_size' function
  uint32_t                         serialize_size;             // 56
  // the following fields are for internal protocol publish and oof messages
  uint64_t                         topic_hash;                 // 64
  uint64_t                         sow_key;                    // 8
  int64_t                          timestamp;                  // 16
  uint16_t                         command_type;               // 18
  uint16_t                         ack_type;                   // 20
  uint16_t                         oof_reason;                 // 22
  uint16_t                         flags;                      // 24
  uint64_t                         sow_batch_count;            // 32
  uint64_t                         client_name_hash;           // 40
  uint64_t                         client_seq;                 // 48
  void*                            user_data;                  // 56
  void*                            reserved;                   // 64
  amps_output_message_header_field publisher_name;             // 16
  /// an array of header fields where only fields set in the field_mask are valid
  // sizeof(amps_output_message_header_field) == 16
  amps_output_message_header_field fields[AMPS_OUTPUT_MESSAGE_HEADER_FIELD_ID_MAX_FIELDS];
};

/// a structure passed from AMPS to the protocol serialization
/// functions that serves as both input and output wrt producing a
/// message for delivery by AMPS. the serialization functions will be
/// passed a protocol output message with the allocation,
/// protocol_context, protocol_handle, header, request_one and data
/// members set. these values will be used by the serialization
/// functions to build a list of segments in the segment_list member
/// value. the byte_count will also be set to indicate the total
/// number of bytes for all segments in the segment list.
struct amps_protocol_output_message
{
  /// segment allocator to allocate segments for pushing onto segment_list
  amps_message_buffer_allocate_function  allocate;                    // 8
  /// the header fields for the output message
  const amps_output_message_header*      header;                      // 16
  /// set by the serialization function to indicate number of byes in the output message
  size_t                                 byte_count;                  // 24
  /// if true then AMPS is requesting that only one segment be created for the message
  uint32_t                               request_one;                 // 28 - if true only return a single segment
  uint32_t                               reserved1;                   // 32
  uint64_t                               reserved2;                   // 40
  /// a list of segments that compose the output message
  amps_message_segment_list*             segment_list;                // 48
  /// A message type context to pass to the message_type_trace_function.
  amps_message_type_context              message_type_context;        // 56
  /// The message type trace function to call with the message payload.
  amps_message_type_trace_function       message_type_trace_function; // 64
  /// the data for the output message which can be pushed onto the segment_list
  amps_message_segment                   data;                        // 128
};

// a structure passed from AMPS to the protocol sow serialization
// function when AMPS is forming a SOW result.
struct amps_output_message_sow_data
{
  /// segment allocator to allocate a segment to hold the serialized sow data
  amps_message_buffer_allocate_function allocate;                    // 8
  /// the protocol context as returned by the protocol create_context function
  amps_protocol_context                 protocol_context;            // 16
  /// the opaque protocol impl handle that can be casted to a protocol implemention pointer
  amps_protocol_impl_handle             protocol_handle;             // 24
  /// the protocol client handle created via the create_client function
  amps_protocol_client_handle           protocol_client_handle;
  /// a pointer to the start of the sow data
  const char*                           data;                        // 32
  /// AMPS can set the sowkey as either a numeric value or a
  /// string. if sowkey_byte_count is non-zero then use sowkey.s and
  /// sowkey_byte_count otherwise use sowkey.u
  union
  {
    /// a pointer to the start of string based sowkey
    const char* s;
    /// a numeric sow key
    uint64_t    u;
  } sowkey;                                                          // 40
  /// a pointer to the start of the correlation_id
  const char*                           correlation_id;              // 48
  /// the length of the sow data
  uint32_t                              data_byte_count;             // 52
  /// the length of a string based sowkey (if 0 then it is numeric)
  uint32_t                              sowkey_byte_count;           // 56
  /// the length of the correlation_id
  uint32_t                              correlation_id_byte_count;   // 60
  uint32_t                              topic_byte_count;            // 64
  /// the topic hash of the sow topic
  const char*                           topic;                       // 72
  uint64_t                              topic_hash;                  // 80
  /// the timestamp of the message (when it was last updated)
  int64_t                               timestamp;                   // 88
  /// the client name hash of the message bookmark
  uint64_t                              client_name_hash;            // 96
  /// the client seq of the message bookmark
  uint64_t                              client_seq;                  // 104
  /// A message type context to pass to the message_type_trace_function.
  amps_message_type_context             message_type_context;        // 112
  /// If select_list handle is set then select processing processing can occur inline.
  amps_message_type_select_function     message_type_select_function;// 120
  amps_select_list_handle               select_list_handle;          // 128
  /// The message type trace function to call with the message payload.
  amps_message_type_trace_function      message_type_trace_function; // 136
  /// the output segment that contains the serialized sow data to be part of a SOW response
  amps_message_segment                  segment;                     // 216 where sizeof(amps_message_segment) == 80
};

/// Structure used to pass trace information to a protocol's
/// amps_protocol_trace entry point, specifying the input message to
/// be traced the output buffer to write the trace messasge to, and
/// the message type function that should be chained if a message
/// exists.

/// the max number of segments that can form a trace message
#define AMPS_TRACE_SEGMENT_LIST_MAX_SIZE 3
#define AMPS_TRACE_SEGMENT_LIST_MAX_COUNT 3

struct amps_trace_context
{
  /// A pointer to the first amps_trace_buffer containing input or
  /// output message data to be traced.
  amps_trace_buffer*               buffers;
  /// The size of the array (in elements) buffer_list.
  size_t                           buffer_count;
  /// if non-zero then byte count of the protocol portion of the first buffer
  size_t                           protocol_byte_count;
  /// the number of trace segments up (0 to max)
  size_t                           segment_count;
  /// the trace segments - the protocol will decide how to put all of the data into 3 segments
  amps_trace_buffer                segment_list[AMPS_TRACE_SEGMENT_LIST_MAX_COUNT];
  /// The output buffer to write trace data to.
  amps_trace_output_buffer         output_buffer;
  /// A message type context to pass to the message_type_trace_function.
  amps_message_type_context        message_type_context;
  /// The message type trace function to call with the message payload.
  amps_message_type_trace_function message_type_trace_function;
  /// An opaque message handle
  const void*                      message_handle;
  bool                             is_pretty_print_buffer;
};

/// AMPS convenience function to ensure that the segment list of an
/// output message has at least the specified number of entries
///
/// \param pMessage_     a pointer to the output message
/// \param newListCount_ the desired segment count
inline void amps_protocol_output_message_segment_list_reserve(amps_protocol_output_message* pMessage_,
                                                              size_t newListCount_)
{
  amps_message_segment_list_reserve(pMessage_->segment_list, newListCount_);
}

/// AMPS convenience function to push a static data block onto the
/// segment list of an output message.  The data pushed must be
/// guaranteed to outlive the lifetime of the segment, i.e. it will
/// not be copied.
///
/// \param pMessage_  a pointer to the output message
/// \param pData_     a pointer to the data
/// \param byteCount_ the byte count of the data segment
inline void amps_protocol_output_message_push_back_static_segment(amps_protocol_output_message* pMessage_,
                                                                  const char* pData_,
                                                                  size_t byteCount_)
{
  amps_message_segment_list_push_back_static_segment(pMessage_->segment_list, pData_, byteCount_);
  pMessage_->byte_count += byteCount_;
}

/// AMPS convenience function to push the data segment of an output
/// message onto the segment list.
///
/// \param pMessage_ a pointer to the output message
inline void amps_protocol_output_message_push_back_data_segment(amps_protocol_output_message* pMessage_)
{
  amps_message_segment_list_push_back_segment(pMessage_->segment_list, &pMessage_->data);
  pMessage_->byte_count += pMessage_->data.byte_count;
}

/// AMPS convenience function to push a segment the segment list of an
/// output message
///
/// \param pMessage_ a pointer to the output message
/// \param pSegment_ the segment to push onto the list
inline void amps_protocol_output_message_push_back_segment(amps_protocol_output_message* pMessage_,
                                                           amps_message_segment* pSegment_)
{
  amps_message_segment_list_push_back_segment(pMessage_->segment_list, pSegment_);
  pMessage_->byte_count += pSegment_->byte_count;
}

/// AMPS convenience function to push a segment onto the segment list
/// of an output message with the specified length (the length
/// overrides the length set on the specified segment)
///
/// \param pMessage_  a pointer to the output message
/// \param pSegment_  the segment to push onto the list
/// \param byteCount_ the byte count of the segment
inline void amps_protocol_output_message_push_back_segment_with_length(amps_protocol_output_message* pMessage_,
                                                                       amps_message_segment* pSegment_,
                                                                       size_t byteCount_)
{
  pSegment_->byte_count = byteCount_;
  pSegment_->byte_offset = 0;
  amps_message_segment_list_push_back_segment(pMessage_->segment_list, pSegment_);
  pMessage_->byte_count += byteCount_;
}

/// AMPS convenience function to push a segment onto the segment list
/// of an output message with the specified length and offset (these
/// values override the length and offset set on the specified
/// segment).
///
/// \param pMessage_  a pointer to the output message
/// \param pSegment_  the segment to push onto the list
/// \param byteCount_ the byte count of the segment
inline void amps_protocol_output_message_push_back_segment_with_offset(amps_protocol_output_message* pMessage_,
                                                                       amps_message_segment* pSegment_,
                                                                       size_t offset_,
                                                                       size_t byteCount_)
{
  pSegment_->byte_count = byteCount_;
  pSegment_->byte_offset = offset_;
  amps_message_segment_list_push_back_segment(pMessage_->segment_list, pSegment_);
  pMessage_->byte_count += byteCount_;
}

/// AMPS convenience function to reserve a segment large enough to hold the serialized sow data
///
/// \param pSowData_         pointer to the sow data to be reserved
/// \param messageByteCount_ the desired size to hold the serialized sow data
inline void amps_output_message_sow_data_reserve(amps_output_message_sow_data* pSowData_,
                                                 size_t messageByteCount_)
{
  if (pSowData_->segment.byte_count + messageByteCount_ < pSowData_->segment.protocol_buffer.allocated_byte_count)
  {
    return;
  }

  amps_message_segment* pCurrent = &(pSowData_->segment);
  const size_t newByteCount  = (2 * pCurrent->protocol_buffer.allocated_byte_count) + messageByteCount_;

  amps_message_buffer buffer;
  amps_message_buffer_allocate(&buffer, newByteCount);

  if (pCurrent->byte_count)
  {
    memcpy(const_cast<char*>(buffer.data), pCurrent->data, pCurrent->byte_count);
  }
  if (pCurrent->protocol_buffer.deallocate)
  {
    pCurrent->protocol_buffer.deallocate(pCurrent->protocol_buffer.buffer_handle);
  }

  pCurrent->protocol_buffer.buffer_handle = buffer.handle;
  pCurrent->protocol_buffer.deallocate = buffer.deallocate;
  pCurrent->protocol_buffer.allocated_byte_count = buffer.allocated_byte_count;
  pCurrent->protocol_buffer.flags = 0;
  pCurrent->data = buffer.data;
}

///
/// Sets a field on the input message to the specified string value
/// where the pointer must point into the input message data.
///
/// \param pMessage_  a pointer to input message
/// \param fieldId_   the field identifier of the desired header field
/// \param pData_     a pointer to the start of the string (required to point inside input message data)
/// \param byteCount_ the byte count of the string
///
extern "C"
void amps_set_input_header_field_string(amps_input_message* pMessage_,
                                        int fieldId_,
                                        const char* pData_,
                                        size_t byteCount_);

///
/// Copy the specified string into the header field.
///
/// \param pMessage_  a pointer to input message
/// \param fieldId_   the field identifier of the desired header field
/// \param pData_     a pointer to the start of the string
/// \param byteCount_ the byte count of the string
///
extern "C"
void amps_assign_input_header_field_string(amps_input_message* pMessage_,
                                           int fieldId_,
                                           const char* pData_,
                                           size_t byteCount_);

///
/// Sets a field on the input message to the specified integer value
///
/// \param pMessage_ a pointer to input message
/// \param fieldId_  the field identifier of the desired header field
/// \param uival_    the unsigned integer value
///
extern "C"
void amps_set_input_header_field_uint64(amps_input_message* pMessage_,
                                        int fieldId_,
                                        uint64_t uival_);

///
/// Sets a field on the input message to the specified integer value
///
/// \param pMessage_ a pointer to input message
/// \param fieldId_  the field identifier of the desired header field
/// \param bool_     the boolean value
///
extern "C"
void amps_set_input_header_field_bool(amps_input_message* pMessage_,
                                      int fieldId_,
                                      int bool_);

///
/// Indicates to AMPS where the body of input message_ begins and ends
/// (and indirectly where the headers end). The protocol plugin
/// parse_header function must call amps_set_input_message_data to
/// record where the message body begins and ends.
///
/// \param pMessage_  a pointer to input message
//  \param offset_    the offset in the input message data where the body data begins
//  \param byteCount_ the byte count of the body data
//
extern "C"
void amps_set_input_message_data(amps_input_message* pMessage_,
                                 size_t offset_,
                                 size_t byteCount_);

///
/// Default trace_message implementation that copies the message header
/// to the trace output buffer.
///
extern "C"
int amps_protocol_default_trace_message(amps_protocol_context, amps_trace_context*);

/// protocol context creation,destruction
typedef amps_protocol_context (*amps_protocol_create_context_function)(amps_module_options);
typedef int (*amps_protocol_destroy_context_function)(amps_protocol_context);

/// protocol client creation, destruction
typedef amps_protocol_client_handle (*amps_protocol_create_client_function)(amps_protocol_create_client_params*);
typedef int (*amps_protocol_destroy_client_function)(amps_protocol_client_handle);

// framing
typedef int (*amps_protocol_allocate_segment_function)(amps_message_buffer_allocate_function allocate_,
                                                       amps_message_segment* pSegment_, size_t size_);
typedef int (*amps_protocol_finalize_segment_function)(amps_message_segment*, size_t messageByteCount_);
typedef int (*amps_protocol_chunk_message_function)(amps_protocol_chunk* pMessage_);

// handshake and logon
typedef int (*amps_protocol_handshake_function)(amps_protocol_handshake*);
typedef int (*amps_protocol_logon_completion_function)(amps_protocol_logon_completion*);

// upgrade
typedef int (*amps_protocol_upgrade_function)(amps_protocol_upgrade*);

// header parsing
typedef int (*amps_protocol_parse_header_function)(amps_input_message*);

// required serialize functions
typedef int (*amps_protocol_compute_serialize_size_function)(amps_output_message_header*);
typedef int (*amps_protocol_serialize_begin_function)(amps_message_segment*, const amps_output_message_header*);
typedef int (*amps_protocol_serialize_end_function)(amps_message_segment*, const amps_output_message_header*);
typedef int (*amps_protocol_serialize_message_function)(amps_protocol_output_message*);

// optional serialize
typedef int (*amps_protocol_serialize_ack_function)(amps_protocol_output_message*);
typedef int (*amps_protocol_serialize_group_function)(amps_protocol_output_message*);
typedef int (*amps_protocol_serialize_publish_function)(amps_protocol_output_message*);
typedef int (*amps_protocol_serialize_oof_function)(amps_protocol_output_message*);
typedef int (*amps_protocol_serialize_sow_function)(amps_protocol_output_message*);
typedef int (*amps_protocol_serialize_sow_data_function)(amps_output_message_sow_data*);

// optional tracing
typedef int(*amps_protocol_trace_function)(amps_protocol_context, amps_trace_context*);

struct amps_protocol_impl
{
  amps_protocol_create_context_function          create_context;
  amps_protocol_destroy_context_function         destroy_context;
  amps_protocol_create_client_function           create_client;
  amps_protocol_destroy_client_function          destroy_client;
  amps_protocol_allocate_segment_function        allocate_segment;
  amps_protocol_finalize_segment_function        finalize_segment;
  amps_protocol_chunk_message_function           chunk_message;
  amps_protocol_handshake_function               handshake;
  amps_protocol_upgrade_function                 upgrade;
  amps_protocol_logon_completion_function        logon_completion;
  amps_protocol_parse_header_function            parse_header;
  amps_protocol_compute_serialize_size_function  compute_serialize_size;
  amps_protocol_serialize_begin_function         serialize_begin;
  amps_protocol_serialize_end_function           serialize_end;
  amps_protocol_serialize_message_function       serialize_message;
  amps_protocol_serialize_ack_function           serialize_ack;
  amps_protocol_serialize_group_function         serialize_group;
  amps_protocol_serialize_publish_function       serialize_publish;
  amps_protocol_serialize_oof_function           serialize_oof;
  amps_protocol_serialize_sow_function           serialize_sow;
  amps_protocol_serialize_sow_data_function      serialize_sow_data;
  amps_protocol_trace_function                   inbound_trace_message;
  amps_protocol_trace_function                   outbound_trace_message;
};

inline void amps_protocol_output_message_expand_segment(amps_protocol_output_message* pOutMessage_,
                                                        struct amps_message_segment* pSegment_,
                                                        size_t newByteCount_)
{
  if (newByteCount_ < pSegment_->protocol_buffer.allocated_byte_count)
  {
    return;
  }

  amps_message_segment copySegment;
  copySegment.byte_offset = pSegment_->byte_offset;
  copySegment.byte_count = pSegment_->byte_count;
  copySegment.data = pSegment_->data;
  copySegment.protocol_buffer.deallocate = pSegment_->protocol_buffer.deallocate;
  copySegment.protocol_buffer.buffer_handle = pSegment_->protocol_buffer.buffer_handle;
  amps_protocol_impl* pProtocolImpl = reinterpret_cast<amps_protocol_impl*>(pOutMessage_->header->protocol_handle);
  pProtocolImpl->allocate_segment(pOutMessage_->allocate, pSegment_, newByteCount_);

  if (copySegment.byte_count)
  {
    memcpy(pSegment_->data, copySegment.data, copySegment.byte_count);
  }

  pSegment_->byte_offset = copySegment.byte_offset;
  pSegment_->byte_count = copySegment.byte_count;

  if (copySegment.protocol_buffer.deallocate)
  {
    copySegment.protocol_buffer.deallocate(copySegment.protocol_buffer.buffer_handle);
  }
}

// The remainder of this file contains the functions that must be defined when
// implementing a custom protocol.
#if 0

extern "C"
{
//
// Functions that define an AMPS protocol plugin
//


///
/// Initialize the protocol plugin globally
///
/// \param options_   the options are passed in as key/value pairs, with the final pair 0, 0
/// \param logger_    a pointer to a logger function
/// \param allocator_ a pointer to an allocator
/// \return AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_module_init(amps_module_options options_,
                       amps_module_logger logger_,
                       amps_module_allocator allocator_);

///
/// The protocol plugin is being shut down and should free up resources
///
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_module_terminate(void);

///
/// Create a unique protocol instance
///
/// \param options_ options from config file are passed in as options
/// \returns a valid protocol context or NULL (AMPS will store, pass back but not use directly)
///
  amps_protocol_context amps_protocol_create_context_function(const amps_module_options options_);

///
/// Free all resources associated with the protocol context
///
/// \param context_ a valid context obtained from a create context call
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_destroy_context_function(amps_protocol_context context_);

///
/// Allocate a segment suitable for proper protocol framing
///
/// \param allocate_  a message buffer allocation function
/// \param pSegment_  a pointer to the segment that will contain the allocated segment
/// \param byteCount_ the byte count of the requested segment
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_allocate_segment_function(amps_message_buffer_allocate_function allocate_,
                                              amps_message_segment* pSegment_,
                                              size_t byteCount_);

///
/// Finalize the framing of a segment to complete a message
///
/// \param pSegment_    a pointer to the segment that will finalized
/// \param messageByteCount_ the size of the message
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_finalize_segment_function(amps_message_segment* pSegment_, size_t messageByteCount_);

///
/// Frame a message
///
/// \param pMessage_   a pointer to the chunk (contains input data pointer and length) to be retrieved if a complete message is availble
/// \returns
/// AMPS_PROTOCOL_COMPLETE MESSAGE   there is enough data to process a message, returned in pMessage_
/// AMPS_PROTOCOL_INCOMPLETE_MESSAGE not enough data for a message
/// AMPS_PROTOCOL_ERROR              protocol error, should cause disconnect
///
  int amps_protocol_chunk_message_function(amps_protocol_chunk* pMessage_);

///
/// Execute a handshake
///
/// \param pHandshake_  a pointer to handshake data structure including input and response fields
/// \returns
/// AMPS_PROTOCOL_COMPLETE MESSAGE   there is enough data to process the handshake
/// AMPS_PROTOCOL_INCOMPLETE_MESSAGE not enough data for a message
/// AMPS_PROTOCOL_ERROR              protocol error, should cause disconnect
///
/// pHandshake_->consumed_byte_count should indicate the number of bytes consumed for the handshake
  int amps_protocol_handshake_function(amps_protocol_handshake* pHandshake_);

///
/// Execute a logon completion
///
/// \param pLogonCompletion_  a pointer to logon completion data structure including input and response fields
/// \returns
/// AMPS_PROTOCOL_COMPLETE MESSAGE   there is enough data to complete the logon
/// AMPS_PROTOCOL_ERROR              protocol error, should cause disconnect
///
  int amps_protocol_logon_completion_function(amps_protocol_logon_completion* pLogonCompletion_);

///
/// compute the minimum required size (in bytes) to serialize the
/// message header (begin and end) and store in serialize_size member
/// of amps_output_message_header
///
/// \param pHeader_  pointer to the header fields for an output message
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_compute_serialize_size_function(amps_output_message_header* pHeader_);

///
/// begin the serialization of an output message header. will update
/// pSegment_->byte_count with the number of bytes written
///
/// \param pSegment_ pointer to the location where the header should be serialized (data + length)
/// \param pHeader_  pointer to the header fields for an output message
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_serialize_begin_function(amps_message_segment* pSegment_, const amps_output_message_header* pHeader_);

///
/// end the serialization of an output message header.  will update
/// pSegment_->byte_count with the number of bytes written
///
/// \param pSegment_ pointer to the location where the header end should be serialized (data + length)
/// \param pHeader_  pointer to the header fields for an output message
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_serialize_end_function(amps_message_segment* pSegment_, const amps_output_message_header* pHeader_);

///
/// generic serialize message function
///
/// \param pMessage_ pointer to an output message
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_serialize_message_function(amps_protocol_output_message* pMessage_);

///
/// parse the header of message
///
/// \param pMessage_  pointer to an input message
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_parse_header_function(amps_input_message* pMessage_);

///
/// serialze an ack for sending over transport
///
/// \param pMessage_ pointer to an output message
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_serialize_ack_function(amps_protocol_output_message* pMessage_);

///
/// serialze a group message for sending over transport
///
/// \param pMessage_  pointer to an output message
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_serialize_group_function(amps_protocol_output_message* pMessage_);

///
/// serialze a publish message for sending over transport
///
/// \param pMessage_ pointer to an output message
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_serialize_publish_function(amps_protocol_output_message* pMessage_);

///
/// serialze an oof message for sending over transport
///
/// \param pMessage_ pointer to an output message
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_serialize_oof_function(amps_protocol_output_message* pMessage_);

///
/// serialze a sow message for sending over transport
///
/// \param pMessage_ pointer to an output message
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_serialize_sow_function(amps_protocol_output_message* pMessage_);

///
/// serialze a sow data segment that will be aggregated into a sow message
///
/// \param pMessage_ pointer to an output message
/// \returns AMPS_SUCCESS or AMPS_FAILURE
///
  int amps_protocol_serialize_sow_data_function(amps_output_message_sow_data* pSowData_);

} // extern "C"

#endif // Functional spec only section

#endif //_AMPS_SDK_PROTOCOL_AMPS_PROTOCOL_H_


