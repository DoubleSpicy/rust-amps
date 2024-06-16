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

#ifndef _AMPS_SDK_TRANSPORT_AMPS_TRANSPORT_H
#define _AMPS_SDK_TRANSPORT_AMPS_TRANSPORT_H

#include <amps_common.h>
#include <protocol/amps_protocol.h>
#include <sys/types.h>
#include <stdint.h>

#define AMPS_TRANSPORT_API_VERSION 1

#define AMPS_TRANSPORT_NORMAL_CLIENT      0
#define AMPS_TRANSPORT_REPLICATION_CLIENT 1

#define AMPS_TRANSPORT_INBOUND_CLIENT  0
#define AMPS_TRANSPORT_OUTBOUND_CLIENT 1

#define AMPS_TRANSPORT_IP_PROTOCOL_V4 3
#define AMPS_TRANSPORT_IP_PROTOCOL_V6 4

typedef uint64_t amps_compression_type;

#define AMPS_COMPRESSION_TYPE_NONE 0UL
#define AMPS_COMPRESSION_TYPE_ZLIB 1UL
#define AMPS_COMPRESSION_TYPE_ZSTD 2UL

struct amps_listen_context
{
  void* context;
};

// A transport type
typedef void* amps_transport;

struct amps_transport_recv_messages
{
  amps_listen_context       listen_context;     // the listen context passed in the listen call
  amps_transport_client_id  client_id;          // the client id for the message
  uint64_t                  received_ticks;     // the ticks when the message is received over the transport
  size_t                    segment_count;      // if non-zero then indicates that message_count is 1 and message is a gather list for a single message
  size_t                    segments_byte_count;// if segment_count is non-zero then indicates the byte count for all segments in the message
  size_t                    message_count;      // the number of messages
  amps_protocol_message*    messages;           // an array of messages
  uint64_t                  txid_requested;     // set to true by transport to indicate that it wants the txid of the last message
  uint64_t                  txid;               // set by AMPS to a monotonically increasing value representing the txid of the last message, e.g messages[message_count-1]
  uint64_t                  last_ackable_txid;  // the last txid that can acked
};

// Callback signature for message handler.
// Transport should invoke message handler when new messages arrive.
// Returns AMPS_SUCCESS or AMPS_FAILURE
typedef int (*amps_transport_message_handler_function)(amps_transport_recv_messages*);

// Transport must call either the message handler or the ping function
// with no greater than a 5 second gap, i.e. if a 5 second gap occurs
// between message handler or ping being called then AMPS will report
// the transport as "stuck". The idle function can be called when the
// transport knows that it will be idle (waiting on poll etc) and the
// 5 second rule will not be enforced. However, it is recommended that
// the transport not be idle for more than 5 seconds.
//
// Returns the last ackable txi which can be used by the transport to
// ack outstanding messages.

typedef uint64_t (*amps_transport_ping_function)(amps_listen_context);
typedef uint64_t (*amps_transport_idle_function)(amps_listen_context);

// Logon a client after the protocol handshake which allows the
// protocol so supply additional options to the previously created
// client

struct amps_transport_logon_client_params
{
  amps_transport_context        transport_context;         // the transport context that was used to create the client
  amps_listen_context           listen_context;            // the listen thread context used to process messages for this client
  amps_transport_client_handle  client_handle;             // the transport assigned identifier set by the transport that it uses to identify the client
  amps_transport_client_id      client_id;                 // the AMPS assigned client_id that it uses to identidy a client
  amps_protocol_client_handle   protocol_client_handle;    // unique identifier set by AMPS to identify a protocol client
  amps_protocol_options*        protocol_options;
  amps_protocol_options         protocol_logon_completion_options; // set by transport logon function to be passed to the protocol logon completion function
};

struct amps_transport_client_stat
{
  uint64_t generation_count;
  uint64_t rx_queue;
  uint64_t tx_queue;
  uint64_t tcp_zero_window_advert;
};

typedef int (*amps_transport_get_client_stat_function)(amps_transport_context, amps_transport_client_handle, amps_transport_client_stat*);
typedef int (*amps_transport_update_client_stats_function)(amps_transport_context, size_t generationCount_);

struct amps_transport_listen_params;

typedef int (*amps_transport_logon_client_function)(amps_transport_logon_client_params*);
typedef int (*amps_transport_listen_ready_function)(amps_transport_listen_params*);

struct amps_transport_listen_params
{
  amps_transport_context                    transport_context;
  amps_protocol_context                     protocol_context;
  amps_protocol_impl_handle                 protocol_handle;
  amps_listen_context                       listen_context;
  amps_transport_message_handler_function   message_handler;
  amps_protocol_chunk_message_function      protocol_chunk_message;
  amps_protocol_handshake_function          protocol_handshake;
  amps_protocol_upgrade_function            protocol_upgrade;
  amps_protocol_logon_completion_function   protocol_logon_completion;
  amps_transport_logon_client_function      logon;
  amps_transport_listen_ready_function      listen_ready;
  amps_transport_ping_function              ping;
  amps_transport_idle_function              idle;
};

typedef void* (*amps_transport_client_name_allocate_function)(size_t);

// Create a new client for the given transport context. The name
// assigned by AMPS is returned in the amps_transport_client_name
// parameter.

// AMPS_TRANSPORT_INBOUND_CLIENT clients will have their "client name"
// set by AMPS. For AMPS_TRANSPORT_OUTBOUND_CLIENT connections, the
// client name will be set in the connect client params and passed to
// the create function when a connection is established.

struct amps_transport_create_client_params
{
  amps_transport_context                       transport_context;         // the transport context returned in create_context
  amps_transport_client_handle                 client_handle;             // unique identifier passed to AMPS by transport to identify a transport client
  amps_protocol_client_handle                  protocol_client_handle;    // unique identifier set by AMPS to identify a protocol client
  const char*                                  client_description;        // description of the client, i.e. "peername -> localname"
  size_t                                       client_description_length; // length of the description
  amps_transport_client_name_allocate_function client_name_allocate;      // function that AMPS can use to allocate space for client name
  char*                                        client_name;               // the client name as set by AMPS using allocator or set by connect caller (see comment above)
  size_t                                       client_name_length;        // the length the client name
  uint64_t                                     direction;                 // AMPS_TRANSPORT_INBOUND_CLIENT or AMPS_TRANSPORT_OUTBOUND_CLIENT
  amps_protocol_response                       connect_preamble;          // can be set by AMPS via protocol to indicate that upon connect a string should be sent to client
  const char*                                  transport_address;         // client's transport address, i.e. "127.0.0.1:9008"
  size_t                                       transport_address_length;  //
  const char*                                  transport_address_host;    // host of the transport address
  size_t                                       transport_address_host_length;
  const char*                                  transport_address_port;    // port of the transport address
  size_t                                       transport_address_port_length;
  const char*                                  client_local_name;         // local name of the client
  size_t                                       client_local_name_length;  //
  const char*                                  client_peer_name;          // peer name of the client
  size_t                                       client_peer_name_length;   //
  bool                                         is_ssl_enabled;            // whether or not the transport is using SSL/TLS
};

typedef amps_transport_client_id (*amps_transport_create_client_function)(amps_transport_create_client_params*);

// When AMPS logon occurs, the client name will be set and to ensure
// that logging is consistent, AMPS will call into the transport
// module to convey the logon name.
struct amps_transport_client_logon_name_params
{
  amps_transport_context       transport_context;        // the transport context that was used to create the client
  amps_transport_client_handle client_handle;            // the transport assigned identifier set by the transport that it uses to identify the client
  const char*                  client_logon_name;        // the logon name as set by AMPS
  size_t                       client_logon_name_length; // the length the logon name
};

typedef int (*amps_transport_set_client_logon_name_function)(const amps_transport_client_logon_name_params*);

// Finalize a client to stop further use based on client id.
// Client is not guaranteed to be immediately destroyed.
// This should ONLY be called from the listen function (send should fail)
// Return AMPS_SUCCESS or AMPS_FAILURE
typedef int (*amps_transport_finalize_client_function)(amps_transport_client_id);

// Callback signature for notifying AMPS that more data can be sent
// Returns AMPS_SUCCESS or AMPS_FAILURE
typedef int (*amps_transport_send_is_ready_function)(amps_transport_client_id);
typedef int (*amps_transport_update_client_stat_function)(amps_transport_client_id, const amps_transport_client_stat*);

typedef void* (*amps_transport_description_allocate_function)(size_t);

struct amps_transport_create_context_params
{
  uint64_t                                      api_version;
  amps_module_options                           module_options;
  amps_transport_create_client_function         create_client;
  amps_transport_finalize_client_function       finalize_client;
  amps_transport_send_is_ready_function         send_is_ready;        // call into AMPS when send is ready after requested notification
  amps_transport_update_client_stat_function    update_client_stat;   // call into AMPS with client stats after update of stats requested
  char*                                         description;          // description of the transport
  size_t                                        description_length;   // length of the description
  amps_transport_description_allocate_function  description_allocate; // function that transport can use to allocate space for description
  int                                           is_secure;            // set to true if secure connections are desired
};

typedef amps_transport_context (*amps_transport_create_context_function)(amps_transport_create_context_params*);

struct amps_transport_connect_client_params
{
  amps_transport_context        transport_context;      // the transport context to connect and call create client
  amps_transport_client_handle  client_handle;          // filled in if connect is successful
  const char*                   uri;                    // the transport should interpret the URI specific to how it connects
  uint64_t                      uri_length;             // the length of the uri
  const char*                   client_name;            // the name of client which will be handed to AMPS in client create call
  uint64_t                      client_name_length;     // the length of the client name
  uint64_t                      timeout;                // the max amount of time to wait for connection establishment
  uint64_t                      client_type;            // AMPS_TRANSPORT_REPLICATION_CLIENT or AMPS_TRANSPORT_NORMAL_CLIENT
  amps_compression_type         compression_type;       // set to zlib or zstd to indicate compression is desired
  int                           ip_proto_prefer;        // Prefer IPv4 orIPv6 when resolving the DNS for the uri
  int                           error;                  // if connect fails, then errno will be set if error or 0 for timeout
  amps_inet_address_handle      inet_addr_handle;       // filled in with the resolved inet address or any DNS errors.
};

typedef int (*amps_transport_destroy_context_function)(amps_transport_context);

typedef int (*amps_transport_start_context_function)(amps_transport_context);

typedef int (*amps_transport_stop_context_function)(amps_transport_context);

typedef int (*amps_transport_shutdown_context_function)(amps_transport_context);

typedef int (*amps_transport_listen_function)(amps_transport_listen_params*);

typedef int (*amps_transport_connect_client_function)(amps_transport_connect_client_params*);

typedef int (*amps_transport_enable_client_function)(amps_transport_context,
                                                     amps_transport_client_handle);

typedef int (*amps_transport_disable_client_function)(amps_transport_context,
                                                      amps_transport_client_handle,
                                                      amps_command_type);

typedef int (*amps_transport_disconnect_client_function)(amps_transport_context,
                                                         amps_transport_client_handle);

typedef int (*amps_transport_destroy_client_function)(amps_transport_context,
                                                      amps_transport_client_handle);

typedef int (*amps_transport_reap_client_function)(amps_transport_context,
                                                   amps_transport_client_handle);

typedef int (*amps_transport_request_client_send_is_ready_function)(amps_transport_context,
                                                                    amps_transport_client_handle);

struct amps_transport_send_message_info
{
  size_t  length;
  size_t  byte_count;
};

struct amps_transport_send_messages
{
  uint64_t                          received_ticks;   // the ticks when the first message in this batch was received
  uint64_t                          processing_ticks; // the ticks when the first message in this batch started processing
  uint64_t                          processed_ticks;  // the ticks when the first message in this batch was processed
  uint64_t                          queued_ticks;     // the ticks when the first message in this batch is picked up off the message pipeline
  uint64_t                          published_ticks;  // the ticks when the first message in this batch was published
  size_t                            message_count;    // the number of messages and the number of 'message_info' entries
  size_t                            data_count;       // the number of 'data' entries
  size_t                            byte_count;       // the total bytes for all messages
  amps_byte_array*                  data;             // the raw segments of data for all messages
  amps_transport_send_message_info* message_info;     // the length and bytecount for each message
};

struct amps_transport_send_client_bytes_request
{
  amps_transport_context        transport_context;     // the transport context
  amps_transport_client_handle  client_handle;         // the transport client handle
  amps_transport_send_messages* messages;              // the messages to send
  size_t                        requested_byte_count;  // the number of bytes to send (same as messages->byte_count)
  size_t                        consumed_byte_count;   // set by transport to indicate number of bytes consumed by transport
  size_t                        deflated_byte_count;   // set by transport to indicate the deflated byte count that represents the consumed byte count
  size_t                        queued_byte_count;     // set by transport to indicate that is has queued bytes to send
  uint64_t                      send_ticks;            // set by transport to indicate the 'ticks' when data was sent
  // 'request_send_is_ready' is set by transport to indciate that
  // client that is desirable if the client request 'send is ready'
  // instead of calling send again and only call send again after
  // transport has indicated that send is ready.
  int                           request_send_is_ready;

  // 'is_deflate' is set by transport to indicate that the transport
  // is using compression. if is_flush_requested is set then AMPS is
  // required to call the send function even when there is no data to
  // send.
  int                           is_deflate;            // set by transport to indicate the deflation is active
  int                           is_flush_requested;    // set by transport to indicate that a flush is requested
  int                           is_connected;          // set by transport to indicate state of the connection
};

typedef int (*amps_transport_send_client_bytes_function)(amps_transport_send_client_bytes_request*);

struct amps_transport_impl
{
  amps_transport_create_context_function               create_context;
  amps_transport_destroy_context_function              destroy_context;
  amps_transport_start_context_function                start_context;
  amps_transport_stop_context_function                 stop_context;
  amps_transport_shutdown_context_function             shutdown_context;
  amps_transport_listen_function                       listen;
  amps_transport_connect_client_function               connect_client;
  amps_transport_enable_client_function                enable_client;
  amps_transport_disable_client_function               disable_client;
  amps_transport_disconnect_client_function            disconnect_client;
  amps_transport_destroy_client_function               destroy_client;
  amps_transport_send_client_bytes_function            send_client_bytes;
  amps_transport_request_client_send_is_ready_function request_client_send_is_ready;
  amps_transport_get_client_stat_function              get_client_stat;
  amps_transport_update_client_stats_function          update_client_stats;
  amps_transport_set_client_logon_name_function        set_client_logon_name;
};

// The remainder of this file contains the functions that must be defined when
// implementing a custom transport.
#if 0

extern "C"
{
// Initialize the transport library globally
// The options are passed in as key/value pairs, with the final pair 0, 0
// Return AMPS_SUCCESS or AMPS_FAILURE
int amps_module_init(amps_module_options, amps_module_logger, amps_module_allocator);

// Transport is being shut down and should free up resources
// Return AMPS_SUCCESS or AMPS_FAILURE
int amps_module_terminate(void);

// Create a unique transport instance
// Options from config file are passed in as options
// The messageHandler is the one that should be invoked for new messages
// All new clients should be created by the clientCreator and
// finalized when done by the clientFinalizer.
// Returns AMPS_SUCCESS or AMPS_FAILURE
amps_transport_context amps_transport_create_context(amps_transport_create_context_params*);

// Free all resources associated with the transport context *context_
// Return AMPS_SUCCESS or AMPS_FAILURE
int amps_transport_destroy_context(amps_transport_context);

// Start the context listening for incoming messages
// Return AMPS_SUCCESS or AMPS_FAILURE

int amps_transport_listen(amps_transport_listen_params*);

// Notify the transport that it's OK to start accepting connections
//   and processing messages.  After this is called by AMPS, the
//   transport is responsible for invoking the registerActivity_
//   function every 5 seconds.
// Return AMPS_SUCCESS or AMPS_FAILURE
int amps_transport_start(amps_transport_context);

// Disconnect the context from the client
// Return AMPS_SUCCESS or AMPS_FAILURE
int amps_transport_disconnect_client(amps_transport_context, amps_transport_client_handle);

// Destroy the client
// Return AMPS_SUCCESS or AMPS_FAILURE
int amps_transport_destroy_client(amps_transport_context, amps_transport_client_handle);

// Shutdown the context gracefully
// Return AMPS_SUCCESS or AMPS_FAILURE
int amps_transport_start_context(amps_transport_context);

// Shutdown the context gracefully
// Return AMPS_SUCCESS or AMPS_FAILURE
int amps_transport_shutdown_context(amps_transport_context);

// Send the given clientId the sequence of messages
// Individual messages may be broken up into pieces inside of messages
// Set the number of bytes sent.
//
// If not all bytes are sent, the sendReady function should be called
// when the transport is ready to send more bytes.
// Should only return failure when the transport is lost.
// Should return success when any bytes are sent or when send just
// needs to be buffered.
//
// Return AMPS_SUCCESS or AMPS_FAILURE.

// Guarantees:
// 1. the data_count within 'messages' will always be less than or equal to 1024
// 2. only whole messages are specified in 'messages' so the api can be used for both stream and message oriented transports

int amps_transport_send_client_bytes(amps_transport_send_client_bytes_request*);

#endif // Functional spec only section

#endif // _AMPS_SDK_TRASPORT_AMPS_TRANSPORT_H
