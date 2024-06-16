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

/* TCP transport functions */
const amps_char* amps_tcp_get_last_error(
  amps_handle transport);
amps_handle amps_tcp_create(void);
amps_result amps_tcp_set_receiver(amps_handle transport, amps_handler receiver, void* userData);
amps_result amps_tcp_set_predisconnect(amps_handle transport, amps_predisconnect_handler receiver, void* userData);
amps_result amps_tcp_set_disconnect(amps_handle transport, amps_handler receiver, void* userData);
void amps_tcp_close(amps_handle transport);
void amps_tcp_destroy(amps_handle transport);
amps_result amps_tcp_send(amps_handle transport, amps_handle message);
amps_result amps_tcp_send_with_version(amps_handle transport, amps_handle message, unsigned* version_out);
amps_result amps_tcp_connect(amps_handle transport, const amps_char* address);
amps_result amps_tcp_attempt_reconnect(amps_handle transport, unsigned version);
amps_result amps_tcp_set_read_timeout(amps_handle transport, int timeout);
amps_result amps_tcp_set_idle_time(amps_handle transport, int millis);
void amps_tcp_set_filter_function(amps_handle transport, amps_transport_filter_function filter, void* userdata);
void amps_tcp_set_thread_created_callback(amps_handle, amps_thread_created_callback, void*);
AMPS_SOCKET amps_tcp_get_socket(amps_handle);

/* TCPS transport functions */
const amps_char* amps_tcps_get_last_error(
  amps_handle transport);
amps_handle amps_tcps_create(void);
amps_result amps_tcps_set_receiver(amps_handle transport, amps_handler receiver, void* userData);
amps_result amps_tcps_set_predisconnect(amps_handle transport, amps_predisconnect_handler receiver, void* userData);
amps_result amps_tcps_set_disconnect(amps_handle transport, amps_handler receiver, void* userData);
void amps_tcps_close(amps_handle transport);
void amps_tcps_destroy(amps_handle transport);
amps_result amps_tcps_send(amps_handle transport, amps_handle message);
amps_result amps_tcps_send_with_version(amps_handle transport, amps_handle message, unsigned* version_out);
amps_result amps_tcps_connect(amps_handle transport, const amps_char* address);
amps_result amps_tcps_attempt_reconnect(amps_handle transport, unsigned version);
amps_result amps_tcps_set_read_timeout(amps_handle transport, int timeout);
amps_result amps_tcps_set_idle_time(amps_handle transport, int millis);
void amps_tcps_set_filter_function(amps_handle transport, amps_transport_filter_function filter, void* userdata);
void amps_tcps_set_thread_created_callback(amps_handle, amps_thread_created_callback, void*);
AMPS_SOCKET amps_tcps_get_socket(amps_handle);

/* UNIX transport functions */
const amps_char* amps_unix_get_last_error(
  amps_handle transport);
amps_handle amps_unix_create(void);
amps_result amps_unix_set_receiver(amps_handle transport, amps_handler receiver, void* userData);
amps_result amps_unix_set_predisconnect(amps_handle transport, amps_predisconnect_handler receiver, void* userData);
amps_result amps_unix_set_disconnect(amps_handle transport, amps_handler receiver, void* userData);
void amps_unix_close(amps_handle transport);
void amps_unix_destroy(amps_handle transport);
amps_result amps_unix_send(amps_handle transport, amps_handle message);
amps_result amps_unix_send_with_version(amps_handle transport, amps_handle message, unsigned* version_out);
amps_result amps_unix_connect(amps_handle transport, const amps_char* address);
amps_result amps_unix_attempt_reconnect(amps_handle transport, unsigned version);
amps_result amps_unix_set_read_timeout(amps_handle transport, int timeout);
amps_result amps_unix_set_idle_time(amps_handle transport, int millis);
void amps_unix_set_filter_function(amps_handle transport, amps_transport_filter_function filter, void* userdata);
void amps_unix_set_thread_created_callback(amps_handle, amps_thread_created_callback, void*);
AMPS_SOCKET amps_unix_get_socket(amps_handle);

transport_entry_t g_transports[] =
{
  {
    "tcp", &amps_tcp_create, &amps_tcp_connect,
    &amps_tcp_set_predisconnect, &amps_tcp_set_disconnect,
    &amps_tcp_set_receiver, &amps_tcp_send, &amps_tcp_send_with_version,
    &amps_tcp_get_last_error, &amps_tcp_close, &amps_tcp_destroy,
    &amps_tcp_attempt_reconnect, &amps_tcp_set_read_timeout,
    &amps_tcp_set_idle_time, &amps_tcp_set_filter_function,
    &amps_tcp_set_thread_created_callback,
    &amps_tcp_get_socket
  },
  {
    "tcps", &amps_tcps_create, &amps_tcps_connect,
    &amps_tcps_set_predisconnect, &amps_tcps_set_disconnect,
    &amps_tcps_set_receiver, &amps_tcps_send, &amps_tcps_send_with_version,
    &amps_tcps_get_last_error, &amps_tcps_close, &amps_tcps_destroy,
    &amps_tcps_attempt_reconnect, &amps_tcps_set_read_timeout,
    &amps_tcps_set_idle_time, &amps_tcps_set_filter_function,
    &amps_tcps_set_thread_created_callback,
    &amps_tcps_get_socket
  },
#ifndef _WIN32
  {
    "unix", &amps_unix_create, &amps_unix_connect,
    &amps_unix_set_predisconnect, &amps_unix_set_disconnect,
    &amps_unix_set_receiver, &amps_unix_send, &amps_unix_send_with_version,
    &amps_unix_get_last_error, &amps_unix_close, &amps_unix_destroy,
    &amps_unix_attempt_reconnect, &amps_unix_set_read_timeout,
    &amps_unix_set_idle_time, &amps_unix_set_filter_function,
    &amps_unix_set_thread_created_callback,
    &amps_unix_get_socket
  },
#endif
  {
    "none", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL
  }
};

