/* ////////////////////////////////////////////////////////////////////////
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

#ifndef _AMPS_H_
#define _AMPS_H_

#ifdef _WIN32
  #include <WinSock2.h>
#else
  #include <netdb.h>
  #include <netinet/ip.h>
  #include <errno.h>
  #include <pthread.h>
  #include <unistd.h>
  #include <time.h>
#endif
#include <string.h>
#include <time.h>
#include <stdio.h>
#include "amps/amps_generated.h"

//Avoid compiler warnings on win
#ifdef INVALID_SOCKET
  #define AMPS_INVALID_SOCKET INVALID_SOCKET
#else
  #define AMPS_INVALID_SOCKET -1
#endif

#ifdef _WIN32
  #ifdef AMPS_SHARED
    #ifdef AMPS_BUILD
      #define AMPSDLL __declspec(dllexport)
    #else
      #define AMPSDLL __declspec(dllimport)
    #endif
  #else
    #define AMPSDLL
  #endif
  #define AMPS_USLEEP(x) Sleep((DWORD)(x)/(DWORD)1000);
  #define AMPS_YIELD() SwitchToThread()
  typedef HANDLE AMPS_THREAD_T;
  typedef SOCKET AMPS_SOCKET;
  typedef __int32            amps_int32_t;
  typedef unsigned __int32   amps_uint32_t;
  typedef __int64            amps_int64_t;
  typedef unsigned __int64   amps_uint64_t;
#else
  #define AMPSDLL
  #define AMPS_USLEEP(x) usleep((useconds_t)x)
  #define AMPS_YIELD() sched_yield()
  typedef pthread_t AMPS_THREAD_T;
  typedef int AMPS_SOCKET;
  typedef int32_t amps_int32_t;
  typedef uint32_t amps_uint32_t;
  typedef int64_t amps_int64_t;
  typedef uint64_t amps_uint64_t;
#endif

#if defined(_WIN32) || defined(__SSE_4_2__)
  #define AMPS_SSE_42 1
#endif

/**
 *  \mainpage AMPS C & C++ Client Reference
 *
 *  Welcome to the AMPS C & C++ client API reference.
 *
 *  The AMPS C++ client is a fully-featured client that provides a high
 *  performance, convenient interface for working with AMPS. The AMPS C
 *  client, which the C++ client is built upon, provides a low-level
 *  API for working with AMPS.
 *
 *  These pages provide a detailed API reference. This API reference
 *  is meant to be used with the C/C++ Developer Guide, the
 *  AMPS Command Reference, and the AMPS User Guide. Detailed information on
 *  how to use AMPS and the AMPS Client is provided in the guides and reference.
 *  The API documentation provides information on the implementation details
 *  (for example, the exact function signatures) and general information
 *  to help you quickly remember what a given function does (or what a
 *  given value represents).
 *
 *  The guide is included with the Client distribution, or you can
 *  visit the [C++ developer page](http://www.crankuptheamps.com/documentation/client-apis/cpp/)
 *  on the 60East web site for more details.
 *
 *  Using the C++ client is simple:
 *
 *  ```cpp
 *
 *    #include <iostream>
 *    #include <amps/ampsplusplus.hpp>
 *
 *     int main(void)
 *     {
 *       AMPS::Client amps("myapp"); // Use a unique name for an actual application
 *
 *       try
 *       {
 *
 *         amps.connect("tcp://localhost:9007/amps/json");
 *         amps.logon();
 *
 *         for (auto msg : amps.subscribe("orders",
 *                                        "/symbol LIKE 'ABCD'"))
 *         {
 *
 *            std::cout << msg.getData() << std::endl;
 *
 *         }
 *
 *       } catch (AMPSException &e) {
 *
 *         std::cerr << e.what() << std::endl;
 *
 *         return -1;
 *
 *       }
 *       return 0;
 *     }
 *
 *  ```
 *
 *  In this short sample, you can see the outline of a typical AMPS
 *  application. An application typically:
 *
 *  * Constructs a {@link AMPS::HAClient} or {@link AMPS::Client} object
 *  * Provides connection (and, if necessary, authentication) information to the Client
 *  * Connects and logs on to AMPS
 *
 *  A subscriber then typically:
 *
 *  * Issues one or more commands to AMPS (using the {@link AMPS::Command} object or the named convenience methods)
 *  * Responds to the results returned by AMPS (which are returned as instances of AMPS::Message)
 *
 *  A publisher then typically:
 *
 *  * Registers a callback to receive publish failures
 *  * Retrieves information from an external source
 *  * Formats that information into the message payload, and publishes the message to AMPS (using the {@link AMPS::Client.publish() } function or a {@link AMPS::Command})
 *  * In the event a publish fails, responds appropriately in the callback
 *
 *  Naturally, the outline above is extremely simplified, and ignores the
 *  options available for setting up a {@link AMPS::HAClient} or {@link AMPS::Client}, as well as the
 *  details of working with AMPS. The distribution includes a number of
 *  sample programs that provide simple implementations of common tasks,
 *  and the Developer Guide provides an overview of available options,
 *  common patterns and usage, as well as best practice advice for
 *  design and implementation of applications that use AMPS.
 *
 *  To help with learning, troubleshooting,  and debugging, 60East includes
 *  full source in the Client distribution, available from the
 *  [C++ developer page](http://www.crankuptheamps.com/documentation/client-apis/cpp/)
 */


/**
 *  @file amps.h
 *  @brief Core type and function declarations for the AMPS C client.
 */

#ifdef __cplusplus
extern "C" {
#endif

typedef char amps_char;

/**
 *
 *  Opaque handle type used to refer to objects in the AMPS api.
 *  Opaque handle type used to refer to objects in the AMPS api.
 *  The size of a pointer.
 */
typedef void* amps_handle;

/**
 *  Return values from amps_xxx functions
 */
typedef enum
{
  /**
   *  Success
   */
  AMPS_E_OK,
  /**
   *  A memory error occurred
   */
  AMPS_E_MEMORY,
  /**
   *  An error with a command occurred.
   */
  AMPS_E_COMMAND,
  /**
   *  A connection error occurred.
   */
  AMPS_E_CONNECTION,
  /**
   *  The specified topic was invalid.
   */
  AMPS_E_TOPIC,
  /**
   *  The specified filter was invalid.
   */
  AMPS_E_FILTER,
  /**
   *  The operation has not succeeded, but ought to be retried.
   */
  AMPS_E_RETRY,
  /**
   *  The client and server are disconnected.
   */
  AMPS_E_DISCONNECTED,
  /**
   *  The server could not be found, or it actively refused our connection.
   */
  AMPS_E_CONNECTION_REFUSED,
  /**
   *  A stream error has occurred.
   */
  AMPS_E_STREAM,
  /**
   *  The specified URI is invalid.
   */
  AMPS_E_URI,
  /**
   *  The specified transport type is invalid.
   */
  AMPS_E_TRANSPORT_TYPE,
  /**
   *  The usage of this function is invalid.
   */
  AMPS_E_USAGE,
  /**
   *  A Secure Sockets Layer (SSL) error occurred.
   */
  AMPS_E_SSL
} amps_result;

typedef amps_result(*amps_handler)(amps_handle, void*);
typedef void(*amps_predisconnect_handler)(amps_handle, unsigned, void*);


/**
 * Functions for creation of an AMPS client
 */

/**
 *   Creates and returns a handle to a new amps client object.
 *  \param clientName A null-terminated, unique name for this client, or NULL if one will be supplied later.
 *  \returns A handle to the newly-created client, or NULL if an error occurred.
 */
AMPSDLL amps_handle amps_client_create(
  const amps_char*   clientName);

/**
 *  Sets the name on an amps client object.  This may only
 *  be called for client that do not already have a name set
 *  via a previous call to this function, or amps_client_create.
 *  \param handle The client object on which to set the name.
 *  \param clientName A null-terminated, unique name for this client.
 *  \returns AMPS_E_OK if the name is successfully passed to the client,
 *    AMPS_E_USAGE if the name could not be set because this client
 *    already has a name.
 */
AMPSDLL amps_result amps_client_set_name(
  amps_handle handle,
  const amps_char*   clientName);

/**
 *  Connects to the AMPS server specified in uri.
 *  \param handle The handle to a client created with amps_client_create().
 *  \param uri An AMPS uri, e.g. <c>tcp://localhost:9004/nvfix</c>
 *  \returns An amps_result indicating the success or failure of this connection attempt.
 *    If not AMPS_E_OK, use amps_client_get_error() to find the text of the error message.
 */
AMPSDLL amps_result amps_client_connect(
  amps_handle handle,
  const amps_char*  uri);

/**
 *  Disconnects from the AMPS server, if connected
 *  \param handle The handle to a client created with amps_client_create().
 */
AMPSDLL void amps_client_disconnect(
  amps_handle handle);

/**
 *  Disconnects and destroys an AMPS client object.
 *  Destroys the AMPS client object, and frees all resources
 *  associated with the client and any underlying transport.
 *  \param handle the client object to destroy.
 */
AMPSDLL void amps_client_destroy(
  amps_handle handle);

/**
 *  Returns the last error set on this client.  The result is undefined
 *  if the last operation on the client was successful.
 *  \param client The client handle to retrieve the last error for.
 *  \param errorMessageOut A buffer to place the error message into, as a null-terminated string.
 *  \param bufferSize The size, in bytes, of the buffer passed in errorMessageOut.
 *  \returns The length of the message written to errorMessageOut.
 */
AMPSDLL size_t amps_client_get_error(
  amps_handle  client,
  amps_char*   errorMessageOut,
  size_t       bufferSize);

/**
 *  Sends a message to the AMPS server.
 *  \param client The client to sent the message on.
 *  \param message An AMPS message to send.
 *  \returns An amps_result indicating the success or failure of this send.
 *    If not AMPS_E_OK, use amps_client_get_error() to find the text of the error message.
 */
AMPSDLL amps_result amps_client_send(
  amps_handle client,
  amps_handle message);

/**
 *  Sends a message to the AMPS server.
 *  \param client The client to sent the message on.
 *  \param message An AMPS message to send.
 *  \param version_out The connection version used to send the message.
 *  \returns An amps_result indicating the success or failure of this send.
 *    If not AMPS_E_OK, use amps_client_get_error() to find the text of the error message.
 */
AMPSDLL amps_result amps_client_send_with_version(
  amps_handle client,
  amps_handle message,
  unsigned* version_out);


/**
 *  Sets the message handler function for this client
 *  \param client An AMPS client object.
 *  \param messageHandler A pointer to function of signature:
 *       <c>amps_result myfunction(amps_handle message, void* userData)</c>
 *    that is called when a message arrives.
 *  \param userData User-defined data to be passed to the handler function.
 */
AMPSDLL void amps_client_set_message_handler(
  amps_handle client,
  amps_handler messageHandler,
  void* userData);

/**
 *  Sets the predisconnect handler function to be called when a disconnect occurs
 *  \param client An AMPS client object on which to set this handler.
 *  \param predisconnectHandler A pointer to function of signature:
 *      <c>amps_result myfunction(amps_handle client, void* userData)</c>
 *     to be called when a disconnect occurs on client.
 *   \param userData User-defined data to be passed to the handler function
 */
AMPSDLL void amps_client_set_predisconnect_handler(
  amps_handle client,
  amps_predisconnect_handler predisconnectHandler,
  void* userData);

/**
 *  Sets the disconnect handler function to be called when a disconnect occurs
 *  \param client An AMPS client object on which to set this handler.
 *  \param disconnectHandler A pointer to function of signature:
 *      <c>amps_result myfunction(amps_handle client, void* userData)</c>
 *     to be called when a disconnect occurs on client.
 *   \param userData User-defined data to be passed to the handler function
 */
AMPSDLL void amps_client_set_disconnect_handler(
  amps_handle client,
  amps_handler disconnectHandler,
  void* userData);

/**
 *  Returns a handle to the transport set in client, or NULL if no transport
 *  is associated with this transport.
 *  \param client An AMPS client object from which to get the transport handle.
 *  \returns An amps_handle that represents the transport used by the client.
 */
AMPSDLL amps_handle amps_client_get_transport(
  amps_handle client);

/**
 *  Manually invokes the user-supplied disconnect handler for this client.
 *  \param client An AMPS client object on which to attempt reconnect.
 *  \param version The connection version that failed and is attempting reconnect.
 *  \returns An amps_result indicating the success or failure of reconnect.
 *    If not AMPS_E_OK, use amps_client_get_error() to find the text of the error message.
 */
AMPSDLL amps_result
amps_client_attempt_reconnect(amps_handle client, unsigned version);

/**
 *  Returns the socket from the underlying transport in client, or NULL if no
 *  transport is associated with this client.
 *  \param client An AMPS client object from which to get the transport handle.
 *  \returns The socket currently used for communication with the server.
 */
AMPSDLL AMPS_SOCKET
amps_client_get_socket(amps_handle client);

/**
 * Functions for creation and manipulation of AMPS messages
 */

/**
 *  Creates and returns a handle to a new AMPS message object for client.
 *  \param client The client for which a new message should be constructed.
 *  \returns An amps_handle that represents the new message, or NULL if an error occurs.
 */
AMPSDLL amps_handle amps_message_create(amps_handle client);

/**
 *  Creates and returns a handle to a new AMPS message object that is a deep copy of the message passed in.
 *  \param message The message to copy.
 *  \returns An amps_handle that represents the new message, or NULL if an error occurs.
 */
AMPSDLL amps_handle amps_message_copy(amps_handle message);

/**
 *  Destroys and frees the memory associated with an AMPS message object.
 *  \param message A handle to the message to destroy.
 */
AMPSDLL void amps_message_destroy(amps_handle message);

/**
 *  Clears all fields and data in a message.
 *  This restores the message to the same state it had at creation,
 *  but with much higher performance.
 *  \param message A handle to the message to clear.
 */
AMPSDLL void amps_message_reset(amps_handle message);

/**
 *  Retrieves the value of a header field in an AMPS message.
 *  \param message The AMPS message from which the value is retrieved.
 *  \param field The header field to retrieve the value of.
 *  \param value_ptr The address of an amps_char* pointer.  This pointer is
 *      modified by amps_message_get_field_value() to point at the data for
 *      this field.
 *  \param length_ptr The address of a size_t in which the length of the data
 *      is written.
 *
 *  \b Note: The returned data is not null-terminated, and is owned
 *  by the message.  You may wish to copy the data into your own memory for
 *  safe-keeping.
 */
AMPSDLL void amps_message_get_field_value(
  amps_handle message,
  FieldId field,
  const amps_char**  value_ptr,
  size_t* length_ptr);

/**
 *  Sets the value of a header field in an AMPS message.
 *  \param message The AMPS message to mutate
 *  \param field The header field to set.
 *  \param value The value to set.
 *  \param length The length (excluding any null-terminator) of the value
 */
AMPSDLL void amps_message_set_field_value(
  amps_handle message,
  FieldId field,
  const amps_char* value,
  size_t   length);

/**
 *  Assigns the value of a header field in an AMPS message, without copying the value.
 *  \param message The AMPS message to mutate
 *  \param field The header field to set.
 *  \param value The value to set.
 *  \param length The length (excluding any null-terminator) of the value
 */
AMPSDLL void amps_message_assign_field_value(
  amps_handle message,
  FieldId field,
  const amps_char* value,
  size_t   length);

/**
 *  Sets the value of a header field in an AMPS message from a
 *  null-terminated string.
 *  \param message The AMPS message to mutate.
 *  \param field The field to set.
 *  \param value The null-terminated string value to set.
 */
AMPSDLL void amps_message_set_field_value_nts(
  amps_handle message,
  FieldId field,
  const amps_char* value);


/**
 *  Sets the value of a header field in an AMPS message to a new,
 *  globally unique identifier("GUID")
 *  \param  message The AMPS message to mutate.
 *  \param  field The field to apply the new GUID to.
 */
AMPSDLL void amps_message_set_field_guid(
  amps_handle message,
  FieldId field);

/**
 *  Sets the data component of an AMPS message.
 *  \param  message The AMPS message to set the data of.
 *  \param  value The value to set the data with.
 *  \param  length The length of the value
 */
AMPSDLL void amps_message_set_data(
  amps_handle message,
  const amps_char* value,
  size_t   length);

/**
 *  Assigns the data component of an AMPS message, without copying the value.
 *  \param  message The AMPS message to set the data of.
 *  \param  value The value to set the data with.
 *  \param  length The length of the value
 */
AMPSDLL void amps_message_assign_data(
  amps_handle message,
  const amps_char* value,
  size_t   length);

/**
 *  Sets the data component of an AMPS message.
 *  \param  message The AMPS message to set the data of.
 *  \param  value The null-terminated string value to set the data with.
 */
AMPSDLL void amps_message_set_data_nts(
  amps_handle message,
  const amps_char* value);

/**
 *  Gets the data component of an AMPS message.
 *  \param message The AMPS message to get the data of.
 *  \param value_ptr A pointer to an amps_char* that is modifed to reflect
 *      the address of the data of this message.
 *  \param length_ptr A pointer to a size_t that will be modified to reflect
 *      the length of the data of this message.
 *
 *  \b Note: The returned data is not null-terminated, and is owned
 *  by the message.  You may wish to copy the data into your own memory for
 *  safe-keeping.
 */
AMPSDLL void amps_message_get_data(
  amps_handle message,
  amps_char** value_ptr,
  size_t*     length_ptr);

/**
 *  Gets the long integer value of a header field in an AMPS message.
 *  \param message The message to examine.
 *  \param field The field containing the long data.
 *  \returns The long value found in the field, or 0 if not present.
 */
AMPSDLL unsigned long amps_message_get_field_long(
  amps_handle message,
  FieldId field);

/**
 *  Gets the unsigned 64-bit int value of a header field in an AMPS message.
 *  \param message The message to examine.
 *  \param field The field containing the long data.
 *  \returns The uint64_t value found in the field, or 0 if not present.
 */
AMPSDLL amps_uint64_t amps_message_get_field_uint64(
  amps_handle message,
  FieldId field);

/**
 *  Sets a read timeout (seconds), in which if no message is received,
 *  the connection is presumed dead.  Useful when a specific publish rate
 *  is expected, OR when using server heartbeating via the `heartbeat' command.
 *  \param client The client on which to set the read timeout.
 *  \param readTimeout The timeout in seconds.
 *  \returns An amps_result indicating the success or failure of setting the read timeout.
 *    If not AMPS_E_OK, use amps_client_get_error() to find the text of the error message.
 */
AMPSDLL amps_result amps_client_set_read_timeout(
  amps_handle client,
  int readTimeout);


/**
 *  Sets an idle-time (milliseconds). If no message arrives in this time,
 *  the message handler is invoked with a NULL message handle, which allows the application
 *  to perform background processing on the message handler thread if it chooses.
 *  \param client The client on which to request idle time.
 *  \param idleTime The time in milliseconds.
 *  \returns An amps_result indicating the success or failure of setting the idle time.
 *    If not AMPS_E_OK, use amps_client_get_error() to find the text of the error message.
 */
AMPSDLL amps_result amps_client_set_idle_time(
  amps_handle client,
  int idleTime);

/**
 * Prototype for a user-supplied callback function for filtering data before it is sent and after it is received.
 * The callback takes four parameters:
 * data      An unsigned char* that points to the raw data bytes.
 * len       The length of the data.
 * direction 0 if data is outgoing/sent, 1 if data is incoming/received.
 * userdata  A user-defined void* supplied with the callback.
 */
typedef void(*amps_transport_filter_function)(const unsigned char*, size_t, short, void*);

/**
 * Sets a user-supplied callback function for filtering data before it is sent and after it is received.
 * The callback takes four parameters:
 * data      An unsigned char* that points to the raw data bytes.
 * len       The length of the data.
 * direction 0 if data is outgoing/sent, 1 if data is incoming/received.
 * userdata  A user-defined void* supplied with the callback.
 *  \param client The client on which to set the callback function.
 *  \param filter The filter function pointer.
 *  \param userData The user data that should be included in each call to the filter.
 *  \returns An amps_result indicating the success or failure of setting the filter.
 *    If not AMPS_E_OK, use amps_client_get_error() to find the text of the error message.
 */
AMPSDLL amps_result amps_client_set_transport_filter_function(
  amps_handle client,
  amps_transport_filter_function filter,
  void* userData);

/**
 * Prototype for a user-supplied callback function to allow thread attributes to be set when a new thread is created for a connection.
 * The callback takes two parameters:
 * thread    A native handle for the thread.
 * userdata  A user-defined void* supplied with the callback.
 */
typedef amps_result(*amps_thread_created_callback)(AMPS_THREAD_T, void*);

/**
 * Sets a user-supplied callback function to allow thread attributes to set when a new thread is created for a connection.
 * The callback takes two parameters:
 * thread    A native handle for the thread.
 * userdata  A user-defined void* supplied with the callback.
 *  \param client The client on which to set the callback function.
 *  \param callback The callback function to invoke.
 *  \param userData The user data that should be included in each call to the callback.
 *  \returns An amps_result indicating the success or failure of setting the callback.
 *    If not AMPS_E_OK, use amps_client_get_error() to find the text of the error message.
 */
AMPSDLL amps_result amps_client_set_thread_created_callback(
  amps_handle client,
  amps_thread_created_callback callback,
  void* userData);

/**
 *
 * TRANSPORT-SPECIFIC APIS
 *
 */
AMPSDLL AMPS_SOCKET
amps_tcp_get_socket(amps_handle transport);

AMPSDLL AMPS_SOCKET
amps_tcps_get_socket(amps_handle transport);

/**
 * Retrieves the SSL object from the underlying TCPS transport.
 * \param   transport The transport handle to retrieve the underling SSL object from.
 * \returns The OpenSSL SSL* object which may be used with the OpenSSL api.
 */
AMPSDLL void*
amps_tcps_get_SSL(amps_handle transport);

/*
 * Returns the current wall-clock time in milliseconds
 */
AMPSDLL amps_uint64_t
amps_now(void);


/**
 * Initializes SSL support in the AMPS Client.
 * This function may be called exactly once, before the first TCPS connection
 * is attempted, in order to provide the path to an OpenSSL DLL or
 * shared library to use.
 *
 * On Windows: this function must be called with either (a) a path to
 *   "ssleay32.dll", in which case the specified DLL will be loaded,
 *   or (b) the string "ssleay32.dll" alone, in which case the default
 *   system DLL search path will be used to locate ssleay32.dll. You
 *   may also choose to link ssleay32.dll into your application directly,
 *   in which case this function does not need to be invoked.
 *
 * On Linux: this function may be called with the path to a copy of
 *   libssl.so (for example, "/lib/libssl.so.1.0.0"), or with the filename
 *   to load, in which case LD_LIBRARY_PATH is searched for the specified
 *   filename.  You may also link or load libssl.sl into your application
 *   directly, in which case this function does not need to be invoked.
 *
 * \param dllPath_ The name or path to your OpenSSL dll.
 * \returns 0 if successful, -1 if an error occurred. Use amps_ssl_get_error
 *   to retrieve the error description.
 */
AMPSDLL int amps_ssl_init(const char* dllPath_);

/**
 * Configures OpenSSL to validate the server's certificate when connecting.
 * This parameter must be set before attempting to connect to AMPS.
 *
 * \param mode_ 1 = enable, 0 = disable. (default: 0)
 * \returns 0 if successful, -1 if an error occurred.
 */
AMPSDLL int amps_ssl_set_verify(int mode_);

/**
 * Configures OpenSSL to use the specified locations for locating CA certificates.
 *
 * \param caFile_ A null-terminated string to pass to OpenSSL as the default PEM file.
 * \param caPath_ A null-terminated string to pass to OpenSSL as the directory containing PEM files.
 * \returns 0 if successful, -1 if an error occurred.
 *
 * See OpenSSL's SSL_CTX_load_verify_locations for more information on OpenSSL's certificate
 * location options.
 */
AMPSDLL int amps_ssl_load_verify_locations(const char* caFile_, const char* caPath_);

/**
 * Frees OpenSSL context and shared library.
 * This function should be called to free up OpenSSL resources and context
 * allocated by amps_ssl_init().
 */
AMPSDLL void amps_ssl_free(void);

/**
 * Returns the description of the last error from calling amps_ssl_init().
 * \returns The formatted error string with information regarding why
 *   amps_ssl_init() failed.
 */
AMPSDLL const char* amps_ssl_get_error(void);

AMPSDLL void amps_noOpFn(void*);

/*
 * Internal API for use by the python client.
 */
AMPSDLL void amps_set_waiting_function(void*);
AMPSDLL void amps_invoke_waiting_function(void);
AMPSDLL void amps_set_remove_route_function(void*);
AMPSDLL void amps_invoke_remove_route_function(void*);
AMPSDLL void amps_set_copy_route_function(void*);
AMPSDLL void* amps_invoke_copy_route_function(void*);

#ifdef __cplusplus
}
#if !defined(_AMPS_BUILD_C_CLIENT) && !defined(_AMPS_SKIP_AMPSPLUSPLUS)
  #include <amps/ampsplusplus.hpp>
#endif
#endif

#endif
