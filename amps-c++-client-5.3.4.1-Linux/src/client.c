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

/* Module globals */
static void* g_waitingFunction = NULL;
static void* g_removeRouteFunction = NULL;
static void* g_copyRouteFunction = NULL;

AMPSDLL amps_handle amps_client_create(
  const amps_char*   clientName)
{
  amps_atfork_init();
  amps_client_t* client = (amps_client_t*)malloc(sizeof(amps_client_t));
  if (client)
  {
    memset(client, 0, sizeof(amps_client_t));
    client->transport = NULL;

    if (clientName)
    {
      size_t end = strlen(clientName) < sizeof(client->clientName)
                   ? strlen(clientName) : (sizeof(client->clientName) - 1);
      memcpy(client->clientName, clientName, end);
      client->clientName[end] = '\0';
    }
    return client;
  }
  else
  {
    return NULL;
  }
}

void
amps_client_record_transport_error(amps_client_t* me)
{
  if (me->transport)
    _AMPS_SNPRINTF(me->lastError, sizeof(me->lastError),
                   "%s", g_transports[me->transportType].getError(me->transport));
  me->lastError[sizeof(me->lastError) - 1] = '\0';
}

void
amps_client_record_error(amps_client_t* me, const amps_char* error)
{
  _AMPS_SNPRINTF(me->lastError, sizeof(me->lastError),
                 "%s", error);
  me->lastError[sizeof(me->lastError) - 1] = '\0';
}

AMPSDLL amps_result amps_client_set_name(
  amps_handle handle,
  const amps_char* clientName)
{
  amps_client_t* me = (amps_client_t*)handle;
  /* You are only allowed to set the name on a client ONCE, either at
   * `construction', or via one call to amps_client_set_name. */
  if ( me->clientName[0] )
  {
    amps_client_record_error(me, "Change to a client's name is not permitted.");
    return AMPS_E_USAGE;
  }

  size_t end = strlen(clientName) < sizeof(me->clientName)
               ? strlen(clientName) : (sizeof(me->clientName) - 1);
  memcpy(me->clientName, clientName, end);
  me->clientName[end] = '\0';
  return AMPS_E_OK;
}



AMPSDLL void amps_client_disconnect(
  amps_handle handle)
{
  amps_client_t* me = (amps_client_t*)handle;
  if (me->transport)
  {
    g_transports[me->transportType].closeFunc(me->transport);
  }
}

AMPSDLL void amps_client_destroy(
  amps_handle handle)
{
  amps_client_t* me = (amps_client_t*)handle;
  amps_client_disconnect(handle);
  if (me->transport)
  {
    g_transports[me->transportType].destroyFunc(me->transport);
  }
  me->transport = NULL;
  free(me);
}

amps_result amps_client_internal_message_handler(
  amps_handle message,
  void* userData)
{
  amps_client_t* me = (amps_client_t*)userData;
  /* figure out if this is message is expected by somebody and let them know. */
  if (me->messageHandler)
  {
    me->messageHandler(message, me->messageHandlerUserData);
  }
  return AMPS_E_OK;
}

void amps_client_internal_predisconnect_handler(
  amps_handle transport,
  unsigned failedVersion,
  void* userData)
{
  amps_client_t* me = (amps_client_t*)userData;

  if (me->predisconnectHandler)
  {
    /* call the reconnector on the new guy */
    me->predisconnectHandler(me, failedVersion, me->predisconnectHandlerUserData);
  }
}

amps_result amps_client_internal_disconnect_handler(
  amps_handle transport,
  void* userData)
{
  /* INV: we have already locked the mutex on this client's transport. */
  amps_client_t* me = (amps_client_t*)userData;
  amps_result rc = AMPS_E_CONNECTION;

  if (me->disconnectHandler)
  {
    /* call the reconnector on the new guy */
    rc = me->disconnectHandler(me, me->disconnectHandlerUserData);
  }

  return rc;
}

AMPSDLL amps_result amps_client_connect(
  amps_handle handle,
  const amps_char*  uri)
{
  const amps_char* original_uri = uri;
  amps_client_t* client = (amps_client_t*)handle;
  amps_result result = AMPS_E_OK;
  size_t i;
  short newConnection = !(client->transport);

  if (!client->clientName[0])
  {
    amps_client_record_error(client, "A client name must be set before connecting.");
    return AMPS_E_USAGE;
  }

  /* just find the transport type, allocate the transport, and keep moving */
  for (i = 0; g_transports[i].createFunc; i++)
  {
    char* registeredTransport = g_transports[i].name;
    uri = original_uri;
    while (*(uri++) == *(registeredTransport++))
    {
      if (!*uri)
      {
        amps_client_record_error(client, "Invalid URI.");
        return AMPS_E_URI; /* bad uri */
      }
    }
    if (*(uri - 1) == ':' && !*(registeredTransport - 1))
    {
      /* call into the transport and make one! */
      if (newConnection)
      {
        client->transportType = i;
        client->transport = g_transports[i].createFunc();
      }
      else
      {
        if (client->transportType != i )
        {
          amps_client_record_error(client, "cannot reconnect using a different transport.");
          return AMPS_E_URI;
        }
      }
      /* setup the message and disconnect handler */
      g_transports[i].receiveFunc(client->transport, amps_client_internal_message_handler, client);
      g_transports[i].predisconnectFunc(client->transport, amps_client_internal_predisconnect_handler, client);
      g_transports[i].disconnectFunc(client->transport, amps_client_internal_disconnect_handler, client);
      /* add optional transport filter and callback for thread creation */
      if (client->transportFilterFunction)
      {
        g_transports[i].setTransportFilterFunc(client->transport,
                                               client->transportFilterFunction,
                                               client->transportFilterFunctionUserData);
      }
      if (client->threadCreatedCallback)
      {
        g_transports[i].setThreadCreatedCallback(client->transport,
                                                 client->threadCreatedCallback,
                                                 client->threadCreatedCallbackUserData);
      }
      /* now connect.  we give it the scheme in case multiple schemes are served by one fxn */
      result = g_transports[i].connectFunc(client->transport, original_uri);
      if (result != AMPS_E_OK)
      {
        amps_client_record_transport_error(client);
        if (newConnection)
        {
          g_transports[i].destroyFunc(client->transport);
          client->transport = NULL;
        }
      }
      return result;
    }
  }
  /* failed to find a transport */
  amps_client_record_error(client, "The URI specifies a transport that is unavailable.");
  return AMPS_E_TRANSPORT_TYPE;
}

AMPSDLL amps_result
amps_client_send(amps_handle client, amps_handle message)
{
  amps_client_t* me = (amps_client_t*)client;
  amps_result result = AMPS_E_RETRY;
  if (me->transport == NULL)
  {
    return AMPS_E_DISCONNECTED;
  }

  while (result == AMPS_E_RETRY)
  {
    result = g_transports[me->transportType].sendFunc(me->transport, message);
  }
  if (result != AMPS_E_OK)
  {
    amps_client_record_transport_error(me);
  }
  return result;
}

AMPSDLL amps_result
amps_client_send_with_version(amps_handle client, amps_handle message, unsigned* version_out)
{
  amps_client_t* me = (amps_client_t*)client;
  amps_result result = AMPS_E_RETRY;
  if (me->transport == NULL)
  {
    return AMPS_E_DISCONNECTED;
  }

  while (result == AMPS_E_RETRY)
  {
    result = g_transports[me->transportType].sendWithVersionFunc(me->transport, message, version_out);
  }
  if (result != AMPS_E_OK)
  {
    amps_client_record_transport_error(me);
  }
  return result;
}
AMPSDLL void amps_client_set_predisconnect_handler(amps_handle client,
                                                   amps_predisconnect_handler disconnectHandler,
                                                   void* userData)
{
  amps_client_t* me = (amps_client_t*)client;
  me->predisconnectHandler = disconnectHandler;
  me->predisconnectHandlerUserData = userData;
}

AMPSDLL void amps_client_set_disconnect_handler(amps_handle client,
                                                amps_handler disconnectHandler,
                                                void* userData)
{
  amps_client_t* me = (amps_client_t*)client;
  me->disconnectHandler = disconnectHandler;
  me->disconnectHandlerUserData = userData;
}

AMPSDLL size_t amps_client_get_error(
  amps_handle  handle,
  amps_char*   errorMessageOut,
  size_t       bufferSize)
{
  amps_client_t* me = (amps_client_t*)handle;
  int bytesWritten = _AMPS_SNPRINTF(errorMessageOut, bufferSize, "%s", me->lastError);
  errorMessageOut[bufferSize - 1] = '\0';
  return (size_t)bytesWritten;
}

AMPSDLL void amps_client_set_message_handler(amps_handle client, amps_handler messageHandler, void* userData)
{
  amps_client_t* me = (amps_client_t*)client;
  me->messageHandler = messageHandler;
  me->messageHandlerUserData = userData;
}

AMPSDLL amps_handle
amps_client_get_transport(amps_handle client)
{
  amps_client_t* me = (amps_client_t*)client;
  return me->transport;
}

AMPSDLL amps_result
amps_client_attempt_reconnect(amps_handle client, unsigned version)
{
  amps_client_t* me = (amps_client_t*)client;
  if (me->transport)
  {
    return g_transports[me->transportType].reconnectFunc(me->transport, version);
  }
  amps_client_record_error(me, "Client does not have a transport. Client must be connected before attempting reconnect.");
  return AMPS_E_DISCONNECTED;
}

AMPSDLL amps_result
amps_client_set_read_timeout(amps_handle client, int readTimeout)
{
  amps_client_t* me = (amps_client_t*)client;
  if (me->transport)
  {
    return g_transports[me->transportType].setReadTimeoutFunc(me->transport, readTimeout);
  }
  amps_client_record_error(me, "Client does not have a transport. Client must be connected before setting read timeout.");
  return AMPS_E_DISCONNECTED;
}

AMPSDLL amps_result
amps_client_set_idle_time(amps_handle client, int idleTime)
{
  amps_client_t* me = (amps_client_t*)client;
  if (me->transport)
  {
    return g_transports[me->transportType].setIdleTimeFunc(me->transport, idleTime);
  }
  amps_client_record_error(me, "Client does not have a transport. Client must be connected before setting idle time.");
  return AMPS_E_DISCONNECTED;
}

AMPSDLL amps_result amps_client_set_transport_filter_function(
  amps_handle client,
  amps_transport_filter_function filter,
  void* userData)
{
  amps_client_t* me = (amps_client_t*)client;
  me->transportFilterFunction = filter;
  me->transportFilterFunctionUserData = userData;
  if (me->transport) // -V1051
  {
    g_transports[me->transportType].setTransportFilterFunc(me->transport, filter, userData);
  }
  return AMPS_E_OK;
}

AMPSDLL amps_result amps_client_set_thread_created_callback(
  amps_handle client,
  amps_thread_created_callback callback,
  void* userData)
{
  amps_client_t* me = (amps_client_t*)client;
  me->threadCreatedCallback = callback;
  me->threadCreatedCallbackUserData = userData;
  if (me->transport)
  {
    g_transports[me->transportType].setThreadCreatedCallback(me->transport, callback, userData);
  }
  return AMPS_E_OK;
}

AMPSDLL AMPS_SOCKET amps_client_get_socket(amps_handle client)
{
  amps_client_t* me = (amps_client_t*)client;
  if (me->transport)
  {
    return g_transports[me->transportType].getSocket(me->transport);
  }
  return AMPS_INVALID_SOCKET;
}

#ifndef _WIN32
amps_uint64_t amps_now(void)
{
  struct timespec ts = {0, 0};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  amps_uint64_t retVal = (amps_uint64_t)(ts.tv_sec * 1000 + (ts.tv_nsec / (1000 * 1000)));
  return retVal;
}
#else
AMPSDLL amps_uint64_t amps_now(void)
{
  static amps_uint64_t s_frequency = 0;
  amps_uint64_t now = 0;
  if (!s_frequency)
  {
    QueryPerformanceFrequency((LARGE_INTEGER*)&s_frequency);
  }
  QueryPerformanceCounter((LARGE_INTEGER*)&now);
  now /= (s_frequency / 1000);
  return now;
}
#endif

AMPSDLL void amps_noOpFn(void* unused) {;}

AMPSDLL void amps_set_waiting_function(void* waitingFunction_)
{
  g_waitingFunction = waitingFunction_;
}

AMPSDLL void amps_invoke_waiting_function(void)
{
  if (g_waitingFunction)
  {
    ((void(*)(void))g_waitingFunction)();
  }
}

AMPSDLL void amps_set_remove_route_function(void* removeRouteFunction_)
{
  g_removeRouteFunction = removeRouteFunction_;
}

AMPSDLL void amps_invoke_remove_route_function(void* vpData_)
{
  if (g_removeRouteFunction)
  {
    ((void(*)(void*))g_removeRouteFunction)(vpData_);
  }
}

AMPSDLL void amps_set_copy_route_function(void* copyRouteFunction_)
{
  g_copyRouteFunction = copyRouteFunction_;
}

AMPSDLL void* amps_invoke_copy_route_function(void* vpData_)
{
  if (g_copyRouteFunction)
  {
    return ((void* (*)(void*))g_copyRouteFunction)(vpData_);
  }
  return NULL;
}

char* _amps_strndup(const char* ptr_,
                    size_t len_)
{
  char* result = NULL;
  if (ptr_ && len_)
  {
    result = (char*)malloc(len_ + 1);
    if (result)
    {
      memcpy(result, ptr_, len_);
      result[len_] = '\0';
    }
  }
  return result;
}
