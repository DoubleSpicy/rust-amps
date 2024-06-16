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
#ifndef _WIN32
#include <amps/amps_impl.h>
#include <sys/un.h>
#include <amps/ampsuri.h>

#define GET_ERRNO (errno)
#define SOCK_ERRORCODE(x) x
#include <pthread.h>

static pthread_mutexattr_t _mutexattr_recursive;
#define AMPS_INITLOCK(x) { \
    pthread_mutexattr_init(&_mutexattr_recursive); \
    pthread_mutexattr_settype(&_mutexattr_recursive, PTHREAD_MUTEX_RECURSIVE);\
    pthread_mutex_init(x, &_mutexattr_recursive); }
#define AMPS_LOCK(x) pthread_mutex_lock(x)
#define AMPS_SPIN_LOCK(x) amps_spin_lock_counted(x)
#define AMPS_SPIN_LOCK_UNLIMITED(x) amps_spin_lock_unlimited(x)

#define AMPS_UNLOCK(x) pthread_mutex_unlock(x)
#define AMPS_KILLLOCK(x) pthread_mutex_destroy(x)
#if __STDC_VERSION__ >= 201100
#include <stdatomic.h>
#define AMPS_IEX(ptr, value) atomic_exchange_explicit((ptr), (value), memory_order_acq_rel)
#define AMPS_IEX_GET(ptr, value) atomic_exchange_explicit((ptr), (value), memory_order_acq_rel)
#define AMPS_IEX_LONG(ptr, value) atomic_exchange_explicit((ptr), (value), memory_order_acq_rel)
#define AMPS_FETCH_ADD(ptr, value) atomic_fetch_add_explicit((ptr), (value), memory_order_acq_rel)
#define AMPS_FETCH_SUB(ptr, value) atomic_fetch_sub_explicit((ptr), (value), memory_order_acq_rel)
#define AMPS_JOIN(x) \
  tid = atomic_exchange((&x), (pthread_t)0); \
  if(tid!=(pthread_t)0 && tid!=pthread_self()) \
  { \
    pthread_join(tid, NULL); \
  }
#else
#define AMPS_IEX(ptr, value) (void)__sync_lock_test_and_set((ptr), (value))
#define AMPS_IEX_GET(ptr, value) __sync_lock_test_and_set((ptr), (value))
#define AMPS_IEX_LONG(ptr, value) (void)__sync_lock_test_and_set((ptr), (value))
#define AMPS_FETCH_ADD(ptr, value) __sync_fetch_and_add((ptr), (value))
#define AMPS_FETCH_SUB(ptr, value) __sync_fetch_and_sub((ptr), (value))

#define AMPS_JOIN(x) \
  tid = AMPS_FETCH_ADD(&x, (pthread_t)0); \
  if(tid!=(pthread_t)0 && tid!=pthread_self()) \
  { \
    if(__sync_bool_compare_and_swap(&x, tid, (pthread_t)0)) \
    { \
      pthread_join(tid, NULL); \
    } \
  }
#endif
#define AMPS_JOIN_DECLARE() \
  pthread_t tid = (pthread_t)0;

#define AMPS_SLEEP(x) { \
    static struct timespec ts = { x/1000, (x%1000) * 1000000 }; \
    nanosleep(&ts, NULL); }

typedef struct
{
  amps_thread_created_callback threadCreatedCallback;
  void* threadCreatedCallbackUserData;
  char* buf;
  amps_int64_t serializer;
  amps_handler messageHandler;
  void* messageHandlerUserData;
  amps_transport_filter_function filterFunction;
  void* filterUserData;

  amps_predisconnect_handler predisconnectHandler;
  void* predisconnectHandlerUserData;
  amps_handler disconnectHandler;
  void* disconnectHandlerUserData;
  amps_uint64_t readTimeoutMillis;
  amps_uint64_t idleTimeMillis;
#if __STDC_VERSION__ >= 201100
  atomic_int_fast64_t threadCreatedCallbackResult;
  _Atomic unsigned connectionVersion;
#else
  volatile amps_int64_t threadCreatedCallbackResult;
  volatile unsigned connectionVersion;
#endif

  size_t capacity;
#if __STDC_VERSION__ >= 201100
  _Atomic AMPS_SOCKET fd;
  atomic_bool disconnecting;
  atomic_bool destroying;
#else
  volatile AMPS_SOCKET fd;
  volatile int disconnecting;
  volatile int destroying;
#endif
  pthread_mutex_t lock;
  pthread_mutex_t sendLock;
#if __STDC_VERSION__ >= 201100
  _Atomic pthread_t thread;
#else
  volatile pthread_t thread;
#endif
  struct sockaddr_un sockaddr;
  char lastErrorBuf[1024];
}
amps_unix_t;


const amps_char* amps_unix_get_last_error(
  amps_handle transport)
{
  return ((amps_unix_t*)transport)->lastErrorBuf;
}

void amps_unix_set_error(
  amps_unix_t* transport,
  const amps_char* buffer)
{
  _AMPS_SNPRINTF(transport->lastErrorBuf, sizeof(transport->lastErrorBuf), "%s", buffer);
  transport->lastErrorBuf[sizeof(transport->lastErrorBuf) - 1] = '\0';
}

void amps_unix_set_socket_error(
  amps_unix_t* transport)
{
  int errorCode;
  errorCode = errno;
  amps_unix_set_error(transport, strerror(errorCode));
}

void
amps_unix_noop_filter_function(const unsigned char* data, size_t length, short direction, void* userdata)
{;}

void amps_unix_atfork_handler(amps_unix_t* transport_, int code_)
{
  switch (code_)
  {
  case 0:
    break;
  case 1:
    break;
  case 2:
    /* Reinitialize the transport lock in the forked child */
    AMPS_INITLOCK(&transport_->lock);
    break;
  }
}

amps_handle amps_unix_create(void)
{
  amps_unix_t* transport = (amps_unix_t*)malloc(sizeof(amps_unix_t));

  if (transport != NULL)
  {
    memset(transport, 0, sizeof(amps_unix_t));
    transport->fd = AMPS_INVALID_SOCKET;
    AMPS_INITLOCK(&transport->lock);
    AMPS_INITLOCK(&transport->sendLock);
    transport->filterFunction = &amps_unix_noop_filter_function;
    amps_atfork_add(transport, (_amps_atfork_callback_function)amps_unix_atfork_handler);
  }

  return transport;
}

void* amps_unix_threaded_reader(void* userData);

int
amps_unix_opt_parse(const char* val, size_t valLen, int* parsed)
{
  size_t i;
  if (valLen == 4 && memcmp(val, "true", 4) == 0)
  {
    *parsed = 1;
    return 1;
  }
  else if (valLen == 5 && memcmp(val, "false", 5) == 0)
  {
    *parsed = 0;
    return 1;
  }
  else
  {
    *parsed = 0;
    for (i = 0; i < valLen ; i++)
    {
      *parsed *= 10;
      if (val[i] >= '0' && val[i] <= '9')
      {
        *parsed += val[i] - '0';
      }
      else
      {
        return 0;
      }
    }
  }
  return 1;
}

amps_result
amps_unix_apply_socket_property(AMPS_SOCKET fd, const char* key, size_t keyLen, const
                                char* val, size_t valLen)
{
  int value = 0, level = SOL_SOCKET, optname;
  unsigned int optValueSize = sizeof(int);
  char* optValuePtr = (char*)&value;
  struct linger lingerStruct;
  if (!amps_unix_opt_parse(val, valLen, &value))
  {
    return AMPS_E_URI;
  }
  if (keyLen == strlen("unix_rcvbuf") && memcmp(key, "unix_rcvbuf", keyLen) == 0)
  {
    optname = SO_RCVBUF;
  }
  else if (keyLen == strlen("unix_keepalive") && memcmp(key, "unix_keepalive", keyLen) == 0 )
  {
    optname = SO_KEEPALIVE;
  }
  else if (keyLen == strlen("unix_sndbuf") && memcmp(key, "unix_sndbuf", keyLen) == 0)
  {
    optname = SO_SNDBUF;
  }
  else if (keyLen == strlen("unix_nodelay") && memcmp(key, "unix_nodelay", keyLen) == 0)
  {
    optname = TCP_NODELAY;
    level = IPPROTO_TCP;
  }
  else if (keyLen == strlen("unix_linger") && memcmp(key, "unix_linger", keyLen) == 0)
  {
    optname = SO_LINGER;
    lingerStruct.l_onoff = value != 0;
    lingerStruct.l_linger = (u_short)value;
    optValuePtr = (char*)&lingerStruct;
    optValueSize = sizeof(struct linger);
  }
  else if (keyLen == 6 && memcmp(key, "pretty", keyLen) == 0)
  {
    // handled in C++ layer
    return AMPS_E_OK;
  }
  else
  {
    return AMPS_E_URI;
  }

  /* set it */
  if (setsockopt(fd, level, optname, optValuePtr, optValueSize))
  {
    return AMPS_E_URI;
  }
  return AMPS_E_OK;
}

amps_result
amps_unix_parse_properties(amps_unix_t* me, const char* address,
                           size_t addressLength, amps_uri_state* uriState)
{
  const char* key = NULL;
  size_t keyLength = 0;
  /* First, enable tcp_keepalive by default */
  int value = 1;
  unsigned int optValueSize = sizeof(int);
  char* optValuePtr = (char*)&value;
  if (setsockopt(me->fd, SOL_SOCKET, SO_KEEPALIVE, optValuePtr, optValueSize))
  {
    return AMPS_E_URI;
  }
  /* Now, parse the rest of the URI */
  while (uriState->part_id < AMPS_URI_ERROR)
  {
    amps_uri_parse(address, addressLength, uriState);
    if (uriState->part_id == AMPS_URI_OPTION_KEY)
    {
      key = uriState->part;
      keyLength = uriState->part_length;
    }
    else if (uriState->part_id == AMPS_URI_OPTION_VALUE)
    {
      const char* value = uriState->part;
      size_t valueLength = uriState->part_length;
      if (keyLength == 4 &&
          (memcmp(key, "path", 4) == 0 || memcmp(key, "bind", 4) == 0))
      {
        /* Maximum path length defined in sys/un.h */
        const size_t max_path_len = 108;
        memcpy(me->sockaddr.sun_path, value,
               valueLength < max_path_len ? valueLength : max_path_len - 1);
        me->sockaddr.sun_family = AF_UNIX;
        me->sockaddr.sun_path[valueLength] = '\0';
      }
      else if (amps_unix_apply_socket_property(me->fd, key,
                                               keyLength, value, valueLength) != AMPS_E_OK)
      {
        return AMPS_E_URI;
      }
    }
  }
  if (uriState->part_id == AMPS_URI_ERROR)
  {
    return AMPS_E_URI;
  }
  return AMPS_E_OK;
}

amps_result amps_unix_connect(amps_handle transport, amps_char* address)
{
  AMPS_JOIN_DECLARE();
  amps_unix_t* me = (amps_unix_t*)transport;
  char protocol[256];
  amps_uri_state uriState;
  int rc;
  amps_result result = AMPS_E_OK;
  AMPS_SOCKET fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);

  AMPS_LOCK(&me->lock);
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me->lock);
  /* if we were previously marked as disconnecting, forget that. */
  AMPS_IEX_LONG(&me->disconnecting, 0);

  /* were we previously connected?  go ahead and close that. */
  if (fd != AMPS_INVALID_SOCKET)
  {
    /* this is a reconnect */
    shutdown(fd, SHUT_RDWR);
    closesocket(fd);
    fd = AMPS_INVALID_SOCKET;
  }
  AMPS_JOIN(me->thread);

  /* parse out the address */
  memset(&uriState, 0, sizeof(amps_uri_state));
  size_t address_length = strlen(address);
  while (uriState.part_id < AMPS_URI_PROTOCOL)
  {
    amps_uri_parse(address, address_length, &uriState);
  }
  if (uriState.part_id != AMPS_URI_PROTOCOL)
  {
    amps_unix_set_error(me, "URI format invalid");
    result = AMPS_E_URI; goto error;
  }
  memcpy(protocol, uriState.part, uriState.part_length);
  protocol[uriState.part_length] = '\0';
  me->serializer = amps_message_get_protocol(protocol);

  if (me->serializer == -1)
  {
    amps_unix_set_error(me, "The URI specified an unavailable protocol.");
    result = AMPS_E_URI; goto error;
  }
  me->fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (me->fd == AMPS_INVALID_SOCKET)
  {
    amps_unix_set_socket_error(me);
    result = AMPS_E_CONNECTION_REFUSED; goto error;
  }

  /* apply socket properties */
  if (amps_unix_parse_properties(me, address, address_length, &uriState) != AMPS_E_OK)
  {
    amps_unix_set_error(me, "The URI specified invalid properties.");
    result = AMPS_E_URI; goto error;
  }
#ifdef __APPLE__
  rc = 1; /* Set NOSIGPIPE to ON */
  if (setsockopt(me->fd, SOL_SOCKET, SO_NOSIGPIPE, &rc, sizeof(rc)) < 0)
  {
    fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
    amps_unix_set_error(me, "Failed to set no SIGPIPE.");
    result = AMPS_E_CONNECTION; goto error;
  }
#endif

  rc = connect(me->fd, (struct sockaddr*)&me->sockaddr,
               sizeof(struct sockaddr_un));
  if (rc == -1)
  {
    fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
    amps_unix_set_socket_error(me);
    result = AMPS_E_CONNECTION_REFUSED; goto error;
  }


  /* Increase the connection version now, so that anyone else attempting a reconnect knows
   * that they may not need to. */
  me->connectionVersion++;
  result = AMPS_E_OK;

  /*  and, start up a new reader thread */
  if (me->threadCreatedCallback)
  {
    AMPS_IEX(&me->threadCreatedCallbackResult, -1);
  }
  rc = pthread_create((pthread_t*)(&me->thread), NULL,
                      &amps_unix_threaded_reader, me);
  if (rc != 0)
  {
    fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
    amps_unix_set_error(me, "Failed to create thread for receive");
    result = AMPS_E_MEMORY; goto error;
  }
  for (int i = 0; i < AMPS_THREAD_START_TIMEOUT && me->threadCreatedCallbackResult == -1; ++i)
  {
    AMPS_SLEEP(4);
  }
  result = (amps_result)me->threadCreatedCallbackResult;
  if (me->threadCreatedCallbackResult == -1)
  {
    amps_unix_set_error(me, "Thread created callback failed to return in a timely manner or returned -1.");
    result = AMPS_E_MEMORY;
  }

error:
  if (result != AMPS_E_OK)
  {
    if (fd != AMPS_INVALID_SOCKET)
    {
      shutdown(fd, SHUT_RDWR);
      closesocket(fd);
    }
    me->thread = (pthread_t)0;
  }
  AMPS_UNLOCK(&me->lock);
  pthread_cleanup_pop(0);
  return result;
}

amps_result amps_unix_set_receiver(amps_handle transport, amps_handler receiver, void* userData)
{
  amps_unix_t* me = (amps_unix_t*)transport;
  me->messageHandlerUserData = userData;
  me->messageHandler = receiver;
  return AMPS_E_OK;
}

amps_result amps_unix_set_predisconnect(amps_handle transport,
                                        amps_predisconnect_handler receiver,
                                        void* userData)
{
  amps_unix_t* me = (amps_unix_t*)transport;
  me->predisconnectHandlerUserData = userData;
  me->predisconnectHandler = receiver;
  return AMPS_E_OK;
}

amps_result amps_unix_set_disconnect(amps_handle transport, amps_handler receiver, void* userData)
{
  amps_unix_t* me = (amps_unix_t*)transport;
  me->disconnectHandlerUserData = userData;
  me->disconnectHandler = receiver;
  return AMPS_E_OK;
}

void amps_unix_close(amps_handle transport)
{
  AMPS_JOIN_DECLARE();
  amps_unix_t* me = (amps_unix_t*)transport;
  AMPS_SOCKET fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
  AMPS_IEX_LONG(&me->disconnecting, 1);
  AMPS_SPIN_LOCK_UNLIMITED(&me->lock);
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me->lock);
  if (fd != AMPS_INVALID_SOCKET)
  {
    shutdown(fd, SHUT_RDWR);
    closesocket(fd);
  }
  AMPS_UNLOCK(&me->lock);
  pthread_cleanup_pop(0);
  AMPS_JOIN(me->thread);
}

void amps_unix_destroy(amps_handle transport)
{
  AMPS_JOIN_DECLARE();
  amps_unix_t* me = (amps_unix_t*)transport;
  AMPS_SOCKET fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
  amps_atfork_remove(me, (_amps_atfork_callback_function)amps_unix_atfork_handler);
  AMPS_LOCK(&me->lock);
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me->lock);
  AMPS_IEX_LONG(&me->destroying, 1);
  AMPS_IEX_LONG(&me->disconnecting, 1);
  if (fd != AMPS_INVALID_SOCKET)
  {
    shutdown(fd, SHUT_RDWR);
    closesocket(fd);
  }

  AMPS_UNLOCK(&me->lock);
  pthread_cleanup_pop(0);
  AMPS_JOIN(me->thread);
  AMPS_SLEEP(1);
  free(me->buf);
  /* Hopefully, nobody else is using me right now. */

  AMPS_KILLLOCK(&me->lock);
  AMPS_KILLLOCK(&me->sendLock);

  free(me);
}

/* called with me->lock not already taken */
amps_result amps_unix_handle_disconnect(
  amps_unix_t* me, unsigned failedConnectionVersion)
{
  int cancelState = 0;
  int unusedCancelState = 0;
  amps_result result = AMPS_E_OK;
  AMPS_SOCKET fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
  if (fd != AMPS_INVALID_SOCKET)
  {
    shutdown(fd, SHUT_RDWR);
    closesocket(fd);
  }

  /* first let waiters know that we've failed. */
  me->predisconnectHandler(me, failedConnectionVersion, me->predisconnectHandlerUserData);

  /* Now take the lock on self.
   * Use a spin, the only case where it might fail should be if a recv
   * thread ends up here while a send thread is already handling disconnect
   * and it is trying to join this thread in disconnect. */
  if (AMPS_SPIN_LOCK(&me->lock) == 0)
  {
    return AMPS_E_RETRY;
  }
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me->lock);

  /* if we're being destroyed, get out of here; don't worry about unlocking */
  if (me->destroying)
  {
    result = AMPS_E_DISCONNECTED; return result;
  }

  /* a new connection is available.  let someone else try. */
  if (failedConnectionVersion != me->connectionVersion)
  {
    result = AMPS_E_RETRY; goto error;
  }

  /* if we're disconnecting, get out of here; don't reconnect */
  if (me->disconnecting)
  {
    result = AMPS_E_DISCONNECTED; goto error;
  }

  pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
  /* Call the disconect handler. */
  result = me->disconnectHandler(me, me->disconnectHandlerUserData);
  pthread_setcancelstate(cancelState, &unusedCancelState);
  if (result == AMPS_E_OK)
  {
    result = AMPS_E_RETRY;
  }
error:
  pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
  AMPS_UNLOCK(&me->lock);
  pthread_cleanup_pop(0);
  pthread_setcancelstate(cancelState, &unusedCancelState);

  return result;
}

void amps_unix_handle_stream_corruption(
  amps_unix_t* me, unsigned failedConnectionVersion)
{
  shutdown(me->fd, SHUT_RDWR);
  amps_unix_set_error(me, "The connection appears corrupt.  Disconnecting.");
  amps_unix_handle_disconnect(me, failedConnectionVersion);
}


amps_result
amps_unix_send_with_version(amps_handle transport,
                            amps_handle message,
                            unsigned* version_out)
{
  amps_unix_t* me = (amps_unix_t*)transport;
  size_t len = 16 * 1024;
  int bytesWritten = -1;
  unsigned int bytesWrittenN = 0;
  int bytesSent = 0;

  *version_out = me->connectionVersion;

  if (me->disconnecting)
  {
    amps_unix_set_error(me, "Disconnecting.");
    return AMPS_E_DISCONNECTED;
  }

  if (me->fd == AMPS_INVALID_SOCKET)
  {
    amps_unix_set_error(me, "Not connected.");
    return AMPS_E_DISCONNECTED;
  }

  /* serialize */
  AMPS_LOCK(&me->sendLock);
  while (bytesWritten < 0)
  {
    if (me->buf == NULL)
    {
      me->buf = (char*)malloc(len);
      if (me->buf == NULL)
      {
        amps_unix_set_error(me, "Unable to allocate memory to send message.");
        AMPS_UNLOCK(&me->sendLock);
        return AMPS_E_MEMORY;
      }
      me->capacity = len;
    }
    else
    {
      len = me->capacity;
    }
    /* reserve 4 bytes for the length of the message */
    bytesWritten = amps_message_serialize(message, me->serializer, (me->buf) + 4, len - 4);
    /* amps_message_serialize could have written 0 bytes.
     * who are we to judge?  If it returns less than 0,
     * there wasn't enough room. */
    if (bytesWritten >= 0)
    {
      break;
    }
    /* free this buffer, allocate a larger buffer next time */
    free(me->buf);
    me->capacity = 0;
    me->buf = NULL;
    len = (size_t)((double)len * 1.5);
  }
  /* once we're done, we don't free buf -- it hangs around until we need it next time.
   * record the message length all up in the first 4 bytes. */
  bytesWrittenN = htonl((unsigned int)bytesWritten);
  me->filterFunction((const unsigned char*)me->buf + 4, (unsigned int)bytesWritten, 0, me->filterUserData);
  *((unsigned int*)(me->buf)) = bytesWrittenN;
  bytesWritten += 4;
  /*now, send */
  while (bytesSent < bytesWritten)
  {
    ssize_t bytesWrittenThisTime;
    bytesWrittenThisTime = send(me->fd, (me->buf) + bytesSent, (unsigned int)(bytesWritten - bytesSent), MSG_NOSIGNAL);
    if (bytesWrittenThisTime <= 0)
    {
      /* record the error */
      amps_unix_set_error(me, "The connection is closed.");
      AMPS_UNLOCK(&me->sendLock);
      return AMPS_E_DISCONNECTED;
    }
    bytesSent += (int)bytesWrittenThisTime;
  }
  AMPS_UNLOCK(&me->sendLock);

  return AMPS_E_OK;
}

amps_result
amps_unix_send(amps_handle transport,
               amps_handle message)
{
  unsigned version_out;
  return amps_unix_send_with_version(transport, message, &version_out);
}


void* amps_unix_threaded_reader(void* userData)
{
  int unusedCancelState = 0;
  amps_unix_t* me = (amps_unix_t*)userData;
  unsigned char* buffer, *newBuffer, *end, *readPoint, *parsePoint;
  unsigned int msglenN;
  unsigned long msglen, currentPosition, bytesRead = 0;
  ssize_t received;
  ssize_t recv_result;
  int cancelState = 0;
  const size_t BUFFER_SIZE = 16 * 1024;
  const unsigned MAX_MESSAGE_SIZE = 1024 * 1024 * 1024;
  amps_uint64_t lastReadTime = 0;
  amps_uint64_t lastIdleTime = 0;
  amps_handle   message;

  /* capture the connection version we are using now. */
  unsigned connectionVersion = me->connectionVersion;
  AMPS_SOCKET fd = me->fd;

  /* Wait for pthread_create to set me->thread */
  while (me->thread != pthread_self()
         && !me->disconnecting
         && !me->destroying)
  {
    AMPS_SLEEP(5);
  }
  if (me->threadCreatedCallback)
  {
    amps_result result = me->threadCreatedCallback(me->thread,
                                                   me->threadCreatedCallbackUserData);
    AMPS_IEX(&me->threadCreatedCallbackResult, (long)result);
    if (result != AMPS_E_OK)
    {
      return NULL;
    }
  }
  else
  {
    AMPS_IEX(&me->threadCreatedCallbackResult, (long)AMPS_E_OK);
  }

  lastReadTime = amps_now();
  lastIdleTime = lastReadTime;

  pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
  message = amps_message_create(NULL);
  buffer = malloc(BUFFER_SIZE);
  pthread_cleanup_push(amps_message_destroy, (void*)message);
  pthread_cleanup_push(amps_cleanup_free_buffer, (void*)(&buffer));
  pthread_setcancelstate(cancelState, &unusedCancelState);
  if (!buffer)
  {
    amps_unix_handle_disconnect(me, connectionVersion);
    goto cleanup;
  }

  end = buffer + BUFFER_SIZE;
  readPoint = buffer;
  parsePoint = buffer;

  /* while we're open and not disconnecting */
  while (connectionVersion == me->connectionVersion
         && !me->disconnecting
         && fd == me->fd
         && me->thread == pthread_self()
        )
  {
    if (me->destroying)
    {
      goto cleanup;
    }
    if (me->idleTimeMillis > 0 &&
        (amps_now() - lastIdleTime) > me->idleTimeMillis)
    {
      lastIdleTime = amps_now();
      cancelState = 0;
      pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
      me->messageHandler(0L,
                         me->messageHandlerUserData);
      pthread_setcancelstate(cancelState, &unusedCancelState);
    }
    recv_result = recv(fd, (char*)readPoint, (size_t)(end - readPoint), 0);
    if (recv_result > 0)
    {
      received = recv_result;
      /* Call filter function without the 4 byte size */
      me->filterFunction(readPoint, (size_t)received, 1, me->filterUserData);
      readPoint += received;
      lastReadTime = amps_now();
    }
    else
    {
      int errorcode = GET_ERRNO;
      if (errorcode == SOCK_ERRORCODE(EINTR))
      {
        continue;
      }
      /* disconnect if not a timeout error, or the user only set a read timeout, or the
       * read timeout has been exceeded. */
      if ( (errorcode != SOCK_ERRORCODE(ETIMEDOUT) && errorcode != SOCK_ERRORCODE(EWOULDBLOCK))
           || me->idleTimeMillis == 0
           || (me->readTimeoutMillis && amps_now() - lastReadTime > me->readTimeoutMillis))
      {
        if (fd != AMPS_INVALID_SOCKET && !me->disconnecting)
        {
          shutdown(fd, SHUT_RDWR);
          amps_unix_handle_disconnect(me, connectionVersion);
        }
        goto cleanup;
      }
      continue;
    }
    while (readPoint >= parsePoint + 4
           && fd == me->fd)
    {
      msglenN = *(unsigned int*)parsePoint;
      msglen = ntohl(msglenN);
      if (msglen > MAX_MESSAGE_SIZE)
      {
        amps_unix_handle_stream_corruption(me, connectionVersion);
        goto cleanup;
      }
      if (readPoint >= (parsePoint + msglen + 4))
      {
        /* there are enough bytes to parse this guy. */
        parsePoint += 4;
        if (amps_message_pre_deserialize(message, me->serializer,
                                         (amps_char*)parsePoint, msglen) == AMPS_E_OK)
        {
          currentPosition = 0;
          while (currentPosition < msglen)
          {
            if (amps_message_deserialize(message, me->serializer,
                                         currentPosition,
                                         &bytesRead) != AMPS_E_OK)
            {
              amps_unix_handle_stream_corruption(me, connectionVersion);
              goto cleanup;
            }
            if (me->messageHandler)
            {
              cancelState = 0;
              pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
              me->messageHandler(message,
                                 me->messageHandlerUserData);
              pthread_setcancelstate(cancelState, &unusedCancelState);
            }
            currentPosition += bytesRead;
          }
        }
        else
        {
          amps_unix_handle_stream_corruption(me, connectionVersion);
          goto cleanup;
        }
        parsePoint += msglen;
      }
      else if (end < buffer + msglen + 4)
      {
        /* this message is larger than the buffer
           * resize to 2* larger of this message or current buffer size */
        size_t newLength = end < buffer + (msglen * 2) ?
                           msglen * 2 : (size_t)(end - buffer);
        newBuffer = malloc( newLength );
        if (newBuffer == NULL)
        {
          assert(readPoint >= parsePoint);
          /* stream broken */
          shutdown(fd, SHUT_RDWR);
          amps_unix_handle_disconnect(me, connectionVersion);
          goto cleanup;
        }
        memcpy(newBuffer, parsePoint, (size_t)(readPoint - parsePoint));
        readPoint = newBuffer + (readPoint - parsePoint);
        parsePoint = newBuffer;
        cancelState = 0;
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
        free(buffer);
        buffer = newBuffer;
        pthread_setcancelstate(cancelState, &unusedCancelState);
        end = newBuffer + newLength;
        assert(readPoint >= parsePoint);
        break;
      }
      else
      {
        /* Need to read more data to finish this message
         * Move what we have to the front of the buffer. */
        break;
      }
    } /*while(readPoint >= parsePoint + 4) */
    if (readPoint > parsePoint)
    {
      memmove(buffer, parsePoint, (size_t)(readPoint - parsePoint));
    }
    readPoint = buffer + (readPoint - parsePoint);
    parsePoint = buffer;
  } /* while(fd != -1) */
cleanup:
  cancelState = 0;
  pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
  pthread_cleanup_pop(0);
  pthread_cleanup_pop(0);
  amps_message_destroy(message);
  free(buffer);
  if (me->destroying)
  {
    return 0;
  }
  pthread_t tid = pthread_self();
#if __STDC_VERSION__ >= 201100
  if (atomic_compare_exchange_strong(&(me->thread), &tid, (pthread_t)0))
  {
    pthread_detach(tid);
  }
#else
  if (__sync_bool_compare_and_swap(&(me->thread), tid, 0))
  {
    pthread_detach(tid);
  }
#endif
  return 0;
}

amps_result
amps_unix_attempt_reconnect(amps_handle transport, unsigned version)
{
  amps_result res;
  amps_unix_t* me = (amps_unix_t*)transport;
  if (version == 0)
  {
    version = me->connectionVersion;
  }
  res = amps_unix_handle_disconnect(transport, version);
  return res;
}

/* public-api -- get a socket */
AMPS_SOCKET
amps_unix_get_socket(amps_handle transport)
{
  amps_unix_t* me = (amps_unix_t*)transport;
  return me->fd;
}

amps_result
amps_unix_update_read_timeout(amps_unix_t* me)
{
  int timeout = (int)((me->readTimeoutMillis && me->idleTimeMillis) ?
                      AMPS_MIN(me->readTimeoutMillis, me->idleTimeMillis) :
                      AMPS_MAX(me->readTimeoutMillis, me->idleTimeMillis));
  int rc = 0;
  struct timeval tv;
  tv.tv_sec = timeout / 1000;
  tv.tv_usec = (timeout % 1000) * 1000;
  rc = setsockopt(me->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(struct timeval));
  if (rc == -1)
  {
    amps_unix_set_socket_error(me);
    return AMPS_E_USAGE;
  }
  return AMPS_E_OK;
}
amps_result
amps_unix_set_read_timeout(amps_handle transport, int readTimeout)
{
  amps_unix_t* me = (amps_unix_t*)transport;
  me->readTimeoutMillis = (amps_uint64_t)readTimeout * 1000;
  return amps_unix_update_read_timeout(me);
}

void
amps_unix_set_filter_function(amps_handle transport, amps_transport_filter_function filterFunction_, void* userdata_)
{
  amps_unix_t* me = (amps_unix_t*)transport;
  me->filterUserData = userdata_;
  me->filterFunction = filterFunction_ ? filterFunction_ : amps_unix_noop_filter_function;
}

void
amps_unix_set_thread_created_callback(amps_handle transport_, amps_thread_created_callback threadCreatedCallback_, void* userdata_)
{
  amps_unix_t* me = (amps_unix_t*)transport_;
  me->threadCreatedCallbackUserData = userdata_;
  me->threadCreatedCallback = threadCreatedCallback_;
}

amps_result
amps_unix_set_idle_time(amps_handle transport, int millis)
{
  amps_unix_t* me = (amps_unix_t*)transport;
  me->idleTimeMillis = (amps_uint64_t)millis;
  return amps_unix_update_read_timeout(me);
}
#endif /* ifndef _WIN32 */
