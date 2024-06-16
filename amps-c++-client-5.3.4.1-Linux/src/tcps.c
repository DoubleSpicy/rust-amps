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
#if _MSC_VER < 1400
  /*support for visual c++ 2003 */
  #define _WIN32_WINNT 0x0400
#endif
#include <amps/amps_impl.h>
#include <amps/amps_ssl.h>
#include <amps/ampsuri.h>
#if __STDC_VERSION__ >= 201100
#include <stdatomic.h>
#include <stdbool.h>
#define AMPS_IEX(ptr, value) atomic_exchange_explicit((ptr), (value), memory_order_acq_rel)
#define AMPS_IEX_GET(ptr, value) atomic_exchange_explicit((ptr), (value), memory_order_acq_rel)
#define AMPS_IEX_LONG(ptr, value) atomic_exchange_explicit((ptr), (value), memory_order_acq_rel)
#define AMPS_FETCH_ADD(ptr, value) atomic_fetch_add_explicit((ptr), (value), memory_order_acq_rel)
#define AMPS_FETCH_SUB(ptr, value) atomic_fetch_sub_explicit((ptr), (value), memory_order_acq_rel)
#elif defined(_WIN32)
#ifdef _WIN64
  #define AMPS_IEX(ptr,value) _InterlockedExchange64((LONG64*)(ptr), (LONG64)(value))
  #define AMPS_IEX_GET(ptr,value) _InterlockedExchange64((LONG64*)(ptr), (LONG64)(value))
  #define AMPS_IEX_LONG(ptr,value) _InterlockedExchange((ptr), (value))
  #define AMPS_FETCH_ADD(ptr, value) InterlockedExchangeAdd64((LONGLONG volatile*)(ptr), (LONGLONG)(value))
  #define AMPS_FETCH_SUB(ptr, value) InterlockedExchangeAdd64((LONGLONG volatile*)(ptr), (LONGLONG)(-1 * (value)))
#else
  #define AMPS_IEX(ptr,value) _InterlockedExchange((long*)(ptr), (LONG)(value))
  #define AMPS_IEX_GET(ptr,value) _InterlockedExchange((long*)(ptr), (LONG)(value))
  #define AMPS_IEX_LONG(ptr,value) _InterlockedExchange((ptr), (LONG)(value))
  #define AMPS_FETCH_ADD(ptr, value) InterlockedExchangeAdd((LONG volatile*)(ptr), (LONG)(value))
  #define AMPS_FETCH_SUB(ptr, value) InterlockedExchangeAdd((LONG volatile*)(ptr), (LONG)(-1 * (value)))
#endif
#else
#define AMPS_IEX(ptr, value) (void)__sync_lock_test_and_set((ptr), (value))
#define AMPS_IEX_GET(ptr, value) __sync_lock_test_and_set((ptr), (value))
#define AMPS_IEX_LONG(ptr, value) (void)__sync_lock_test_and_set((ptr), (value))
#define AMPS_FETCH_ADD(ptr, value) __sync_fetch_and_add((ptr), (value))
#define AMPS_FETCH_SUB(ptr, value) __sync_fetch_and_sub((ptr), (value))
#endif

#ifdef _WIN32

/* Causes linked programs to add ws2_32.lib which is required by
 * this module. If you'd like to turn this off, define the macro
 * AMPS_WINDOWS_NO_DEFAULT_LIBS.
 */
#ifndef AMPS_WINDOWS_NO_DEFAULT_LIBS
  #pragma comment(lib,"ws2_32.lib")
#endif

#include <Ws2tcpip.h>
#define GET_ERRNO (WSAGetLastError())
#define SOCK_ERRORCODE(x) WSA##x
#define AMPS_INITLOCK(x) InitializeCriticalSection(x)
#define AMPS_LOCK(x) EnterCriticalSection(x)
/* defined in tcp.c */
int amps_win_spin_lock(LPCRITICAL_SECTION x);
#define AMPS_SPIN_LOCK(x) amps_win_spin_lock(x)
#define AMPS_SPIN_LOCK_UNLIMITED(x) EnterCriticalSection(x)
#define AMPS_UNLOCK(x) LeaveCriticalSection(x)
#define AMPS_KILLLOCK(x) DeleteCriticalSection(x)
int tcpsThreadTimeoutMillis = 10000;
#define AMPS_JOIN_DECLARE() \
  DWORD tid = (DWORD)0; \
  HANDLE tidH = (HANDLE)0

#define AMPS_JOIN(x) \
  tid = (DWORD)AMPS_IEX(&x, 0); \
  tidH = (HANDLE)AMPS_IEX(&x##H, 0); \
  if (tid != 0 && tid != GetCurrentThreadId()) \
  { \
    if(WAIT_TIMEOUT == WaitForSingleObject(tidH, tcpsThreadTimeoutMillis)) \
    { \
      TerminateThread(tidH, -1); \
    } \
  }
#define AMPS_SLEEP(x) \
  Sleep(x)
#define SHUT_RDWR SD_BOTH
#else
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
#define AMPS_JOIN_DECLARE() \
  pthread_t tid = (pthread_t)0;

#if __STDC_VERSION__ >= 201100
#define AMPS_JOIN(x) \
  tid = atomic_exchange((&x), (pthread_t)0); \
  if(tid!=(pthread_t)0 && tid!=pthread_self()) \
  { \
    pthread_join(tid, NULL); \
  }
#else
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

#define AMPS_SLEEP(x) { \
    static struct timespec ts = { x/1000, (x%1000) * 1000000 }; \
    nanosleep(&ts, NULL); }
#endif

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
#if defined(_WIN32) && !defined(_WIN64)
  volatile long threadCreatedCallbackResult;
#else
  volatile amps_int64_t threadCreatedCallbackResult;
#endif
  volatile unsigned connectionVersion;
#endif

  size_t capacity;
#if __STDC_VERSION__ >= 201100
  _Atomic AMPS_SOCKET fd;
  _Atomic(_amps_SSL*)  ssl;
  atomic_bool disconnecting;
  atomic_bool destroying;
#else
  volatile AMPS_SOCKET fd;
  _amps_SSL*  ssl;
  volatile long disconnecting;
  volatile long destroying;
#endif
#ifdef _WIN32
  CRITICAL_SECTION lock;
  CRITICAL_SECTION sendLock;
#if __STDC_VERSION__ >= 201100
  _Atomic DWORD thread;
  _Atomic HANDLE threadH;
#else
  volatile DWORD thread;
  volatile HANDLE threadH;
#endif
#else
  pthread_mutex_t lock;
  pthread_mutex_t sendLock;
#if __STDC_VERSION__ >= 201100
  _Atomic pthread_t thread;
#else
  volatile pthread_t thread;
#endif
#endif
  char lastErrorBuf[1024];
}
amps_tcps_t;

const amps_char* amps_tcps_get_last_error(
  amps_handle transport)
{
  return ((amps_tcps_t*)transport)->lastErrorBuf;
}

void amps_tcps_set_error(
  amps_tcps_t* transport,
  const amps_char* buffer)
{
  _AMPS_SNPRINTF(transport->lastErrorBuf, sizeof(transport->lastErrorBuf), "%s", buffer);
  transport->lastErrorBuf[sizeof(transport->lastErrorBuf) - 1] = '\0';
}

void amps_tcps_set_socket_error(
  amps_tcps_t* transport,
  int errorCode)
{
#ifdef _WIN32
  char localBuf[256];
  FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM, NULL, errorCode, 0, localBuf, sizeof(localBuf), NULL);
  amps_tcps_set_error(transport, localBuf);
#else
  amps_tcps_set_error(transport, strerror(errorCode));
#endif
}

void amps_tcps_set_hostname_error(
  amps_tcps_t* transport,
  int rc,
  int errorCode)
{
#ifdef _WIN32
  amps_tcps_set_socket_error(transport, errorCode);
#else
  strncpy(transport->lastErrorBuf, gai_strerror(rc), sizeof(transport->lastErrorBuf));
  transport->lastErrorBuf[sizeof(transport->lastErrorBuf) - 1] = '\0';
#endif
}

void amps_tcps_set_error_stack_error(amps_tcps_t* transport)
{
  char buffer[256];
  _amps_ERR_error_string_n(_amps_ERR_get_error(), buffer, 256);
  amps_tcps_set_error(transport, buffer);
}

void amps_tcps_set_ssl_error(amps_tcps_t* transport,
                             int rc,
                             int sysError)
{
  char buffer[256];
  if (!transport->ssl)
  {
    _AMPS_SNPRINTF(buffer, 256, "SSL error, SSL is closing");
    amps_tcps_set_error(transport, buffer);
    return;
  }
  int sslError = _amps_SSL_get_error(transport->ssl, rc);
  unsigned long errorQueue = 0;
  switch (sslError)
  {
  case AMPS_SSL_ERROR_SSL:
    amps_tcps_set_error_stack_error(transport);
    break;
  case AMPS_SSL_ERROR_SYSCALL:
    errorQueue = _amps_ERR_get_error();
    if (errorQueue)
    {
      _amps_ERR_error_string_n(errorQueue, buffer, 256);
      amps_tcps_set_error(transport, buffer);
    }
    else if (rc == 0 || sysError == 0)
    {
      amps_tcps_set_error(transport, "An unexpected disconnect occurred.");
    }
    else
    {
      amps_tcps_set_socket_error(transport, sysError);
    }
    break;
  default:
    _AMPS_SNPRINTF(buffer, 256, "Unexpected SSL error %d", sslError);
    amps_tcps_set_error(transport, buffer);
    return;
  }
}

void
amps_tcps_noop_filter_function(const unsigned char* data, size_t length, short direction, void* userdata)
{;}

void amps_tcps_atfork_handler(amps_tcps_t* transport_, int code_)
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

amps_handle amps_tcps_create(void)
{
  amps_tcps_t* transport = (amps_tcps_t*)malloc(sizeof(amps_tcps_t));

  if (transport != NULL)
  {
    memset(transport, 0, sizeof(amps_tcps_t));
    transport->fd = AMPS_INVALID_SOCKET;
    transport->ssl = NULL;
    AMPS_INITLOCK(&transport->lock);
    AMPS_INITLOCK(&transport->sendLock);
#if __STDC_VERSION__ >= 201100
    transport->disconnecting = false;
    transport->destroying = false;
#endif
    transport->filterFunction = &amps_tcps_noop_filter_function;
    amps_atfork_add(transport, (_amps_atfork_callback_function)amps_tcps_atfork_handler);
  }

  return transport;
}
#ifdef _WIN32
  DWORD amps_tcps_threaded_reader(void* userData);
#else
  void* amps_tcps_threaded_reader(void* userData);
#endif

int
amps_tcps_opt_parse(const char* val, size_t valLen, int* parsed)
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
amps_tcps_apply_socket_property(AMPS_SOCKET fd, const char* key, size_t keyLen, const
                                char* val, size_t valLen)
{
  int value = 0, level = SOL_SOCKET, optname;
  unsigned int optValueSize = sizeof(int);
  char* optValuePtr = (char*)&value;
  struct linger lingerStruct;
  if (keyLen == 4 && memcmp(key, "bind", keyLen) == 0)
  {
    struct addrinfo* pAddrInfo = NULL, addrhints;
    char addr[256] = { 0 };
    char port[256] = { 0 };
    size_t addrLen = valLen, portLen = 0;
    char* portStr = (char*)(memchr((void*)val, ':', valLen));
    char* ipv6Str = (char*)(memchr((void*)val, '[', valLen));

#ifdef _WIN32
    int addrFamily = 0;
    WSAPROTOCOL_INFOW localProto;
    int localProtoLen = sizeof(WSAPROTOCOL_INFO);
    if (getsockopt(fd, SOL_SOCKET, SO_PROTOCOL_INFO, (char*)&localProto, &localProtoLen) == SOCKET_ERROR)
    {
      return AMPS_E_URI;
    }
    addrFamily = localProto.iAddressFamily;
#elif !defined(__APPLE__)
    int addrFamily = 0;
    socklen_t addrFamilyLen = sizeof(addrFamily);
    if (getsockopt(fd, SOL_SOCKET, AMPS_SO_DOMAIN, (void*)&addrFamily, &addrFamilyLen) < 0)
    {
      return AMPS_E_URI;
    }
#endif

    if (ipv6Str)
    {
      ++ipv6Str;
      char* ipv6StrEnd = (char*)(memchr((void*)ipv6Str, ']', (size_t)(val + valLen - ipv6Str)));
      if (!ipv6StrEnd)
      {
        return AMPS_E_URI;
      }
      addrLen = (size_t)(ipv6StrEnd - ipv6Str);
      memcpy(addr, ipv6Str, addrLen);
      addr[addrLen] = '\0';

      if ((size_t)(val + valLen - ++ipv6StrEnd) < valLen && *ipv6StrEnd == ':')
      {
        portLen = (size_t)(val + valLen - ++ipv6StrEnd);
        memcpy(port, ipv6StrEnd, portLen);
        port[portLen] = '\0';
      }
    }
    else if (portStr)
    {
      addrLen = (size_t)(portStr - val);
      portLen = (size_t)(val + valLen - ++portStr);
      memcpy(addr, val, addrLen);
      addr[addrLen] = '\0';
      memcpy(port, portStr, portLen);
      port[portLen] = '\0';
    }
    else
    {
      memcpy(addr, val, addrLen);
      addr[addrLen] = '\0';
    }

    /* use getaddrinfo() to find an appropriate address */
    memset(&addrhints, 0, sizeof(struct addrinfo));
    addrhints.ai_socktype = SOCK_STREAM;
    addrhints.ai_protocol = 0;
#ifndef __APPLE__
    addrhints.ai_family = addrFamily; // match the family of the socket
#else
    if (ipv6Str)
    {
      addrhints.ai_family = AF_INET6;
    }
    else
    {
      addrhints.ai_family = AF_INET;
    }
#endif
    addrhints.ai_flags = (AI_V4MAPPED | AI_ADDRCONFIG | AI_PASSIVE);

    int rc = getaddrinfo(addr, port, &addrhints, &pAddrInfo);
    if (rc != 0 || pAddrInfo == NULL)
    {
      if (pAddrInfo)
      {
        freeaddrinfo(pAddrInfo);
      }
      return AMPS_E_URI;
    }

    if (bind(fd, pAddrInfo->ai_addr, (socklen_t)pAddrInfo->ai_addrlen) != 0)
    {
      freeaddrinfo(pAddrInfo);
      return AMPS_E_URI;
    }
    freeaddrinfo(pAddrInfo);
    return AMPS_E_OK;
  }
  else if (keyLen == 18 && memcmp(key, "ip_protocol_prefer", keyLen) == 0)
  {
    // no-op; handled in the connect function
    return AMPS_E_OK;
  }
  else if (keyLen == 3 && memcmp(key, "sni", keyLen) == 0)
  {
    // no-op; handled in amps_tcps_apply_ssl_properties
    return AMPS_E_OK;
  }
  // properties after this point must only be boolean or numeric
  if (!amps_tcps_opt_parse(val, valLen, &value))
  {
    return AMPS_E_URI;
  }
  if (keyLen == 10 && memcmp(key, "tcp_rcvbuf", keyLen) == 0)
  {
    optname = SO_RCVBUF;
  }
  else if (keyLen == 13 && memcmp(key, "tcp_keepalive", keyLen) == 0 )
  {
    optname = SO_KEEPALIVE;
  }
  else if (keyLen == 10 && memcmp(key, "tcp_sndbuf", keyLen) == 0)
  {
    optname = SO_SNDBUF;
  }
  else if (keyLen == 11 && memcmp(key, "tcp_nodelay", keyLen) == 0)
  {
    optname = TCP_NODELAY;
    level = IPPROTO_TCP;
  }
  else if (keyLen == 10 && memcmp(key, "tcp_linger", keyLen) == 0)
  {
    optname = SO_LINGER;
    lingerStruct.l_onoff = value != 0;
    lingerStruct.l_linger = (u_short)value;
    optValuePtr = (char*)&lingerStruct;
    optValueSize = sizeof(struct linger);
  }
  else if (keyLen == 6 && memcmp(key, "pretty", keyLen) == 0)
  {
    // no-op; handled in C++ layer
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
amps_tcps_apply_socket_properties(AMPS_SOCKET fd, const char* uri,
                                  size_t uriLength, amps_uri_state* uriState)
{
  const char* key = NULL;
  size_t keyLength = 0;
  /* First, enable tcp_keepalive by default */
  int value = 1;
  unsigned int optValueSize = sizeof(int);
  char* optValuePtr = (char*)&value;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, optValuePtr, optValueSize))
  {
    return AMPS_E_URI;
  }
  /* Now, parse the rest of the URI */
  while (uriState->part_id < AMPS_URI_ERROR)
  {
    amps_uri_parse(uri, uriLength, uriState);
    if (uriState->part_id == AMPS_URI_OPTION_KEY)
    {
      key = uriState->part;
      keyLength = uriState->part_length;
    }
    else if (uriState->part_id == AMPS_URI_OPTION_VALUE)
    {
      if (amps_tcps_apply_socket_property(fd, key, keyLength,
                                          uriState->part, uriState->part_length) != AMPS_E_OK)
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

int
amps_tcps_apply_ssl_properties(_amps_SSL *ssl, const char* uri,
                               size_t uriLength, amps_uri_state* uriState)
{
  int rc = 1;
  const char *key = NULL;
  size_t keyLength = 0;
  while (uriState->part_id < AMPS_URI_ERROR)
  {
    amps_uri_parse(uri, uriLength, uriState);
    if (uriState->part_id == AMPS_URI_OPTION_KEY)
    {
      key = uriState->part;
      keyLength = uriState->part_length;
    }
    else if (uriState->part_id == AMPS_URI_OPTION_VALUE)
    {
      if (keyLength == 3 && memcmp(key, "sni", keyLength) == 0)
      {
        char* hostname = (char*)malloc(uriState->part_length + 1);
        memcpy(hostname, uriState->part, uriState->part_length);
        hostname[uriState->part_length] = '\0';
        rc = _amps_SSL_ctrl(ssl, AMPS_SSL_CTRL_SET_TLSEXT_HOSTNAME, AMPS_TLSEXT_NAMETYPE_host_name, hostname);
        free(hostname);
        if (rc != 1)
        {
          return rc;
        }
      }
    }
  }
  return rc;
}

amps_result amps_tcps_set_idle_time(amps_handle transport, int idleTimeMillis);
amps_result amps_tcps_connect(amps_handle transport, const amps_char* address)
{
  AMPS_JOIN_DECLARE();
  amps_tcps_t* me = (amps_tcps_t*)transport;
  char* host = NULL, *port = NULL, *protocol = NULL;
  amps_uri_state uri_state_socket_props, uri_state_addr_props, uri_state_ssl_props;
  amps_ip_protocol_preference ip_proto_prefer = AMPS_DEFAULT_IP_PROTOCOL_PREFERENCE;
  int ip_proto_pref_override = 0;
  int rc, sysError, i;
  amps_result result = AMPS_E_OK;
  struct addrinfo* pAddrInfo = NULL, addrhints;
  size_t address_length = 0;
  AMPS_SOCKET fd = AMPS_INVALID_SOCKET;
  _amps_SSL* ssl = 0;

#ifdef _WIN32
  WSADATA wsaData;
  rc = WSAStartup(MAKEWORD(2, 2), &wsaData);
  if (rc != 0)
  {
    amps_tcps_set_error(me, "Windows Sockets could not be started.");
    return AMPS_E_MEMORY;
  }
#endif

  AMPS_LOCK(&me->lock);
#ifndef _WIN32
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me->lock);
#endif
  /* if we were previously marked as disconnecting, forget that. */
  AMPS_IEX_LONG(&me->disconnecting, 0);
  AMPS_LOCK(&me->sendLock);
#ifndef _WIN32
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me->sendLock);
#endif
  fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
  ssl = (_amps_SSL*)AMPS_IEX_GET(&me->ssl, 0);

  /* were we previously connected?  go ahead and close that. */
  if (ssl)
  {
    rc = _amps_SSL_shutdown(ssl);
#ifdef _WIN32
    sysError = GetLastError();
#else
    sysError = errno;
#endif
    if (rc < 0)
    {
      amps_tcps_set_ssl_error(me, rc, sysError);
    }
  }
  if (fd != AMPS_INVALID_SOCKET)
  {
    /* this is a reconnect */
    shutdown(fd, SHUT_RDWR);
    closesocket(fd);
    fd = AMPS_INVALID_SOCKET;
  }
  AMPS_JOIN(me->thread);
  if (ssl)
  {
    _amps_SSL_free(ssl);
    ssl = 0;
  }

  /* parse out the address */
  memset(&uri_state_socket_props, 0, sizeof(amps_uri_state));
  memset(&uri_state_addr_props, 0, sizeof(amps_uri_state));
  memset(&uri_state_ssl_props, 0, sizeof(amps_uri_state));
  address_length = strlen(address);
  while (uri_state_addr_props.part_id < AMPS_URI_ERROR)
  {
    amps_uri_parse(address, address_length, &uri_state_addr_props);
    switch (uri_state_addr_props.part_id)
    {
    case AMPS_URI_PROTOCOL:
      protocol = _AMPS_STRNDUP(uri_state_addr_props.part, uri_state_addr_props.part_length);
      me->serializer = amps_message_get_protocol(protocol);
      if (me->serializer == -1)
      {
        amps_tcps_set_error(me,
                            "The URI specified an unavailable protocol.");
        result = AMPS_E_URI;
        goto error;
      }
      memcpy(&uri_state_socket_props, &uri_state_addr_props, sizeof(amps_uri_state));
      memcpy(&uri_state_ssl_props, &uri_state_addr_props, sizeof(amps_uri_state));
      break;
    case AMPS_URI_HOST:
      host = _AMPS_STRNDUP(uri_state_addr_props.part, uri_state_addr_props.part_length);
      break;
    case AMPS_URI_PORT:
      port = _AMPS_STRNDUP(uri_state_addr_props.part, uri_state_addr_props.part_length);
      break;
    case AMPS_URI_OPTION_KEY:
      if (uri_state_addr_props.part_length == 18 && memcmp(uri_state_addr_props.part, "ip_protocol_prefer", uri_state_addr_props.part_length) == 0)
      {
        ip_proto_pref_override = 1;
      }
      break;
    case AMPS_URI_OPTION_VALUE:
      if (ip_proto_pref_override)
      {
        if (uri_state_addr_props.part_length == 4 && memcmp(uri_state_addr_props.part, "ipv4", uri_state_addr_props.part_length) == 0)
        {
          ip_proto_prefer = AMPS_PREFER_IPV4;
        }
        else if (uri_state_addr_props.part_length == 4 && memcmp(uri_state_addr_props.part, "ipv6", uri_state_addr_props.part_length) == 0)
        {
          ip_proto_prefer = AMPS_PREFER_IPV6;
        }
        else
        {
          amps_tcps_set_error(me,
                              "The URI specified an invalid ip protocol preference.");
          result = AMPS_E_URI;
          goto error;
        }
      }
      break;
    default:
      break;
    }
  }
  if (uri_state_addr_props.part_id == AMPS_URI_ERROR)
  {
    amps_tcps_set_error(me, "URI format invalid.");
    result = AMPS_E_URI;
    goto error;
  }

  /* use getaddrinfo() to find an appropriate address */
  memset(&addrhints, 0, sizeof(struct addrinfo));
  addrhints.ai_socktype = SOCK_STREAM;
  addrhints.ai_protocol = 0;
  addrhints.ai_flags = (AI_V4MAPPED | AI_ADDRCONFIG);
  addrhints.ai_family = ip_proto_prefer == AMPS_PREFER_IPV6 ? AF_INET6 : AF_INET; /* Try the preferred protocol first */

  rc = getaddrinfo(host, port, &addrhints, &pAddrInfo);
#ifdef _WIN32
  sysError = GetLastError();
  if (rc == EAI_FAMILY || rc == EAI_NONAME || rc == EAI_AGAIN || rc == EAI_SERVICE)
#else
  sysError = errno;
  if (rc == EAI_ADDRFAMILY || rc == EAI_NONAME || rc == EAI_AGAIN || rc == EAI_SERVICE)
#endif
  {
    // didn't find any addresses of the preferred protocol, try the other one.
    addrhints.ai_family = ip_proto_prefer == AMPS_PREFER_IPV6 ? AF_INET : AF_INET6;
    rc = getaddrinfo(host, port, &addrhints, &pAddrInfo);
#ifdef _WIN32
    sysError = GetLastError();
#else
    sysError = errno;
#endif
    if (rc != 0)
    {
      /* Try preferred protocol again without AI_ADDRCONFIG */
      addrhints.ai_flags = AI_V4MAPPED;
      addrhints.ai_family = ip_proto_prefer == AMPS_PREFER_IPV6 ? AF_INET6 : AF_INET;
      rc = getaddrinfo(host, port, &addrhints, &pAddrInfo);
#ifdef _WIN32
      sysError = GetLastError();
#else
      sysError = errno;
#endif
    }
  }
  if (rc != 0)
  {
    result = AMPS_E_CONNECTION_REFUSED;
    amps_tcps_set_hostname_error(me, rc, sysError);
    freeaddrinfo(pAddrInfo);
    goto error;
  }

  me->fd = socket(pAddrInfo->ai_family, pAddrInfo->ai_socktype, pAddrInfo->ai_protocol);
#ifdef _WIN32
  sysError = GetLastError();
#else
  sysError = errno;
#endif
  if (me->fd == AMPS_INVALID_SOCKET)
  {
    freeaddrinfo(pAddrInfo);
    amps_tcps_set_socket_error(me, sysError);
    result = AMPS_E_CONNECTION_REFUSED;
    goto error;
  }

  /* apply socket properties */
  if (amps_tcps_apply_socket_properties(me->fd, address, address_length, &uri_state_socket_props) != AMPS_E_OK)
  {
    freeaddrinfo(pAddrInfo);
    fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
    amps_tcps_set_error(me, "The URI specified invalid properties.");
    result = AMPS_E_URI;
    goto error;
  }
#ifdef __APPLE__
  rc = 1; /* Set NOSIGPIPE to ON */
  if (setsockopt(me->fd, SOL_SOCKET, SO_NOSIGPIPE, &rc, sizeof(rc)) < 0)
  {
    freeaddrinfo(pAddrInfo);
    fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
    amps_tcps_set_error(me, "Failed to set no SIGPIPE.");
    result = AMPS_E_CONNECTION;
    goto error;
  }
#endif

  rc = connect(me->fd, pAddrInfo->ai_addr,
               (socklen_t)pAddrInfo->ai_addrlen);
#ifdef _WIN32
  sysError = GetLastError();
#else
  sysError = errno;
#endif
  freeaddrinfo(pAddrInfo);
  if (rc == -1)
  {
    fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
    amps_tcps_set_socket_error(me, sysError);
    result = AMPS_E_CONNECTION_REFUSED;
    goto error;
  }

  /* On first use, initialize an SSL context if one hasn't been created. */
  if (!_amps_ssl_ctx)
  {
    rc = amps_ssl_init(NULL);
    if (rc)
    {
      fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
      amps_tcps_set_error(me, "Could not initialize an SSL context.");
      result = AMPS_E_SSL;
      goto error;
    }
  }
  /* Handshake via SSL */
  _amps_ERR_clear_error();
  if (!me->ssl)
  {
    me->ssl = _amps_SSL_new(_amps_ssl_ctx);
  }
  if (me->ssl == 0)
  {
    fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
    amps_tcps_set_error_stack_error(me);
    result = AMPS_E_SSL;
    goto error;
  }

  rc = _amps_SSL_set_fd(me->ssl, (int)me->fd);
#ifdef _WIN32
  sysError = GetLastError();
#else
  sysError = errno;
#endif
  if (rc != 1)
  {
    fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
    ssl = (_amps_SSL*)AMPS_IEX_GET(&me->ssl, 0);
    amps_tcps_set_ssl_error(me, rc, sysError);
    result = AMPS_E_SSL;
    goto error;
  }

  rc = amps_tcps_apply_ssl_properties(me->ssl, address, address_length, &uri_state_ssl_props);
#ifdef _WIN32
  sysError = GetLastError();
#else
  sysError = errno;
#endif
  if (rc != 1)
  {
    fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
    ssl = (_amps_SSL*)AMPS_IEX_GET(&me->ssl, 0);
    amps_tcps_set_ssl_error(me, rc, sysError);
    result = AMPS_E_SSL;
    goto error;
  }

  _amps_SSL_ctrl(me->ssl, AMPS_SSL_CTRL_MODE, AMPS_SSL_AUTO_RETRY, NULL);

  rc = _amps_SSL_connect(me->ssl);
#ifdef _WIN32
  sysError = GetLastError();
#else
  sysError = errno;
#endif
  if (rc != 1)
  {
    fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
    ssl = (_amps_SSL*)AMPS_IEX_GET(&me->ssl, 0);
    amps_tcps_set_ssl_error(me, rc, sysError);
    result = AMPS_E_SSL;
    goto error;
  }

  // tcps transport must use read with a timeout.
  amps_tcps_set_idle_time(transport, 100);

  /* Increase the connection version now, so that anyone else attempting a reconnect knows
   * that they may not need to. */
  me->connectionVersion++;
  result = AMPS_E_OK;

  /*  and, start up a new reader thread */
  if (me->threadCreatedCallback)
  {
    AMPS_IEX(&me->threadCreatedCallbackResult, -1);
  }
#ifdef _WIN32
  me->threadH = CreateThread(NULL, 0,
                             (LPTHREAD_START_ROUTINE)amps_tcps_threaded_reader,
                             me, CREATE_SUSPENDED, (LPDWORD)&me->thread);
  /* Now that me->thread is populated, we can start the thread.
   * important to do it in two steps, since we use me->thread as a poor-man's cancellation mechanism */
  if (me->threadH)
  {
    ResumeThread(me->threadH);
  }
  else
  {
    fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
    ssl = (_amps_SSL*)AMPS_IEX_GET(&me->ssl, 0);
    amps_tcps_set_error(me, "Failed to create thread for receive");
    result = AMPS_E_MEMORY;
    goto error;
  }

#else
  rc = pthread_create((pthread_t*)(&me->thread), NULL,
                      &amps_tcps_threaded_reader, me);
  if (rc != 0)
  {
    fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
    ssl = AMPS_IEX_GET(&me->ssl, 0);
    amps_tcps_set_error(me, "Failed to create thread for receive");
    result = AMPS_E_MEMORY;
    goto error;
  }
#endif
  for (i = 0; i < AMPS_THREAD_START_TIMEOUT && me->threadCreatedCallbackResult == -1; ++i)
  {
    AMPS_SLEEP(4);
  }
  result = (amps_result)me->threadCreatedCallbackResult;
  if (me->threadCreatedCallbackResult == -1)
  {
    amps_tcps_set_error(me, "Thread created callback failed to return in a timely manner or returned -1.");
    result = AMPS_E_MEMORY;
  }
  if (result != AMPS_E_OK)
  {
    fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
    ssl = (_amps_SSL*)AMPS_IEX_GET(&me->ssl, 0);
    amps_tcps_set_error(me, "Thread creation callback failed");
  }

error:
  if (result != AMPS_E_OK)
  {
    if (ssl)
    {
      rc = _amps_SSL_shutdown(ssl);
#ifdef _WIN32
      sysError = GetLastError();
#else
      sysError = errno;
#endif
      if (rc < 0)
      {
        amps_tcps_set_ssl_error(me, rc, sysError);
      }
    }
    if (fd != AMPS_INVALID_SOCKET)
    {
      shutdown(fd, SHUT_RDWR);
      closesocket(fd);
    }
    if (ssl)
    {
      _amps_SSL_free(ssl);
      ssl = 0;
    }
#ifdef _WIN32
    me->thread = 0;
#else
    me->thread = (pthread_t)0;
#endif
  }
  free(protocol);
  free(host);
  free(port);
  AMPS_UNLOCK(&me->sendLock);
  AMPS_UNLOCK(&me->lock);
#ifndef _WIN32
  pthread_cleanup_pop(0);
  pthread_cleanup_pop(0);
#endif
  return result;

}

amps_result amps_tcps_set_receiver(amps_handle transport, amps_handler receiver, void* userData)
{
  amps_tcps_t* me = (amps_tcps_t*)transport;
  me->messageHandlerUserData = userData;
  me->messageHandler = receiver;
  return AMPS_E_OK;
}

amps_result amps_tcps_set_predisconnect(amps_handle transport,
                                        amps_predisconnect_handler receiver,
                                        void* userData)
{
  amps_tcps_t* me = (amps_tcps_t*)transport;
  me->predisconnectHandlerUserData = userData;
  me->predisconnectHandler = receiver;
  return AMPS_E_OK;
}

amps_result amps_tcps_set_disconnect(amps_handle transport, amps_handler receiver, void* userData)
{
  amps_tcps_t* me = (amps_tcps_t*)transport;
  me->disconnectHandlerUserData = userData;
  me->disconnectHandler = receiver;
  return AMPS_E_OK;
}

void amps_tcps_close(amps_handle transport)
{
  AMPS_JOIN_DECLARE();
  amps_tcps_t* me = (amps_tcps_t*)transport;
  AMPS_SOCKET fd = AMPS_INVALID_SOCKET;
  _amps_SSL* ssl = 0;
  AMPS_IEX_LONG(&me->disconnecting, 1);
  AMPS_LOCK(&me->sendLock);
#ifndef _WIN32
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me->sendLock);
#endif
  fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
  ssl = (_amps_SSL*)AMPS_IEX_GET(&me->ssl, 0);
  AMPS_UNLOCK(&me->sendLock);
#ifndef _WIN32
  pthread_cleanup_pop(0);
#endif
  AMPS_SPIN_LOCK_UNLIMITED(&me->lock);
#ifndef _WIN32
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me->lock);
#endif
  if (ssl)
  {
    int rc = _amps_SSL_shutdown(ssl);
#ifdef _WIN32
    int sysError = GetLastError();
#else
    int sysError = errno;
#endif
    if (rc == 0 && fd != AMPS_INVALID_SOCKET)
    {
      // attempt to wait for the server to send back the matching close notify.
      struct timeval tv;
      fd_set readfds, exceptfds;
      tv.tv_sec = 0;
      tv.tv_usec = 10000;
      FD_ZERO(&readfds);
      FD_SET(fd, &readfds);
      FD_ZERO(&exceptfds);
      FD_SET(fd, &exceptfds);
#ifdef _WIN32
      select(1, &readfds, 0, &exceptfds, &tv);
#else
      select(fd + 1, &readfds, 0, &exceptfds, &tv);
#endif
      rc = _amps_SSL_shutdown(ssl);
    }
    if (rc < 0)
    {
      amps_tcps_set_ssl_error(me, rc, sysError);
    }
  }
  if (fd != AMPS_INVALID_SOCKET)
  {
    shutdown(fd, SHUT_RDWR);
    closesocket(fd);
  }
  if (ssl)
  {
    _amps_SSL_free(ssl);
  }
  AMPS_UNLOCK(&me->lock);
#ifndef _WIN32
  pthread_cleanup_pop(0);
#endif
  AMPS_JOIN(me->thread);
}

void amps_tcps_destroy(amps_handle transport)
{
  AMPS_JOIN_DECLARE();
  amps_tcps_t* me = (amps_tcps_t*)transport;
  AMPS_SOCKET fd = AMPS_INVALID_SOCKET;
  _amps_SSL* ssl = 0;
  amps_atfork_remove(me, (_amps_atfork_callback_function)amps_tcps_atfork_handler);
  AMPS_LOCK(&me->sendLock);
#ifndef _WIN32
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me->sendLock);
#endif
  fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
  ssl = (_amps_SSL*)AMPS_IEX_GET(&me->ssl, 0);
  AMPS_UNLOCK(&me->sendLock);
#ifndef _WIN32
  pthread_cleanup_pop(0);
#endif
  AMPS_LOCK(&me->lock);
#ifndef _WIN32
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me->lock);
#endif
#if __STDC_VERSION__ >= 201100
  me->destroying = true;
  me->disconnecting = true;
#else
  AMPS_IEX_LONG(&me->destroying, 1);
  AMPS_IEX_LONG(&me->disconnecting, 1);
#endif
  if (ssl)
  {
    int rc = _amps_SSL_shutdown(ssl);
#ifdef _WIN32
    int sysError = GetLastError();
#else
    int sysError = errno;
#endif
    if (rc == 0 && fd != AMPS_INVALID_SOCKET)
    {
      // attempt to wait for the server to send back the matching close notify.
      struct timeval tv;
      fd_set readfds, exceptfds;
      tv.tv_sec = 0;
      tv.tv_usec = 10000;
      FD_ZERO(&readfds);
      FD_SET(fd, &readfds);
      FD_ZERO(&exceptfds);
      FD_SET(fd, &exceptfds);
#ifdef _WIN32
      select(1, &readfds, 0, &exceptfds, &tv);
#else
      select(fd + 1, &readfds, 0, &exceptfds, &tv);
#endif
      rc = _amps_SSL_shutdown(ssl);
    }
    if (rc < 0)
    {
      amps_tcps_set_ssl_error(me, rc, sysError);
    }
  }
  if (fd != AMPS_INVALID_SOCKET)
  {
    shutdown(fd, SHUT_RDWR);
    closesocket(fd);
  }
  if (ssl)
  {
    _amps_SSL_free(ssl);
  }

  AMPS_UNLOCK(&me->lock);
#ifndef _WIN32
  pthread_cleanup_pop(0);
#endif
  AMPS_JOIN(me->thread);
  AMPS_SLEEP(1);
  free(me->buf);
  /* Hopefully, nobody else is using me right now. */

  AMPS_KILLLOCK(&me->lock);
  AMPS_KILLLOCK(&me->sendLock);

  free(me);
}

/* called with me->lock not already taken */
amps_result amps_tcps_handle_disconnect(
  amps_tcps_t* me, unsigned failedConnectionVersion)
{
  AMPS_SOCKET fd = AMPS_INVALID_SOCKET;
  _amps_SSL* ssl = 0;
#ifndef _WIN32
  int cancelState = 0;
  int unusedCancelState = 0;
#endif
  amps_result result = AMPS_E_OK;
  AMPS_LOCK(&me->sendLock);
#ifndef _WIN32
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me->sendLock);
#endif
  fd = AMPS_IEX_GET(&me->fd, AMPS_INVALID_SOCKET);
  ssl = (_amps_SSL*)AMPS_IEX_GET(&me->ssl, 0);
  AMPS_UNLOCK(&me->sendLock);
#ifndef _WIN32
  pthread_cleanup_pop(0);
#endif
  /* close what we have right now. */
  if (ssl)
  {
    int rc = _amps_SSL_shutdown(ssl);
#ifdef _WIN32
    int sysError = GetLastError();
#else
    int sysError = errno;
#endif
    if (rc == 0 && fd != AMPS_INVALID_SOCKET)
    {
      // attempt to wait for the server to send back the matching close notify.
      struct timeval tv;
      fd_set readfds, exceptfds;
      tv.tv_sec = 0;
      tv.tv_usec = 10000;
      FD_ZERO(&readfds);
      FD_SET(fd, &readfds);
      FD_ZERO(&exceptfds);
      FD_SET(fd, &exceptfds);
#ifdef _WIN32
      select(1, &readfds, 0, &exceptfds, &tv);
#else
      select(fd + 1, &readfds, 0, &exceptfds, &tv);
#endif
      rc = _amps_SSL_shutdown(ssl);
    }
    if (rc < 0)
    {
      amps_tcps_set_ssl_error(me, rc, sysError);
    }
  }
  if (fd != AMPS_INVALID_SOCKET)
  {
    shutdown(fd, SHUT_RDWR);
    closesocket(fd);
  }
  if (ssl)
  {
    _amps_SSL_free(ssl);
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
#ifndef _WIN32
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me->lock);
#endif

  /* if we're being destroyed, get out of here; don't worry about unlocking */
  if (me->destroying)
  {
    result = AMPS_E_DISCONNECTED; return result;
  }

  /* a new connection is available.  let someone else try. */
  if (failedConnectionVersion != me->connectionVersion)
  {
    result = AMPS_E_RETRY;
    goto error;
  }

  /* if we're disconnecting, get out of here; don't reconnect */
  if (me->disconnecting)
  {
    result = AMPS_E_DISCONNECTED;
    goto error;
  }

#ifndef _WIN32
  pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
#endif
  /* Call the disconect handler. */
  result = me->disconnectHandler(me, me->disconnectHandlerUserData);
#ifndef _WIN32
  pthread_setcancelstate(cancelState, &unusedCancelState);
#endif
  if (result == AMPS_E_OK)
  {
    result = AMPS_E_RETRY;
  }

error:
#ifndef _WIN32
  pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
#endif
  AMPS_UNLOCK(&me->lock);
#ifndef _WIN32
  pthread_cleanup_pop(0);
  pthread_setcancelstate(cancelState, &unusedCancelState);
#endif

  return result;
}

void amps_tcps_handle_stream_corruption(
  amps_tcps_t* me, unsigned failedConnectionVersion)
{
  amps_tcps_set_error(me, "The connection appears corrupt.  Disconnecting.");
  amps_tcps_handle_disconnect(me, failedConnectionVersion);
}


amps_result
amps_tcps_send_with_version(amps_handle transport,
                            amps_handle message,
                            unsigned* version_out)
{
  amps_result result = AMPS_E_OK;
  amps_tcps_t* me = (amps_tcps_t*)transport;
  size_t len = 16 * 1024;
  int bytesWritten = -1;
  unsigned int bytesWrittenN = 0;
  int bytesSent = 0;

  *version_out = me->connectionVersion;

  if (me->disconnecting)
  {
    amps_tcps_set_error(me, "Disconnecting.");
    return AMPS_E_DISCONNECTED;
  }

  if (me->fd == AMPS_INVALID_SOCKET)
  {
    amps_tcps_set_error(me, "Not connected.");
    return AMPS_E_DISCONNECTED;
  }

  /* serialize */
  AMPS_LOCK(&me->sendLock);
#ifndef _WIN32
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me->sendLock);
#endif
  if (me->ssl == 0)
  {
    amps_tcps_set_error(me, "Not connected.");
    result = AMPS_E_CONNECTION;
    goto error;
  }
  while (bytesWritten < 0)
  {
    if (me->buf == NULL)
    {
      me->buf = (char*)malloc(len);
      if (me->buf == NULL)
      {
        amps_tcps_set_error(me, "Unable to allocate memory to send message.");
        result = AMPS_E_MEMORY;
        goto error;
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
  me->filterFunction((const unsigned char*)(me->buf + 4), (size_t)bytesWritten, 0, me->filterUserData);
  *((unsigned int*)(me->buf)) = bytesWrittenN;
  bytesWritten += 4;
  /*now, send */
  while (bytesSent < bytesWritten)
  {
    int bytesWrittenThisTime;
    bytesWrittenThisTime = _amps_SSL_write(me->ssl, (me->buf) + bytesSent, bytesWritten - bytesSent);
    if (bytesWrittenThisTime <= 0)
    {
      /* record the error */
      amps_tcps_set_error(me, "The connection is closed.");
      result = AMPS_E_DISCONNECTED;
      goto error;
    }
    bytesSent += bytesWrittenThisTime;
  }
error:
  AMPS_UNLOCK(&me->sendLock);
#ifndef _WIN32
  pthread_cleanup_pop(0);
#endif

  return result;
}

amps_result
amps_tcps_send(amps_handle transport,
               amps_handle message)
{
  unsigned version_out;
  return amps_tcps_send_with_version(transport, message, &version_out);
}

/* Returns # bytes read, 0 if a timeout occurred without a read,
 * and <0 if an error occurred.
 */
int amps_tcps_locked_read(amps_tcps_t* me_,
                          char* readPoint_,
                          int bytes_)
{
  int rc = 0;
  int errorcode = 0;
  int fd = (int)me_->fd;
  int locked = 0;

  while (locked == 0)
  {
    if (me_->disconnecting || fd != me_->fd || !me_->ssl
#ifdef _WIN32
        || me_->thread != GetCurrentThreadId()  /* our cancellation mechanism on win32 */
#else
        || me_->thread != pthread_self()
#endif
        || me_->destroying || fd == AMPS_INVALID_SOCKET)
    {
      return 0;
    }
    locked = (int)AMPS_SPIN_LOCK(&me_->sendLock);
  }
  if (me_->disconnecting || fd != me_->fd || !me_->ssl
#ifdef _WIN32
      || me_->thread != GetCurrentThreadId()  /* our cancellation mechanism on win32 */
#else
      || me_->thread != pthread_self()
#endif
      || me_->destroying || fd == AMPS_INVALID_SOCKET)
  {
    AMPS_UNLOCK(&me_->sendLock);
    return 0;
  }
  // Use select to wait for data if none already exists on the ssl.
  if (!_amps_SSL_pending(me_->ssl))
  {
    struct timeval tv;
    fd_set readfds, exceptfds;
    tv.tv_sec = 0;
    tv.tv_usec = 1000;
    FD_ZERO(&readfds);
    FD_SET(fd, &readfds);
    FD_ZERO(&exceptfds);
    FD_SET(fd, &exceptfds);
    AMPS_UNLOCK(&me_->sendLock);
#ifdef _WIN32
    if (select(1, &readfds, 0, &exceptfds, &tv) == 0)
#else
    if (select(fd + 1, &readfds, 0, &exceptfds, &tv) == 0)
#endif
    {
      return 0;
    }
    AMPS_LOCK(&me_->sendLock);
    if (me_->disconnecting || fd != me_->fd || !me_->ssl
#ifdef _WIN32
        || me_->thread != GetCurrentThreadId()  /* our cancellation mechanism on win32 */
#else
        || me_->thread != pthread_self()
#endif
        || me_->destroying || fd == AMPS_INVALID_SOCKET)
    {
      AMPS_UNLOCK(&me_->sendLock);
      return 0;
    }
  }

#ifndef _WIN32
  pthread_cleanup_push(amps_cleanup_unlock_mutex, (void*)&me_->sendLock);
#endif
  rc = _amps_SSL_read(me_->ssl, readPoint_, bytes_);
  errorcode = rc <= 0 ? _amps_SSL_get_error(me_->ssl, rc) : 0;

  AMPS_UNLOCK(&me_->sendLock);
#ifndef _WIN32
  pthread_cleanup_pop(0);
#endif

  if (rc <= 0)
  {
    if (errorcode == AMPS_SSL_ERROR_WANT_READ)
    {
      return 0;
    }
    // End of stream
    return -1;
  }
  return rc;
}

#ifdef _WIN32
DWORD amps_tcps_threaded_reader(void* userData)
{
  DWORD self = GetCurrentThreadId();
  HANDLE selfH = GetCurrentThread();
  amps_tcps_t* me = (amps_tcps_t*)userData;
  if (me->threadCreatedCallback)
  {
    amps_result result = me->threadCreatedCallback(selfH,
                                                   me->threadCreatedCallbackUserData);
    AMPS_IEX(&me->threadCreatedCallbackResult, (long)result);
    if (result != AMPS_E_OK)
    {
      return (DWORD)0;
    }
  }
  else
  {
    AMPS_IEX(&me->threadCreatedCallbackResult, (long)AMPS_E_OK);
  }
#else
void* amps_tcps_threaded_reader(void* userData)
{
  int unusedCancelState = 0;
  amps_tcps_t* me = (amps_tcps_t*)userData;
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
#endif
  unsigned char* buffer, *newBuffer, *end, *readPoint, *parsePoint;

  unsigned int msglenN;
  unsigned long msglen, currentPosition, bytesRead = 0;
  int received;
  int cancelState = 0;
  const size_t BUFFER_SIZE = 16 * 1024;
  const unsigned MAX_MESSAGE_SIZE = 1024 * 1024 * 1024;
  amps_uint64_t lastReadTime = 0;
  amps_uint64_t lastIdleTime = 0;
  amps_handle   message;

  /* capture the connection version we are using now. */
  unsigned connectionVersion = me->connectionVersion;
  AMPS_SOCKET fd = me->fd;

  lastReadTime = amps_now();
  lastIdleTime = lastReadTime;

#ifndef _WIN32
  pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
#endif
  message = amps_message_create(NULL);
  buffer = (unsigned char*)malloc(BUFFER_SIZE);
#ifndef _WIN32
  pthread_cleanup_push(amps_message_destroy, (void*)message);
  pthread_cleanup_push(amps_cleanup_free_buffer, (void*)(&buffer));
  pthread_setcancelstate(cancelState, &unusedCancelState);
#endif
  if (!buffer)
  {
    amps_tcps_handle_disconnect(me, connectionVersion);
    goto cleanup;
  }

  end = buffer + BUFFER_SIZE;
  readPoint = buffer;
  parsePoint = buffer;

  /* while we're open and not disconnecting */
  while (connectionVersion == me->connectionVersion
         && !me->disconnecting
         && fd == me->fd
#ifdef _WIN32
         && me->thread == GetCurrentThreadId()  /* our cancellation mechanism on win32 */
#else
         && me->thread == pthread_self()
#endif
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
#ifndef _WIN32
      cancelState = 0;
      pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
#endif
      me->messageHandler(0L,
                         me->messageHandlerUserData);
#ifndef _WIN32
      pthread_setcancelstate(cancelState, &unusedCancelState);
#endif
    }

    received = amps_tcps_locked_read(me, (char*) readPoint, (int)(end - readPoint));

    if (received > 0)
    {
      /* Call filter function without the 4 byte size */
      me->filterFunction(readPoint, (size_t)received, 1, me->filterUserData);
      readPoint += received;
      lastReadTime = amps_now();
    }
    else if (received < 0)
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
        if (!me->disconnecting)
        {
          amps_tcps_handle_disconnect(me, connectionVersion);
        }
        goto cleanup;
      }
      continue;
    }
    else
    {
      continue;
    }

    while (readPoint >= parsePoint + 4 &&
           fd == me->fd &&
           me->connectionVersion == connectionVersion)
    {
      msglenN = *(unsigned int*)parsePoint;
      msglen = ntohl(msglenN);
      if (msglen > MAX_MESSAGE_SIZE)
      {
        amps_tcps_handle_stream_corruption(me, connectionVersion);
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
          while (currentPosition < msglen &&
                 fd == me->fd &&
                 me->connectionVersion == connectionVersion)
          {
            if (amps_message_deserialize(message, me->serializer,
                                         currentPosition,
                                         &bytesRead) != AMPS_E_OK)
            {
              amps_tcps_handle_stream_corruption(me, connectionVersion);
              goto cleanup;
            }
            if (me->messageHandler)
            {
#ifndef _WIN32
              cancelState = 0;
              pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
#endif
              me->messageHandler(message,
                                 me->messageHandlerUserData);
#ifndef _WIN32
              pthread_setcancelstate(cancelState, &unusedCancelState);
#endif
            }
            currentPosition += bytesRead;
          }
        }
        else
        {
          amps_tcps_handle_stream_corruption(me, connectionVersion);
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
        newBuffer = (unsigned char*)malloc( newLength );
        if (newBuffer == NULL)
        {
          assert(readPoint >= parsePoint);
          /* stream broken */
          amps_tcps_handle_disconnect(me, connectionVersion);
          goto cleanup;
        }
        memcpy(newBuffer, parsePoint, (size_t)(readPoint - parsePoint));
        readPoint = newBuffer + (readPoint - parsePoint);
        parsePoint = newBuffer;
#ifndef _WIN32
        cancelState = 0;
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
#endif
        free(buffer);
        buffer = newBuffer;
#ifndef _WIN32
        pthread_setcancelstate(cancelState, &unusedCancelState);
#endif
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
  } /* while(fd == me->fd) */
cleanup:
#ifndef _WIN32
  cancelState = 0;
  pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancelState);
  pthread_cleanup_pop(0);
  pthread_cleanup_pop(0);
#endif
  amps_message_destroy(message);
  free(buffer);
  if (me->destroying)
  {
    return 0;
  }
#ifndef _WIN32
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
#endif

  return 0;
}

amps_result
amps_tcps_attempt_reconnect(amps_handle transport, unsigned version)
{
  amps_result res;
  amps_tcps_t* me = (amps_tcps_t*)transport;
  if (version == 0)
  {
    version = me->connectionVersion;
  }
  res = amps_tcps_handle_disconnect(me, version);
  return res;
}

/* public-api -- get a socket */
AMPS_SOCKET
amps_tcps_get_socket(amps_handle transport)
{
  amps_tcps_t* me = (amps_tcps_t*)transport;
  return me->fd;
}

void* amps_tcps_get_SSL(amps_handle transport)
{
  amps_tcps_t* me = (amps_tcps_t*)transport;
  return me->ssl;
}

amps_result
amps_tcps_update_read_timeout(amps_tcps_t* me)
{
  int timeout = (int)((me->readTimeoutMillis && me->idleTimeMillis) ?
                      AMPS_MIN(me->readTimeoutMillis, me->idleTimeMillis) :
                      AMPS_MAX(me->readTimeoutMillis, me->idleTimeMillis));
  int rc = 0;
#ifdef _WIN32
  DWORD timeoutMillis = timeout;
  rc = setsockopt(me->fd, SOL_SOCKET, SO_RCVTIMEO,
                  (const char*)&timeoutMillis, sizeof(DWORD));
  int sysError = GetLastError();
#else
  struct timeval tv;
  tv.tv_sec = timeout / 1000;
  tv.tv_usec = (timeout % 1000) * 1000;
  rc = setsockopt(me->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(struct timeval));
  int sysError = errno;
#endif
  if (rc == -1)
  {
    amps_tcps_set_socket_error(me, sysError);
    return AMPS_E_USAGE;
  }
  return AMPS_E_OK;
}
amps_result
amps_tcps_set_read_timeout(amps_handle transport, int readTimeout)
{
  amps_tcps_t* me = (amps_tcps_t*)transport;
  me->readTimeoutMillis = (amps_uint64_t)readTimeout * 1000;
  return amps_tcps_update_read_timeout(me);
}


void
amps_tcps_set_filter_function(amps_handle transport, amps_transport_filter_function filterFunction_, void* userdata_)
{
  amps_tcps_t* me = (amps_tcps_t*)transport;
  me->filterUserData = userdata_;
  me->filterFunction = filterFunction_ ? filterFunction_ : amps_tcps_noop_filter_function;
}

void
amps_tcps_set_thread_created_callback(amps_handle transport_, amps_thread_created_callback threadCreatedCallback_, void* userdata_)
{
  amps_tcps_t* me = (amps_tcps_t*)transport_;
  me->threadCreatedCallbackUserData = userdata_;
  me->threadCreatedCallback = threadCreatedCallback_;
}

amps_result
amps_tcps_set_idle_time(amps_handle transport, int millis)
{
  amps_tcps_t* me = (amps_tcps_t*)transport;
  if (!me->idleTimeMillis || (amps_uint64_t)millis < me->idleTimeMillis)
  {
    me->idleTimeMillis = (amps_uint64_t)millis;
  }
  return amps_tcps_update_read_timeout(me);
}
