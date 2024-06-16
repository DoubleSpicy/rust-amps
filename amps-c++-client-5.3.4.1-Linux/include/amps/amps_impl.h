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
////////////////////////////////////////////////////////////////////////// */
#ifndef _AMPS_IMPL_H_
#define _AMPS_IMPL_H_
#ifndef _WIN32
  #define _GNU_SOURCE 1
#endif
#define _AMPS_BUILD_C_CLIENT 1
#include <amps/amps.h>
#include <assert.h>
#include <stdlib.h>

#define AMPSASSERT assert
#define AMPS_THREAD_START_TIMEOUT 120

#ifdef _WIN32

  #include <Windows.h>
  #define _AMPS_SNPRINTF(buf_, count_, ...) _snprintf_s(buf_, count_, _TRUNCATE, __VA_ARGS__)
  #define _AMPS_KEYCREATE TlsAlloc
  #define _AMPS_TLS_KEY DWORD
  char* _amps_strndup(const char* ptr_, size_t len_);
  #define _AMPS_STRNDUP _amps_strndup

#else
  #include <pthread.h>
  #include <unistd.h>
  #include <sys/socket.h>
  #include <netinet/in.h>
  #include <netinet/tcp.h>
  #include <arpa/inet.h>
  #define _AMPS_SNPRINTF snprintf
  #define _AMPS_KEYCREATE pthread_key_create
  #define _AMPS_TLS_KEY pthread_key_t
  #define closesocket close
  #define _AMPS_STRNDUP strndup

  #ifndef SO_DOMAIN
    #define AMPS_SO_DOMAIN 39
  #else
    #define AMPS_SO_DOMAIN (SO_DOMAIN)
  #endif

#endif

#define AMPS_MIN(x,y) ((x)<(y)?(x):(y))
#define AMPS_MAX(x,y) ((x)>(y)?(x):(y))

typedef enum
{
  AMPS_PREFER_IPV4,
  AMPS_PREFER_IPV6
}
amps_ip_protocol_preference;
#define AMPS_DEFAULT_IP_PROTOCOL_PREFERENCE AMPS_PREFER_IPV4

typedef struct
{
  char clientName[128];
  size_t transportType;
  amps_handle transport;
  char lastError[1024];

  amps_predisconnect_handler predisconnectHandler;
  void* predisconnectHandlerUserData;
  amps_handler disconnectHandler;
  void* disconnectHandlerUserData;

  amps_handler messageHandler;
  void* messageHandlerUserData;

  amps_transport_filter_function transportFilterFunction;
  void* transportFilterFunctionUserData;

  amps_thread_created_callback threadCreatedCallback;
  void* threadCreatedCallbackUserData;
}
amps_client_t;

typedef struct
{
  char name[8];
  amps_handle(*createFunc)(void);
  amps_result(*connectFunc)(amps_handle, const amps_char*);
  amps_result(*predisconnectFunc)(amps_handle, amps_predisconnect_handler, void*);
  amps_result(*disconnectFunc)(amps_handle, amps_handler, void*);
  amps_result(*receiveFunc)(amps_handle, amps_handler, void*);
  amps_result(*sendFunc)(amps_handle, amps_handle);
  amps_result(*sendWithVersionFunc)(amps_handle, amps_handle, unsigned*);
  const amps_char* (*getError)(amps_handle);
  void(*closeFunc)(amps_handle);
  void(*destroyFunc)(amps_handle);
  amps_result(*reconnectFunc)(amps_handle, unsigned);
  amps_result(*setReadTimeoutFunc)(amps_handle, int);
  amps_result(*setIdleTimeFunc)(amps_handle, int);
  void(*setTransportFilterFunc)(amps_handle, amps_transport_filter_function, void*);
  void(*setThreadCreatedCallback)(amps_handle, amps_thread_created_callback, void*);
  AMPS_SOCKET(*getSocket)(amps_handle);
} transport_entry_t;

extern transport_entry_t g_transports[];
extern size_t g_transport_count;
typedef struct
{
  char* begin;
  size_t length;
  short owner;
  size_t capacity;
} amps_field_t;


typedef struct
{
  const char* rawBuffer;
  size_t length;
  size_t capacity;
  unsigned long long bitmask;
  amps_field_t fields[MESSAGEFIELDS];
  amps_field_t data;
} amps_message_t;

typedef struct
{
  char name[8];
  int(*serializeFunc)(amps_handle message, amps_char* buffer, size_t length);
  amps_result(*preDeserializeFunc)(amps_handle message, const amps_char* buffer, size_t length);
  amps_result(*deserializeFunc)(amps_handle message, size_t startingPosition, unsigned long* bytesRead);
} protocol_entry_t;
extern protocol_entry_t g_message_protocols[];
extern size_t g_message_protocol_count;



/**
 *  returns an integer form of the protocol specified.
 *  \returns -1 if the message type is unknown.
 *  \param protocolname the null terminated protocol name to look up.
 */
amps_int64_t amps_message_get_protocol(
  const amps_char* protocolname);

int amps_message_serialize(
  amps_handle message,
  amps_int64_t serializer,
  amps_char* buffer,
  size_t length);

amps_result amps_message_pre_deserialize(
  amps_handle message,
  amps_int64_t serializer,
  const amps_char* buffer,
  size_t length);

amps_result amps_message_deserialize(
  amps_handle message,
  amps_int64_t serializer,
  size_t startingPosition,
  unsigned long* bytesRead);


/**
 * "amps" protocol function prototypes
 */
amps_result amps_protocol_pre_deserialize(amps_handle message, const amps_char* buffer, size_t length);
amps_result amps_protocol_deserialize(amps_handle message, size_t startingPosition, unsigned long* bytesRead);
int amps_protocol_serialize(amps_handle message, amps_char* buffer, size_t length);

#ifndef _WIN32
  int amps_spin_lock_counted(pthread_mutex_t* lock_);
  void amps_spin_lock_unlimited(pthread_mutex_t* lock_);
  void amps_cleanup_unlock_mutex(void* m);
  void amps_cleanup_free_buffer(void* data);
  void amps_mutex_pair_atfork(void* vpMutex_, int code_);
#endif

typedef void(*_amps_atfork_callback_function)(void*, int);
AMPSDLL void amps_atfork_init(void);
AMPSDLL void amps_atfork_add(void*, _amps_atfork_callback_function);
AMPSDLL void amps_atfork_remove(void*, _amps_atfork_callback_function);


#endif
