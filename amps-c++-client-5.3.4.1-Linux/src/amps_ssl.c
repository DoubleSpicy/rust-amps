/** ////////////////////////////////////////////////////////////////////////
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


#ifndef _WIN32
  #include <dlfcn.h>
#endif


#include <amps/amps_impl.h>
#include <amps/amps_ssl.h>
#include <signal.h>
#include <stdio.h>


/*
 * This is a small dynamic loader for the OpenSSL shared library
 * (or DLLs on Windows). When amps_ssl_init() is called, we attempt
 * to locate the specified shared library, or see if it is already loaded,
 * and then look for and save all the symbols we need from OpenSSL
 * in order for the tcps transport to function. The address of
 * each OpenSSL function we load is saved in a global of the same name,
 * with "_amps_" prepended. That means to call "SSL_CTX_new" from OpenSSL,
 * for example, use "_amps_SSL_CTX_new".
 */

/* depending on compiler settings, may not be defined simply
 * by including dlfcn.h. */
#ifdef RTLD_DEFAULT
  #define AMPS_RTLD_DEFAULT RTLD_DEFAULT
#else
  #define AMPS_RTLD_DEFAULT ((void*)0)
#endif

/* function pointers for openssl functions */
void              (*_amps_SSL_library_init)(void);
void              (*_amps_SSL_load_error_strings)(void);
unsigned long     (*_amps_ERR_get_error)(void);
void              (*_amps_ERR_error_string_n)(unsigned long, char*, size_t);
void              (*_amps_ERR_clear_error)(void);
int               (*_amps_CRYPTO_num_locks)(void);
void              (*_amps_CRYPTO_set_locking_callback)( void(*)(int, int, const char*, int) );
_amps_SSL_METHOD* (*_amps_SSLv23_client_method)(void);
_amps_SSL_METHOD* (*_amps_TLS_client_method)(void);
_amps_SSL_CTX*    (*_amps_SSL_CTX_new)(const _amps_SSL_METHOD*);
void              (*_amps_SSL_CTX_free)(_amps_SSL_CTX*);
void              (*_amps_SSL_CTX_set_verify)(_amps_SSL_CTX*, int, void*);
int               (*_amps_SSL_CTX_load_verify_locations)(_amps_SSL_CTX*, const char*, const char*);
_amps_SSL*        (*_amps_SSL_new)(_amps_SSL_CTX*);
int               (*_amps_SSL_set_fd)(_amps_SSL*, int);
int               (*_amps_SSL_get_error)(_amps_SSL*, int);
int               (*_amps_SSL_connect)(_amps_SSL*);
int               (*_amps_SSL_ctrl)(_amps_SSL*, int, long, void*);
int               (*_amps_SSL_read)(_amps_SSL*, void*, int);
int               (*_amps_SSL_write)(_amps_SSL*, const void*, int);
int               (*_amps_SSL_shutdown)(_amps_SSL*);
void              (*_amps_SSL_free)(_amps_SSL*);
int               (*_amps_SSL_pending)(_amps_SSL*);


_amps_SSL_CTX* _amps_ssl_ctx = 0L;

#define ERROR_BUFFER_LENGTH 256
char           _amps_ssl_initialization_error[ERROR_BUFFER_LENGTH];

#ifdef _WIN32
typedef int (*local_proc)(void);
HMODULE _amps_ssl_libeay_handle;
HMODULE _amps_ssl_ssleay_handle;
#define AMPS_strcpy strcpy_s

void _amps_get_last_error(char* buffer, size_t length)
{
  DWORD lastError = 0;
  lastError = GetLastError();
  lastError = FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM,
                             NULL,
                             lastError,
                             MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                             buffer,
                             ERROR_BUFFER_LENGTH,
                             NULL);
  if (lastError == 0)
  {
    FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM,
                   NULL,
                   GetLastError(),
                   MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                   buffer,
                   ERROR_BUFFER_LENGTH,
                   NULL);
  }
}
#define LOAD_AMPS_LIBEAY_FUNCTION(name)                                         \
  *((FARPROC*)&_amps_##name) = GetProcAddress(_amps_ssl_libeay_handle,#name);   \
  if(!_amps_##name)                                                             \
  {                                                                             \
    _amps_get_last_error(errorBuffer, ERROR_BUFFER_LENGTH);                   \
    _AMPS_SNPRINTF(_amps_ssl_initialization_error,                            \
                   ERROR_BUFFER_LENGTH,                                      \
                   "Error locating SSL procedure %s: %s",                    \
                   #name,                                                    \
                   errorBuffer);                                             \
    return -1;                                                                \
  }
#define LOAD_AMPS_SSLEAY_FUNCTION(name)                                         \
  *((FARPROC*)&_amps_##name) = GetProcAddress(_amps_ssl_ssleay_handle,#name);   \
  if(!_amps_##name)                                                             \
  {                                                                             \
    _amps_get_last_error(errorBuffer, ERROR_BUFFER_LENGTH);                   \
    _AMPS_SNPRINTF(_amps_ssl_initialization_error,                            \
                   ERROR_BUFFER_LENGTH,                                       \
                   "Error locating SSL procedure %s: %s",                     \
                   #name,                                                     \
                   errorBuffer);                                              \
    return -1;                                                                \
  }
#define LOAD_AMPS_LIBEAY_FUNCTION_OPTIONAL(name)                                \
  *((FARPROC*)&_amps_##name) = GetProcAddress(_amps_ssl_libeay_handle,#name);

#define LOAD_AMPS_SSLEAY_FUNCTION_OPTIONAL(name)                                \
  *((FARPROC*)&_amps_##name) = GetProcAddress(_amps_ssl_ssleay_handle,#name);

int _amps_ssl_load(const char* dllPath_)
{
  char errorBuffer[ERROR_BUFFER_LENGTH];
  DWORD lastError = 0;
  _amps_ssl_initialization_error[0] = '\0';
  if (dllPath_)
  {
    /* attempt to locate via GetModuleHandle since it
     *  may already be loaded
     */
    _amps_ssl_ssleay_handle = (HMODULE) GetModuleHandle(dllPath_);
    if (!_amps_ssl_ssleay_handle)
    {
      _amps_ssl_ssleay_handle = (HMODULE) LoadLibraryExA(dllPath_, NULL, LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_DEFAULT_DIRS);
      if (!_amps_ssl_ssleay_handle)
      {
        _amps_get_last_error(errorBuffer, ERROR_BUFFER_LENGTH);
        _AMPS_SNPRINTF(_amps_ssl_initialization_error, ERROR_BUFFER_LENGTH,
                       "Error opening SSL dll: %s",
                       errorBuffer);
        return -1;
      }
    }
  }
  else
  {
#ifdef AMPS_LESS_SECURE_LOAD_SSLEAY32
    /* NOTE: this is off by default; that means you have to
     *       specify a ssleay32.dll path or at least filename,
     *       we won't default to looking around ourselves.
     */
    _amps_ssl_ssleay_handle = LoadLibrary("ssleay32.dll");
    if (!_amps_ssl_ssleay_handle)
    {
      _amps_ssl_ssleay_handle = LoadLibrary("libssl-1_1-x64.dll");
    }
    if (!_amps_ssl_ssleay_handle)
    {
      _amps_ssl_ssleay_handle = LoadLibrary("libssl-1_1-x86.dll");
    }
#else
    _amps_ssl_ssleay_handle = GetModuleHandle("ssleay32.dll");
    if (!_amps_ssl_ssleay_handle)
    {
      _amps_ssl_ssleay_handle = GetModuleHandle("libssl-1_1-x64.dll");
    }
    if (!_amps_ssl_ssleay_handle)
    {
      _amps_ssl_ssleay_handle = GetModuleHandle("libssl-1_1-x86.dll");
    }
#endif
    if (!_amps_ssl_ssleay_handle)
    {
      _AMPS_SNPRINTF(_amps_ssl_initialization_error, ERROR_BUFFER_LENGTH,
                     "AMPS ssl_init() must be called with the path to " \
                     "ssleay32.dll, libssl-1_1-x64.dll, or libssl-1_1-x86.dll,"
                     " or one must be previously loaded.");
      return -1;
    }
  }

  /* Should have been loaded by ssleay32.dll loading if necessary */
  _amps_ssl_libeay_handle = GetModuleHandle("libeay32.dll");
  if (!_amps_ssl_libeay_handle)
  {
    _amps_ssl_libeay_handle = GetModuleHandle("libcrypto-1_1-x64.dll");
  }

  if (!_amps_ssl_libeay_handle)
  {
    _amps_ssl_libeay_handle = GetModuleHandle("libcrypto-1_1-x86.dll");
  }

  if (!_amps_ssl_libeay_handle)
  {
    _AMPS_SNPRINTF(_amps_ssl_initialization_error, ERROR_BUFFER_LENGTH,
                   "The OpenSSL modules libeay32.dll, libcrypto-1_1-x64.dll, libcrypto-1_1-x86.dll"
                   " were not loaded.");
    return -1;
  }

  LOAD_AMPS_SSLEAY_FUNCTION_OPTIONAL(SSL_library_init);
  LOAD_AMPS_SSLEAY_FUNCTION_OPTIONAL(SSL_load_error_strings);
  LOAD_AMPS_LIBEAY_FUNCTION(ERR_get_error);
  LOAD_AMPS_LIBEAY_FUNCTION(ERR_error_string_n);
  LOAD_AMPS_LIBEAY_FUNCTION(ERR_clear_error);
  LOAD_AMPS_LIBEAY_FUNCTION_OPTIONAL(CRYPTO_num_locks);
  LOAD_AMPS_LIBEAY_FUNCTION_OPTIONAL(CRYPTO_set_locking_callback);
  LOAD_AMPS_SSLEAY_FUNCTION_OPTIONAL(SSLv23_client_method);
  LOAD_AMPS_SSLEAY_FUNCTION_OPTIONAL(TLS_client_method);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_CTX_new);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_CTX_free);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_CTX_set_verify);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_CTX_load_verify_locations);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_new);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_get_error);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_ctrl);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_set_fd);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_connect);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_read);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_write);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_shutdown);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_free);
  LOAD_AMPS_SSLEAY_FUNCTION(SSL_pending);

  return 0;
}

#else

void*          _amps_ssl_library_handle = 0;

#define AMPS_strcpy(x, y, z) strcpy(x, z)
#define LOAD_AMPS_SSL_FUNCTION_OPTIONAL(name)           \
  _amps_##name = dlsym(_amps_ssl_library_handle,#name);

#define LOAD_AMPS_SSL_FUNCTION(name)                    \
  _amps_##name = dlsym(_amps_ssl_library_handle,#name); \
  if(!_amps_##name)                                     \
  {                                                     \
    _AMPS_SNPRINTF(_amps_ssl_initialization_error,    \
                   ERROR_BUFFER_LENGTH,               \
                   "Error loading SSL module: %s",    \
                   dlerror());                        \
    return -1;                                        \
  }

int _amps_ssl_load(const char* dllPath_)
{
  _amps_ssl_initialization_error[0] = '\0';
  if (dllPath_)
  {
    _amps_ssl_library_handle = dlopen(dllPath_, RTLD_LOCAL | RTLD_LAZY);
    if (!_amps_ssl_library_handle)
    {
      _AMPS_SNPRINTF(_amps_ssl_initialization_error, // -V576
                     ERROR_BUFFER_LENGTH,
                     "Error loading SSL module: %s",
                     dlerror());
      return -1;
    }
  }
  else
  {
    _amps_ssl_library_handle = AMPS_RTLD_DEFAULT;
  }

  LOAD_AMPS_SSL_FUNCTION_OPTIONAL(SSL_library_init);
  LOAD_AMPS_SSL_FUNCTION_OPTIONAL(SSL_load_error_strings);
  LOAD_AMPS_SSL_FUNCTION(ERR_get_error);
  LOAD_AMPS_SSL_FUNCTION(ERR_error_string_n);
  LOAD_AMPS_SSL_FUNCTION(ERR_clear_error);
  LOAD_AMPS_SSL_FUNCTION_OPTIONAL(CRYPTO_num_locks);
  LOAD_AMPS_SSL_FUNCTION_OPTIONAL(CRYPTO_set_locking_callback);
  LOAD_AMPS_SSL_FUNCTION_OPTIONAL(SSLv23_client_method);
  LOAD_AMPS_SSL_FUNCTION_OPTIONAL(TLS_client_method);
  LOAD_AMPS_SSL_FUNCTION(SSL_CTX_new);
  LOAD_AMPS_SSL_FUNCTION(SSL_CTX_free);
  LOAD_AMPS_SSL_FUNCTION(SSL_CTX_set_verify);
  LOAD_AMPS_SSL_FUNCTION(SSL_CTX_load_verify_locations);
  LOAD_AMPS_SSL_FUNCTION(SSL_new);
  LOAD_AMPS_SSL_FUNCTION(SSL_ctrl);
  LOAD_AMPS_SSL_FUNCTION(SSL_get_error);
  LOAD_AMPS_SSL_FUNCTION(SSL_set_fd);
  LOAD_AMPS_SSL_FUNCTION(SSL_connect);
  LOAD_AMPS_SSL_FUNCTION(SSL_read);
  LOAD_AMPS_SSL_FUNCTION(SSL_write);
  LOAD_AMPS_SSL_FUNCTION(SSL_shutdown);
  LOAD_AMPS_SSL_FUNCTION(SSL_free);
  LOAD_AMPS_SSL_FUNCTION(SSL_pending);

  return 0;
}
#endif

/****************** THREADING SUPPORT *********************/
#ifdef _WIN32
  CRITICAL_SECTION* _amps_ssl_mutexes;
#else
  pthread_mutex_t*  _amps_ssl_mutexes;
#endif
size_t            _amps_ssl_mutex_count;

void amps_ssl_locking_callback(int mode_, int n_, const char* filename_, int line_)
{
  if (mode_ & AMPS_CRYPTO_LOCK) /* CRYPTO_LOCK == 0x01 */
  {
#ifdef _WIN32
    EnterCriticalSection(_amps_ssl_mutexes + n_);
#else
    pthread_mutex_lock(_amps_ssl_mutexes + n_);
#endif
  }
  else
  {
#ifdef _WIN32
    LeaveCriticalSection(_amps_ssl_mutexes + n_);
#else
    pthread_mutex_unlock(_amps_ssl_mutexes + n_);
#endif
  }
}

void amps_ssl_setup_threading_functions(void)
{
  size_t i = 0;

  _amps_ssl_mutex_count = (size_t)_amps_CRYPTO_num_locks();

#ifdef _WIN32
  _amps_ssl_mutexes = (CRITICAL_SECTION*)malloc (_amps_ssl_mutex_count * sizeof(CRITICAL_SECTION));
  if (!_amps_ssl_mutexes)
  {
    return;
  }
  for (i = 0; i < _amps_ssl_mutex_count; ++i)
  {
    InitializeCriticalSection( _amps_ssl_mutexes + i);
  }
#else
  _amps_ssl_mutexes = malloc( _amps_ssl_mutex_count * sizeof(pthread_mutex_t) );
  if (!_amps_ssl_mutexes)
  {
    return;
  }
  for (i = 0; i < _amps_ssl_mutex_count; ++i)
  {
    pthread_mutex_init( _amps_ssl_mutexes + i, NULL );
  }
#endif

  _amps_CRYPTO_set_locking_callback(amps_ssl_locking_callback);
}
/**********************************************************/

void _amps_ssl_set_error_from_stack(const char* defaultMsg_)
{
  size_t errorCode = 0;
  errorCode = _amps_ERR_get_error();
  if (errorCode)
  {
    _amps_ERR_error_string_n((unsigned long)errorCode,
                             _amps_ssl_initialization_error,
                             ERROR_BUFFER_LENGTH);
  }
  else
  {
    AMPS_strcpy(_amps_ssl_initialization_error, ERROR_BUFFER_LENGTH,
                defaultMsg_);
  }
}

AMPSDLL int amps_ssl_init(const char* dllPath_)
{
  const void* isOpenSSLv10x = NULL;
  _amps_SSL_METHOD* sslMethod = 0L;
  if (_amps_ssl_load(dllPath_) != 0)
  {
    return -1;
  }
  isOpenSSLv10x = _amps_SSL_library_init;

  if (isOpenSSLv10x)
  {
    if (!_amps_SSL_library_init || !_amps_CRYPTO_set_locking_callback ||
        !_amps_SSLv23_client_method)
    {
      _AMPS_SNPRINTF(_amps_ssl_initialization_error,
                     ERROR_BUFFER_LENGTH,
                     "Unable to load SSL module; v1.0 functions missing.");
      return -1;
    }
    _amps_SSL_load_error_strings();
    _amps_SSL_library_init();
    amps_ssl_setup_threading_functions();
    sslMethod = _amps_SSLv23_client_method();
  }
  else
  {
    if (!_amps_TLS_client_method)
    {
      _AMPS_SNPRINTF(_amps_ssl_initialization_error,
                     ERROR_BUFFER_LENGTH,
                     "Unable to load SSL module; v1.1 library does not contain TLS_client_method.");
      return -1;
    }
    sslMethod = _amps_TLS_client_method();
  }

  _amps_ssl_ctx = _amps_SSL_CTX_new(sslMethod);

  if (!_amps_ssl_ctx)
  {
    _amps_ssl_set_error_from_stack("Unknown error creating SSL context.");
    return -1;
  }

#ifndef _WIN32
  /* No SIGPIPE */
  signal(SIGPIPE, SIG_IGN);
#endif

  return 0;
}

AMPSDLL const char* amps_ssl_get_error(void)
{
  return _amps_ssl_initialization_error;
}

AMPSDLL int amps_ssl_set_verify(int mode_)
{
  if (!_amps_ssl_ctx)
  {
    AMPS_strcpy(_amps_ssl_initialization_error, ERROR_BUFFER_LENGTH,
                "amps_ssl_init must have been called successfully before setting this value.");
    return -1;
  }

  _amps_SSL_CTX_set_verify(_amps_ssl_ctx, mode_, 0L);
  return 0;
}

AMPSDLL int amps_ssl_load_verify_locations(const char* caFile_, const char* caPath_)
{
  if (!_amps_ssl_ctx)
  {
    AMPS_strcpy(_amps_ssl_initialization_error, ERROR_BUFFER_LENGTH,
                "amps_ssl_init must have been called successfully before setting this value.");
    return -1;
  }
  if (!_amps_SSL_CTX_load_verify_locations(_amps_ssl_ctx, caFile_, caPath_))
  {
    _amps_ssl_set_error_from_stack("Unknown error setting CA certificate location.");
    return -1;
  }
  return 0;
}

AMPSDLL void amps_ssl_free(void)
{
  _amps_SSL_CTX_free(_amps_ssl_ctx);
  _amps_ssl_ctx = 0;
}
