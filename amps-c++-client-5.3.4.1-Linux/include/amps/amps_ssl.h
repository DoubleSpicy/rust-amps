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

#ifndef _AMPS_AMPS_SSL_H_
#define _AMPS_AMPS_SSL_H_

/*
 * This file contains the small number of type declarations and forward
 * declarations needed to work with OpenSSL.
 */

#define AMPS_SSL_ERROR_NONE                 0
#define AMPS_SSL_ERROR_SSL                  1
#define AMPS_SSL_ERROR_WANT_READ            2
#define AMPS_SSL_ERROR_WANT_WRITE           3
#define AMPS_SSL_ERROR_WANT_X509_LOOKUP     4
#define AMPS_SSL_ERROR_SYSCALL              5
#define AMPS_SSL_ERROR_ZERO_RETURN          6
#define AMPS_SSL_ERROR_WANT_CONNECT         7
#define AMPS_SSL_ERROR_WANT_ACCEPT          8

#define AMPS_SSL_CTRL_MODE                  33
#define AMPS_SSL_CTRL_SET_TLSEXT_HOSTNAME   55
#define AMPS_SSL_AUTO_RETRY                 0x4L

#define AMPS_CRYPTO_LOCK                    0x01

#define AMPS_TLSEXT_NAMETYPE_host_name      0

typedef void _amps_SSL_CTX;
typedef void _amps_SSL_METHOD;
typedef void _amps_SSL;

extern void              (*_amps_SSL_library_init)(void);
extern void              (*_amps_SSL_load_error_strings)(void);
extern unsigned long     (*_amps_ERR_get_error)(void);
extern void              (*_amps_ERR_error_string_n)(unsigned long, char*, size_t);
extern void              (*_amps_ERR_clear_error)(void);
extern _amps_SSL_METHOD* (*_amps_SSLv23_client_method)(void);
extern _amps_SSL_CTX*    (*_amps_SSL_CTX_new)(const _amps_SSL_METHOD*);
extern void              (*_amps_SSL_CTX_free)(_amps_SSL_CTX*);
extern _amps_SSL*        (*_amps_SSL_new)(_amps_SSL_CTX*);
extern int               (*_amps_SSL_set_fd)(_amps_SSL*, int);
extern int               (*_amps_SSL_get_error)(_amps_SSL*, int);
extern int               (*_amps_SSL_connect)(_amps_SSL*);
extern int               (*_amps_SSL_read)(_amps_SSL*, void*, int);
extern int               (*_amps_SSL_ctrl)(_amps_SSL*, int, long, void*);
extern int               (*_amps_SSL_write)(_amps_SSL*, const void*, int);
extern int               (*_amps_SSL_shutdown)(_amps_SSL*);
extern int               (*_amps_SSL_pending)(_amps_SSL*);
extern void              (*_amps_SSL_free)(_amps_SSL*);

/* global context created when amps_ssl_init is called. */
extern _amps_SSL_CTX*    _amps_ssl_ctx;

#endif
