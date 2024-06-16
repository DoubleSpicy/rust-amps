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

#ifndef _AMPS_AUTHORIZATION_H
#define _AMPS_AUTHORIZATION_H

#include <module/amps_module.h>

///
/// @file authorization/amps_authorization.h
/// @brief Core type and function declarations for the AMPS SDK Authorization and Entitlement Plugin APIs
/// Authorization modules verify the identity of an AMPS user. Entitlement
/// modules enforce permissions on AMPS resources. Typically, these modules
/// are used together to provide security for an AMPS installation.
///

/// Returned in response to an authorization request to indicate that
/// the client must respond with additional information for authorization.
const int AMPS_RETRY               = 0xF0;

/// @defgroup permission_type Requested permissions
/// Request for read access to a resource.
const int AMPS_READ_ALLOWED        = 0x01;
/// @ingroup permission_type
/// Request for write access to a resource.
const int AMPS_WRITE_ALLOWED       = 0x02;
/// @ingroup permission_type
/// Request for write access to a resource.
const int AMPS_REPLICATION_ALLOWED = 0x04;



///
/// Opaque handle returned from amps_authentication_create_context. AMPS
/// will store this value and pass it to the authentication module functions.
/// AMPS does not dereference this handle.
/// The size of a pointer.
///
typedef void* amps_authentication_context;
///
/// Opaque handle returned from amps_entitlement_create_context. AMPS
/// will store this value and pass it to the authentication module functions.
/// AMPS does not dereference this handle.
/// The size of a pointer.
///
typedef void* amps_entitlement_context;
typedef void* amps_authenticator_context;

/// Indicates the type of resource in an entitlement request.
enum amps_entitlement_resource_type
{
  /// The request is for a topic (including views and topic replicas)
  amps_resource_topic,
  /// The request is for an admin resource.
  amps_resource_admin,
  /// The request is for a logon.
  amps_resource_logon,
  /// The request is for a replication source logon.
  amps_resource_replication_logon
};

#endif

