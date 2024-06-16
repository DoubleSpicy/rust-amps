////////////////////////////////////////////////////////////////////////////
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
////////////////////////////////////////////////////////////////////////////

#ifndef _SERVERCHOOSERIMPL_H_
#define _SERVERCHOOSERIMPL_H_

#include <amps/ampsplusplus.hpp>

namespace AMPS
{

///
/// Abstract base class for choosing amongst multiple URIs for both the initial
/// connection and reconnection. Used by HAClient to pick an initial server and
/// also to pick a server if there is a failure.
///
  class ServerChooserImpl : public RefBody
  {
  public:
    ///
    /// Returns the current URI.
    ///
    /// \return The current URI or empty string if no server is available.
    virtual std::string getCurrentURI() = 0;

    ///
    /// Returns the Authenticator instance associated with the current URI.
    ///
    /// \return An Authenticator or NULL if none is required for logon.
    virtual Authenticator& getCurrentAuthenticator() = 0;

    ///
    /// Called by HAClient when an error occurs connecting to the current URI,
    /// and/or when an error occurs logging on. Implementors will likely
    /// advance the current URI to the next one in a list, or choose to stay
    /// with the current one, based on the exception type.
    /// \param exception_ The exception associated with the failure.
    /// \param info_ The information about the connection that failed.
    virtual void reportFailure(const AMPSException& exception_,
                               const ConnectionInfo& info_) = 0;

    ///
    /// Called by HAClient when no servers are available to
    /// provide detailed error message in exception message
    /// \return The detailed error message.
    virtual std::string getError()
    {
      return std::string();
    }

    ///
    /// Called by the HAClient when successfully connected and logged on to
    /// the current instance.
    /// \param info_ The information about the connection that failed.
    virtual void reportSuccess(const ConnectionInfo& info_) = 0;

    ///
    /// Add a server to a server chooser if its policy permits
    /// \param uri_ The URI of the server to add
    virtual void add(const std::string& uri_) = 0;

    ///
    /// Remove a server from a server chooser if its policy permits
    /// \param uri_ The URI of the server to remove
    virtual void remove(const std::string& uri_) = 0;

    virtual ~ServerChooserImpl() {}
  };

} //namespace AMPS

#endif //_SERVERCHOOSERIMPL_H_

