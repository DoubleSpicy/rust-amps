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

#ifndef _SERVERCHOOSER_H_
#define _SERVERCHOOSER_H_

#include <amps/ampsplusplus.hpp>
#include <amps/ServerChooserImpl.hpp>

/// \file ServerChooser.hpp
/// \brief Provides AMPS::ServerChooser, the abstract base class that defines
/// the interface that an AMPS::HAClient uses for determining which server
/// to connect to.


namespace AMPS
{

///
/// Abstract base class for choosing amongst multiple URIs for both the initial
/// connection and reconnection. Used by HAClient to pick an initial server and
/// also to pick a server if there is a failure.
///
  class ServerChooser
  {
    RefHandle<ServerChooserImpl> _body;
  public:
    ServerChooser() : _body(NULL) {}

    ServerChooser(ServerChooserImpl* body_) : _body(body_) {}

    bool isValid()
    {
      return _body.isValid();
    }

    ///
    /// Returns the current URI.
    ///
    /// \return The current URI or empty string if no server is available.
    ///
    std::string getCurrentURI()
    {
      return _body.get().getCurrentURI();
    }

    ///
    /// Returns the Authenticator instance associated with the current URI.
    ///
    /// \return An Authenticator or NULL if none is required for logon.
    ///
    Authenticator& getCurrentAuthenticator()
    {
      return _body.get().getCurrentAuthenticator();
    }

    ///
    /// Called by HAClient when an error occurs connecting to the current URI,
    /// and/or when an error occurs logging on. Implementors will likely
    /// advance the current URI to the next one in a list, or choose to stay
    /// with the current one, based on the exception type.
    /// \param exception_ The exception associated with the failure.
    /// \param info_ a map of information about the connection at time of failure
    void reportFailure(const AMPSException& exception_,
                       const ConnectionInfo& info_)
    {
      _body.get().reportFailure(exception_, info_);
    }

    ///
    /// Provides additional detail to be included in an exception thrown
    /// when the AMPS instance(s) are not available. Called by the HAClient
    /// when creating an exception.
    /// \return A string with information about the connection that failed
    ///         and the reason for the failure. When no further information
    ///         is available, returns an empty string.
    std::string getError()
    {
      return _body.get().getError();
    }

    ///
    /// Called by the HAClient when successfully connected and logged on to
    /// the current instance.
    /// \param info_ information about the successful connection
    void reportSuccess(const ConnectionInfo& info_)
    {
      _body.get().reportSuccess(info_);
    }

    ///
    /// Add a server to a server chooser if its policy permits
    /// \param uri_ The URI of the server to add
    ServerChooser& add(const std::string& uri_)
    {
      _body.get().add(uri_);
      return *this;
    }

    ///
    /// Remove a server from a server chooser if its policy permits
    /// \param uri_ The URI of the server to remove
    void remove(const std::string& uri_)
    {
      _body.get().remove(uri_);
    }

    ~ServerChooser() {}

    ServerChooser& operator=(const ServerChooser& rhs)
    {
      _body = rhs._body;
      return *this;
    }

    ServerChooser(const ServerChooser& rhs) : _body(rhs._body) {}

  };

} //namespace AMPS

#endif //_SERVERCHOOSER_H_

