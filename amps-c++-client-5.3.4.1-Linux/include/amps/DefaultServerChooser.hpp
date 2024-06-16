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

#ifndef _DEFAULTSERVERCHOOSER_H_
#define _DEFAULTSERVERCHOOSER_H_

#include <amps/ampsplusplus.hpp>
#include <amps/ServerChooser.hpp>
#include <vector>

///
/// @file DefaultServerChooser.hpp
/// @brief Provides AMPS::DefaultServerChooser, a simple server
/// chooser that implements basic round-robin failover.
///

namespace AMPS
{

///
/// A server chooser that rotates through multiple URIs, in order.
/// Used by HAClient to pick an initial server and
/// also to pick a server if there is a failure.
///
  class DefaultServerChooser : public ServerChooserImpl
  {
  public:
    ///
    /// Default constructor which initializes an empty DefaultServerChooser.
    DefaultServerChooser() : _current(0) {}

    ///
    /// Returns the current URI.
    ///
    /// \return The current URI or empty string if no server is available.
    ///
    virtual std::string getCurrentURI()
    {
      if (_uris.size() > 0)
      {
        return _uris[_current];
      }
      return std::string();
    }

    ///
    /// Adds the given URI to self's list of URIs
    /// \param uri_ The URI to add to self.
    void add(const std::string& uri_)
    {
      _uris.push_back(uri_);
    }

    ///
    /// Adds the given URIs to self's list of URIs
    /// \param uriContainer_ The URIs to add to self.
    template <class T>
    void addAll(const T& uriContainer_)
    {
      for (typename T::const_iterator i = uriContainer_.begin(); i != uriContainer_.end(); ++i)
      {
        _uris.push_back(*i);
      }
    }

    ///
    /// Removes the given URI from self's list of URIs if found.
    /// For DefaultServerChooser, this method has no effect.
    void remove(const std::string& /*uri_*/)
    {
      //Not permitted in DefaultServerChooser
    }

    ///
    /// Returns the Authenticator instance associated with the current URI.
    ///
    /// \return The DefaultAuthenticator instance
    ///
    virtual Authenticator& getCurrentAuthenticator()
    {
      return DefaultAuthenticator::instance();
    }

    ///
    /// Called by HAClient when an error occurs connecting to the current URI,
    /// and/or when an error occurs logging on.
    /// Advance the current URI to the next one in a list
    /// \param exception_ The exception associated with the failure.
    virtual void reportFailure(const AMPSException& exception_,
                               const ConnectionInfo& /* info_ */)
    {
      if (strcmp(exception_.getClassName(), "DisconnectedException") != 0)
      {
        next();
      }
    }

    ///
    /// Called by HAClient when no servers are available to
    /// provide a more detailed error message in the exception message.
    /// This implementation returns an empty string.
    /// \return The detailed error message.
    virtual std::string getError()
    {
      return std::string();
    }

    ///
    /// Called by the HAClient when successfully connected and logged on to
    /// the current instance. This implementation does nothing.
    virtual void reportSuccess(const ConnectionInfo& /* info_ */)
    {
    }

    ///
    /// Advance the server chooser to the next server in the list, starting
    /// over with the first server when the chooser reaches the end of the list.
    virtual void next()
    {
      if (_uris.size() == 0)
      {
        return;
      }
      _current = (_current + 1) % _uris.size();
    }

    ///
    /// Destroy self.
    ///
    ~DefaultServerChooser() {}

  private:
    std::vector<std::string> _uris;
    size_t _current;
  };

} //namespace AMPS

#endif //_DEFAULTSERVERCHOOSER_H_

