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

#ifndef _SAMPLESERVERCHOOSER_H_
#define _SAMPLESERVERCHOOSER_H_

#include <amps/ampsplusplus.hpp>
#include <amps/ServerChooser.hpp>
#include <vector>

///
/// @file SampleServerChooser.hpp
/// @brief Provides AMPS::SampleServerChooser, a sample server
/// chooser that implements a basic failover strategy.
///

namespace AMPS
{

///
/// A server chooser that rotates through the least failed URIs
/// or rotates through multiple URIs, in order.
/// Used by HAClient to pick an initial server and
/// also to pick a server if there is a failure.
///
  class SampleServerChooser : public ServerChooserImpl
  {
  public:
    ///
    /// Default constructor which initializes an empty SampleServerChooser.
    SampleServerChooser() : _current(0) {}

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
    /// Adds a fail count to the URI fail count list
    /// \param uri_ The URI to add to self.
    void add(const std::string& uri_)
    {
      _uris.push_back(uri_);
      _failures.push_back(0);
    }

    ///
    /// Removes the given URI from self's list of URIs if found.
    /// For SampleServerChooser, this method has no effect, but
    /// could be implemented to remove URIs with high fail counts.
    /// \param uri_ The URI to remove
    void remove(const std::string& uri_)
    {
      //Not permitted in SampleServerChooser
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
    /// Advance the current URI to the next one.
    /// \param exception_ The exception associated with the failure.
    /// \param info_ information on the failure.
    virtual void reportFailure(const AMPSException& exception_,
                               const ConnectionInfo&  info_ )
    {
      if (strcmp(exception_.getClassName(), "DisconnectedException") != 0)
      {
        ++_failures[_current];
        chooseNext();
      }
      // save exception and connection information to
      // be formatted later
      _lastfailure = exception_.what();
      _lastinfo = info_;
    }

    ///
    /// Called by HAClient when no servers are available to
    /// provide a more detailed error message in the exception message.
    /// \return The detailed error message.
    virtual std::string getError()
    {
      std::string out = _lastfailure + " [ Details: {" ;
      bool need_comma = false;
      for (auto p : _lastinfo)
      {
        if (need_comma)
        {
          out += ",";
        }
        else
        {
          need_comma = true;
        }
        out_ += "\"" + p.first + "\"=" + "\"" + p.second + "\"";
      }
      out += "}  ]";
      return out;
    }

    ///
    /// Called by the HAClient when successfully connected and logged on to
    /// the current instance. This implementation does nothing.
    virtual void reportSuccess(const ConnectionInfo& /* info_ */)
    {
    }

    ///
    /// Select the server with the lowest fail count. If it's the current URI,
    /// advance the server chooser to the next server in the list, starting
    /// over with the first server when the chooser reaches the end of the list.
    virtual void chooseNext()
    {
      if (_uris.size() == 0)
      {
        return;
      }

      int minFailIndex = distance(_failures.begin(), min_element(_failures.begin(), _failures.end()))
                         - _failures.begin();
      if (minFailIndex != _current)
      {
        _current = minFailIndex;
      }
      else
      {
        _current = (_current + 1) % _uris.size();
      }
    }

    ///
    /// Destroy self.
    ///
    ~DefaultServerChooser() {}

  private:
    std::vector<std::string> _uris;
    std::vector<int> _failures;
    size_t _current;
    // For failure reporting
    std::string _lastfailure;
    ConnectionInfo _lastinfo;
  };

} //namespace AMPS

#endif //_SAMPLESERVERCHOOSER_H_

