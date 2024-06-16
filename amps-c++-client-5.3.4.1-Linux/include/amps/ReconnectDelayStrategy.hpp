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

#ifndef _AMPS_RECONNECTDELAYSTRATEGY_HPP
#define _AMPS_RECONNECTDELAYSTRATEGY_HPP


#include <amps/ReconnectDelayStrategyImpl.hpp>

/// \file ReconnectDelayStrategy.hpp
/// \brief Provides AMPS::ReconnectDelayStrategy, called by an AMPS::HAClient
/// to determine how long to wait between attempts to connect or reconnect to
/// a server.

namespace AMPS
{
  ///
  ///  ReconnectDelayStrategy is called by AMPS::HAClient to determine how
  ///  long to wait between attempts to connect or reconnect to a server. The
  ///  class is implemented as a handle that reference counts an instance of
  ///  AMPS::ReconnectDelayStrategyImpl.
  class ReconnectDelayStrategy
  {
  public:
    ///
    /// Constructs a ReconnectDelayStrategy with a default implementation.
    /// Chooses an AMPS::ExponentialDelayStrategy by default.
    ///
    ReconnectDelayStrategy()
      : _implPtr(new ExponentialDelayStrategy())
    {;}

    ///
    /// Constructs a ReconnectDelayStrategy with a given implementation.
    ///
    /// \param pImpl_ An instance of a class derived from
    ///               AMPS::ReconnectDelayStrategyImpl to wrap.
    ///
    ReconnectDelayStrategy(ReconnectDelayStrategyImpl* pImpl_)
      : _implPtr(pImpl_)
    {;}


    ///
    /// Returns the time that the client should delay before connecting
    /// to the given server URI.
    ///
    /// \param uri_ The URI which the client plans to connect.
    ///
    /// \return     The time, in milliseconds, which the client should
    ///             delay before connecting to uri_.
    ///
    /// \throws  Any exception thrown indicates no connection should be
    ///          attempted; the client should in essence "give up."
    ///
    unsigned int getConnectWaitDuration(const std::string& uri_)
    {
      return _implPtr->getConnectWaitDuration(uri_);
    }

    ///
    /// Reset the state of this reconnect delay. AMPS calls this method
    /// when a connection is successfully established.
    ///
    void reset(void)
    {
      _implPtr->reset();
    }

    ///
    /// Returns a pointer to the raw ReconnetDelayStrategyImpl this
    /// class is wrapping.
    ///
    ReconnectDelayStrategyImpl* get(void)
    {
      return &(_implPtr.get());
    }
  protected:
    AMPS::RefHandle<ReconnectDelayStrategyImpl> _implPtr;
  };
}


#endif
