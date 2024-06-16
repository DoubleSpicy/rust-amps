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

#ifndef _AMPS_RECONNECTDELAYSTRATEGYIMPL_HPP_
#define _AMPS_RECONNECTDELAYSTRATEGYIMPL_HPP_
#include <amps/util.hpp>
#include <string>

#ifdef _WIN32
  #include <windows.h>
#else
  #include <sys/time.h>
#endif

#include <math.h>
#include <stdexcept>
#include <stdio.h>

namespace AMPS
{
  ///
  /// Base class for ReconnectDelayStrategy implementations.
  ///
  class ReconnectDelayStrategyImpl : public RefBody
  {
  public:
    virtual ~ReconnectDelayStrategyImpl(void) {;}

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
    virtual unsigned int getConnectWaitDuration(
      const std::string& uri_) = 0;
    ///
    /// Reset the state of this reconnect delay. AMPS calls this method
    /// when a connection is successfully established.
    ///
    virtual void      reset(void) = 0;
  };

  ///
  /// ExponentialDelayStrategy is an implementation that exponentially
  /// "backs off" when reconnecting to the same server, with a maximum
  /// number of retries before it gives up entirely.
  ///
  class ExponentialDelayStrategy : public ReconnectDelayStrategyImpl
  {
  public:
    ///
    /// Constructs an exponential delay strategy, the default strategy
    /// for HAClient.
    ///
    /// \param initialDelay_     The time (in milliseconds) to wait before
    ///                          reconnecting to a server for the first
    ///                          time after a failed connection attempt.
    ///
    /// \param maximumDelay_     The maximum time to wait for any reconnect
    ///                          attempt (milliseconds). Exponential
    ///                          backoff will not exceed this maximum.
    ///
    /// \param backoffExponent_  The exponent to use for calculating the
    ///                          next delay time. For example, if the
    ///                          initial time is 200ms and the exponent
    ///                          is 2.0, the next delay will be 400ms,
    ///                          then 800ms, etc.
    ///
    /// \param maximumRetryTime_ The maximum time (milliseconds) to allow
    ///                          reconnect attempts to continue without
    ///                          a successful connection, before "giving
    ///                          up" and abandoning the connection attempt.
    ///
    /// \param jitter_           The amount of 'jitter' to apply when
    ///                          calculating a delay time, measured in
    ///                          multiples of the initial delay. Jitter is
    ///                          used to reduce the number of simultaneous
    ///                          reconnects that may be issued from multiple
    ///                          clients.
    ///
    ExponentialDelayStrategy(
      unsigned int initialDelay_    = 200,
      unsigned int maximumDelay_    = 20 * 1000,
      double       backoffExponent_ = 2.0,
      unsigned int maximumRetryTime_ = 0,
      double       jitter_          = 1.0)
      : _initialDelay(initialDelay_),
        _maximumDelay(maximumDelay_),
        _backoffExponent(backoffExponent_),
        _jitter(jitter_),
        _maximumRetryTime(maximumRetryTime_),
        _timer(maximumRetryTime_)
    {
      if (_jitter > 0.0)
      {
        ::srand((unsigned int)amps_now());
      }
    }

    unsigned int getConnectWaitDuration(const std::string& uri_)
    {
      _throwIfMaximumExceeded();
      URIDelayMapIterator currentDelay = _currentDelays.find(uri_);
      if (currentDelay == _currentDelays.end())
      {
        // Is this our first attempt?
        if (_maximumRetryTime != 0 && _currentDelays.empty())
        {
          _timer.start();
        }
        // New URI so delay will be zero
        _currentDelays[uri_] = 0;
        return 0;
      }
      // We've tried this one before, so increase it and return with jitter
      return _currentDurationAndIncrease(&(currentDelay->second));
    }
    void reset(void)
    {
      _currentDelays.clear();
      _timer.reset();
    }

  protected:

    void _throwError(void)
    {
      throw ReconnectMaximumExceededException(
        "The maximum time to attempt "
        "connection to a server has been exceeded.");
    }

    void _throwIfMaximumExceeded(void)
    {
      if (_timer.check())
      {
        _throwError();
      }
    }

    unsigned int _currentDurationAndIncrease(unsigned int* pCurrentDelay_)
    {
      // Calculate the increase to the base delay
      unsigned long long newDelay = (*pCurrentDelay_ == 0) ?
                                    (unsigned long long)_initialDelay :
                                    (unsigned long long)(*pCurrentDelay_ * _backoffExponent);
      if (newDelay > _maximumDelay)
      {
        newDelay = _maximumDelay;
      }
      // Save the base delay
      *pCurrentDelay_ = (unsigned int)newDelay;
      // Add jitter, if any, for current delay
      unsigned int delay = (unsigned int)newDelay;
      unsigned int maxJitter = (unsigned int)(_initialDelay * _jitter);
      if (_jitter > 0.0)
      {
        if (delay > _maximumDelay - maxJitter)
          delay = (_maximumDelay - maxJitter > _initialDelay) ?
                  _maximumDelay - maxJitter : _initialDelay;
        delay +=
          (unsigned int)(_initialDelay * _jitter * (::rand() * 1.0 / RAND_MAX));
        if (delay > _maximumDelay)
        {
          delay = _maximumDelay;
        }
      }
      // Avoid delaying past any configured max retry time.
      if (_maximumRetryTime)
      {
        double remaining = 0.0;
        if (_timer.checkAndGetRemaining(&remaining))
        {
          _throwError();
        }
        unsigned int remainingMillis = (unsigned int)remaining + 1U;
        if (remainingMillis < delay)
        {
          delay = remainingMillis;
        }
      }
      return delay;
    }

    unsigned int       _initialDelay;
    unsigned int       _maximumDelay;
    double             _backoffExponent;
    double             _jitter;
    unsigned int       _maximumRetryTime;
    typedef std::map<std::string, unsigned int> URIDelayMap;
    typedef std::map<std::string, unsigned int>::iterator URIDelayMapIterator;
    URIDelayMap        _currentDelays;
    Timer              _timer;
  };

  ///
  /// FixedDelayStrategy is an implementation that delays for a fixed time
  /// period, as specified in the constructor, when reconnecting to the same
  /// server as we were previously connected to, or if we are invoked again
  /// for the first server we ever tried.
  ///
  class FixedDelayStrategy : public ReconnectDelayStrategyImpl
  {
  public:
    ///
    /// Construct a FixedDelayStrategy with a given duration.
    ///
    /// \param duration_ The delay (milliseconds) to be used between
    ///                  reconnect attempts to the same server.
    ///                  (defaults to 200ms).
    /// \param maximum_ The maximum time (milliseconds) to retry
    ///                 before giving up. Default is 0, or don't
    ///                 give up.
    ///
    FixedDelayStrategy(unsigned int duration_ = 200, unsigned maximum_ = 0)
      : _duration(duration_),
        _maximum(maximum_),
        _timer(maximum_)
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
      double remaining = 0.0;
      if (_maximum > 0)
      {
        // Start the timer
        if (_triedURIs.empty())
        {
          _timer.start();
        }
        // or check for max retry time exceeded
        else if (_timer.checkAndGetRemaining(&remaining))
        {
          throw ReconnectMaximumExceededException(
            "The maximum time to attempt "
            "connection to a server has been exceeded.");
        }
      }
      // Only wait to reconnect to a repeat server.
      if (_triedURIs.count(uri_) == 0)
      {
        _triedURIs.insert(uri_);
        return 0;
      }
      // Check for max retry time exceeded after delay
      if (_maximum > 0 && remaining <= _duration)
      {
        throw ReconnectMaximumExceededException(
          "The maximum time to attempt connection to a server "
          "would be exceeded by another delay.");
      }
      // We're trying to reconnect to a previously tried address, delay.
      return _duration;
    }

    ///
    /// Reset the state of this reconnect delay. AMPS calls this method
    /// when a connection is successfully established.
    ///
    void reset(void)
    {
      // Forget the last and first so we get one immediate reconnect
      // attempt if we try this server again.
      _triedURIs.clear();
      _timer.reset();
    }

  private:
    unsigned int          _duration;
    unsigned int          _maximum;
    std::set<std::string> _triedURIs;
    Timer                 _timer;
  };
}
#endif // _AMPS_RECONNECTDELAYSTRATEGYIMPL_HPP_

