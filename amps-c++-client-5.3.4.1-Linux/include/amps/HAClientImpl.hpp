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

#ifndef _HACLIENTIMPL_H_
#define _HACLIENTIMPL_H_

#include <typeinfo>
#include <amps/ampsplusplus.hpp>
#include <amps/ServerChooser.hpp>
#include <amps/MemorySubscriptionManager.hpp>
#include <amps/ReconnectDelayStrategy.hpp>
#if __cplusplus >= 201103L || _MSC_VER >= 1900
#include <atomic>
#endif

namespace AMPS
{

  class HAClientImpl : public ClientImpl
  {
  public:
    HAClientImpl(const std::string& name_)
      : ClientImpl(name_), _timeout(AMPS_HACLIENT_TIMEOUT_DEFAULT)
      , _reconnectDelay(AMPS_HACLIENT_RECONNECT_DEFAULT)
      , _reconnectDelayStrategy(new ExponentialDelayStrategy(_reconnectDelay))
      , _disconnected(false)
    {
#ifdef AMPS_USE_FUNCTIONAL
      setDisconnectHandler(HADisconnectHandler());
#else
      setDisconnectHandler(AMPS::DisconnectHandler(&HADisconnectHandler::invoke, NULL));
#endif
      setSubscriptionManager(new MemorySubscriptionManager());
    }

    ~HAClientImpl()
    {
      _disconnected = true;
      _cleanup();
    }

    void setTimeout(int timeout_)
    {
      _timeout = timeout_;
    }

    int getTimeout() const
    {
      return _timeout;
    }

    unsigned int getReconnectDelay(void) const
    {
      return _reconnectDelay;
    }

    void setReconnectDelay(unsigned int reconnectDelay_)
    {
      _reconnectDelay = reconnectDelay_;
      setReconnectDelayStrategy(new FixedDelayStrategy(
                                  (unsigned int)reconnectDelay_));
    }

    void setReconnectDelayStrategy(const ReconnectDelayStrategy& strategy_)
    {
      _reconnectDelayStrategy = strategy_;
    }

    ReconnectDelayStrategy getReconnectDelayStrategy(void) const
    {
      return _reconnectDelayStrategy;
    }

    std::string getLogonOptions(void) const
    {
      return _logonOptions;
    }

    void setLogonOptions(const std::string& logonOptions_)
    {
      _logonOptions = logonOptions_;
    }

    void setLogonOptions(const char* logonOptions_)
    {
      _logonOptions = logonOptions_;
    }

    ServerChooser getServerChooser() const
    {
      return _serverChooser;
    }

    void setServerChooser(const ServerChooser& serverChooser_)
    {
      _serverChooser = serverChooser_;
    }

    class HADisconnectHandler
    {
    public:
      HADisconnectHandler() {}
      static void invoke(Client& client, void* );
#ifdef AMPS_USE_FUNCTIONAL
      void operator()(Client& client)
      {
        invoke(client, NULL);
      }
#endif
    };
    void connectAndLogon()
    {
      Lock<Mutex> l(_connectAndLogonLock);
      // AC-1030 In case this is called on a client after delay strategy caused a failure.
      _reconnectDelayStrategy.reset();
      try
      {
        while (true)
        {
          _disconnected = false;
          connectAndLogonInternal();
          try
          {
            // Resubscribe
            if (_subscriptionManager)
            {
              Client c(this);
              _subscriptionManager->resubscribe(c);
              broadcastConnectionStateChanged(
                ConnectionStateListener::Resubscribed);
            }
            return;
          }
          catch (const AMPSException& subEx)
          {
            // Keep receive thread from reconnecting
            _disconnected = true;
            _serverChooser.reportFailure(subEx, getConnectionInfo());
            ClientImpl::setDisconnected();
          }
        }
      }
      catch (...)
      {
        // Failure, make sure we're disconnected
        disconnect();
        throw;
      }
    }

    virtual void connect(const std::string& /*uri*/)
    {
      connectAndLogon();
    }

    virtual std::string logon(long /*timeout_*/, Authenticator& /*authenticator_*/,
                              const char* /*options_*/)
    {
      if (_disconnected)
      {
        throw DisconnectedException("Attempt to call logon on a disconnected HAClient. Use connectAndLogon() instead.");
      }
      throw AlreadyConnectedException("Attempt to call logon on an HAClient. Use connectAndLogon() instead.");
    }

    static void HADoNothingDisconnectHandler(amps_handle /*client*/,
                                             void* /*data*/)
    {
      ;
    }

    class DisconnectHandlerDisabler
    {
    public:
      DisconnectHandlerDisabler()
        : _pClient(NULL), _queueAckTimeout(0)  { }
      DisconnectHandlerDisabler(HAClientImpl* pClient_)
        : _pClient(pClient_)
      {
        setHandler();
        _queueAckTimeout = _pClient->getAckTimeout();
        _pClient->setAckTimeout(0);
      }
      ~DisconnectHandlerDisabler()
      {
        clear();
      }
      void clear()
      {
        if (_pClient)
        {
          amps_client_set_disconnect_handler(
            _pClient->getHandle(),
            (amps_handler)ClientImpl::ClientImplDisconnectHandler,
            _pClient);
          if (_queueAckTimeout)
          {
            _pClient->setAckTimeout(_queueAckTimeout);
            _queueAckTimeout = 0;
          }
          _pClient = NULL;
        }
      }
      void setClient(HAClientImpl* pClient_)
      {
        if (!_pClient)
        {
          _pClient = pClient_;
          setHandler();
          _queueAckTimeout = _pClient->getAckTimeout();
          _pClient->setAckTimeout(0);
          amps_client_disconnect(_pClient->getHandle());
        }
      }
      void setHandler()
      {
        amps_client_set_disconnect_handler(
          _pClient->getHandle(),
          (amps_handler)HAClientImpl::HADoNothingDisconnectHandler,
          _pClient);
      }
    private:
      HAClientImpl* _pClient;
      int _queueAckTimeout;
    };

    void connectAndLogonInternal()
    {
      if (!_serverChooser.isValid())
      {
        throw ConnectionException("No server chooser registered with HAClient");
      }
      {
        DisconnectHandlerDisabler disconnectDisabler;
        while (!_disconnected)
        {
          std::string uri = _serverChooser.getCurrentURI();
          if (uri.empty())
          {
            throw ConnectionException("No AMPS instances available for connection. " + _serverChooser.getError());
          }
          Authenticator& auth = _serverChooser.getCurrentAuthenticator();
          // Begin locked section - see AC-1017
          Lock<Mutex> l(_connectLock);
          _sleepBeforeConnecting(uri);
          try
          {
            // Check if another thread disconnected or already connected
            if (_disconnected || _connected)
            {
              return;
            }
            // Temporarily unset the disconnect handler since we will loop
            disconnectDisabler.setClient((HAClientImpl*)this);
            // Connect and logon while holding the _lock
            {
              Lock<Mutex> clientLock(_lock);
              ClientImpl::_connect(uri);
              try
              {
                if (_logonOptions.empty())
                {
                  ClientImpl::_logon(_timeout, auth);
                }
                else
                {
                  ClientImpl::_logon(_timeout, auth, _logonOptions.c_str());
                }
              } // All of the following may not disconnect the client
              catch (const AuthenticationException&)
              {
                ClientImpl::setDisconnected();
                throw;
              }
              catch (const NotEntitledException&)
              {
                ClientImpl::setDisconnected();
                throw;
              }
              catch (const DuplicateLogonException&)
              {
                ClientImpl::setDisconnected();
                throw;
              }
              catch (const NameInUseException&)
              {
                ClientImpl::setDisconnected();
                throw;
              }
              catch (const TimedOutException&)
              {
                ClientImpl::setDisconnected();
                throw;
              }
            }
            disconnectDisabler.clear();
            try
            {
              _serverChooser.reportSuccess(getConnectionInfo());
              _reconnectDelayStrategy.reset();
            }
            catch (const AMPSException&)
            {
              ClientImpl::disconnect();
              throw;
            }
            break;
          }
          catch (const AMPSException& ex)
          {
            Unlock<Mutex> u(_connectLock);
            ConnectionInfo ci = getConnectionInfo();
            // Substitute the URI on the connection info with the one we attempted
            ci["client.uri"] = uri;
            _serverChooser.reportFailure(ex, ci);
            try
            {
              ClientImpl::setDisconnected();
            }
            catch (const std::exception& e)
            {
              try
              {
                _exceptionListener->exceptionThrown(e);
              }
              catch (...) { }   // -V565
            }
            catch (...)
            {
              try
              {
                _exceptionListener->exceptionThrown(UnknownException("Unknown exception calling setDisconnected"));
              }
              catch (...) { }   // -V565
            }
          }
        }
      }
      return;
    }

    ConnectionInfo gatherConnectionInfo() const
    {
      return getConnectionInfo();
    }

    ConnectionInfo getConnectionInfo() const
    {
      ConnectionInfo info = ClientImpl::getConnectionInfo();
      std::ostringstream writer;

      writer << getReconnectDelay();
      info["haClient.reconnectDelay"] = writer.str();
      writer.clear(); writer.str("");
      writer << _timeout;
      info["haClient.timeout"] = writer.str();

      return info;
    }

    bool disconnected() const
    {
      return _disconnected;
    }
  private:

    void disconnect()
    {
      {
        Lock<Mutex> l(_connectLock);
        _disconnected = true;
      }
      ClientImpl::disconnect();
    }
    void _millisleep(unsigned int millis_)
    {
      if (millis_ == 0)
      {
        return;
      }
      double waitTime = (double)millis_;
      Timer timer(waitTime);
      timer.start();
      while (!timer.checkAndGetRemaining(&waitTime))
      {
        if (waitTime - 1000.0 > 0.0)
        {
          AMPS_USLEEP(1000000);
        }
        else
        {
          AMPS_USLEEP(1000UL * (unsigned int)waitTime);
        }
        amps_invoke_waiting_function();
      }
    }
    void _sleepBeforeConnecting(const std::string& uri_)
    {
      try
      {
        _millisleep(
          _reconnectDelayStrategy.getConnectWaitDuration(uri_));
      }
      catch (const ConnectionException&)
      {
        throw;
      }
      catch (const std::exception& ex_)
      {
        _exceptionListener->exceptionThrown(ex_);
        throw ConnectionException(ex_.what());
      }
      catch (...)
      {
        throw ConnectionException("Unknown exception thrown by "
                                  "the HAClient's delay strategy.");
      }
    }

    Mutex                  _connectLock;
    Mutex                  _connectAndLogonLock;
    int                    _timeout;
    unsigned int           _reconnectDelay;
    ReconnectDelayStrategy _reconnectDelayStrategy;
    ServerChooser          _serverChooser;
#if __cplusplus >= 201103L || _MSC_VER >= 1900
    std::atomic<bool>      _disconnected;
#else
    volatile bool          _disconnected;
#endif
    std::string            _logonOptions;

  }; // class HAClientImpl

}// namespace AMPS

#endif //_HACLIENTIMPL_H_

