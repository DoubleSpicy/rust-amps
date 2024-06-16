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

#ifndef _HACLIENT_H_
#define _HACLIENT_H_

#include <amps/ampsplusplus.hpp>
#include <amps/HAClientImpl.hpp>
#include <amps/MemoryPublishStore.hpp>
#include <amps/PublishStore.hpp>
#include <amps/MemoryBookmarkStore.hpp>
#include <amps/MMapBookmarkStore.hpp>

///
/// \file HAClient.hpp
/// \brief Defines AMPS::HAClient, a client that can provide failover, durable
/// publication, and resumable subscriptions.
///


namespace AMPS
{
/// A highly-available Client that automatically reconnects and
/// re-subscribes to AMPS instances upon disconnect.
///
/// An HAClient provides failover and resubscription functionality by default.
/// For reliable publish, set a PublishStore for the HAClient (and, in most
/// cases, set a FailedWriteHandler as well so the application can detect
/// any failed publishes). For managing transaction log replays, set a
/// bookmark store (such as LoggedBookmarkStore or MemoryBookmarkStore).
///
/// To connect to AMPS, you must provide a ServerChooser implementation to
/// the HAClient. By default, the HAClient provides an ExponentialDelayStrategy
/// with the default delay and backoff behavior for that class.
///
/// By default, the HAClient object has the reconnect and resubscribe
/// functionality provided by the MemorySubscriptionManager and a default
/// DisconnectHandler. It is typically not necessary to replace the
/// disconnect handler or subscription manager for this class. Notice that
/// replacing either of these defaults will change the failover, resubscription,
/// and/or republish behavior of the HAClient.
///
/// See the <em>Developer Guide</em> for more information on using the HAClient.

  class HAClient : public Client
  {
  public:
    ///
    /// Create an HAClient with no name
    /// The name for the client must be set using setName before it can be used.
    HAClient()
      : Client(new HAClientImpl(std::string()))
    {
    }

    ///
    /// Create an HAClient with the given name
    /// \param name_ Name for the client. This name is used for duplicate
    /// message detection and should be unique. AMPS does not enforce
    /// specific restrictions on the character set used, however some
    /// protocols (for example, XML) may not allow specific characters.
    /// 60East recommends that the client name be meaningful, short, human
    /// readable, and avoids using control characters, newline characters,
    /// or square brackets.
    HAClient(const std::string& name_)
      : Client(new HAClientImpl(name_))
    {
    }

    ///
    /// Create an HAClient wrapping the given implementation, used internally.
    /// \param body_ The implementation for the client to wrap.
    HAClient(HAClientImpl* body_) : Client((ClientImpl*)body_) {}

    ///
    /// Copy constructor, makes a new reference to the same body.
    /// \param rhs The HAClient to copy.
    HAClient(const HAClient& rhs) : Client((const Client&)rhs) {}

    ///
    /// Assignment operator, the body will be shared after this.
    /// \param rhs The HAClient to copy.
    HAClient& operator=(const HAClient& rhs)
    {
      Client::operator=((const Client&)rhs);
      return *this;
    }

    ///
    /// Set the timeout used when logging into AMPS.
    /// \param timeout_ The timeout, in milliseconds. 0 indicates no timeout.
    void setTimeout(int timeout_)
    {
      getBody().setTimeout(timeout_);
    }

    ///
    /// Get the current timeout used when attempting to log into AMPS.
    /// \return The number of milliseconds used for the logon timeout.
    int getTimeout() const
    {
      return getBody().getTimeout();
    }

    ///
    /// Set the delay between reconnect attempts in milliseconds.
    /// This method constructs a new FixedDelayStrategy with the specified
    /// reconnect time and sets that as the delay strategy for the class.
    /// \param reconnectDelay_ The number of milliseconds to wait between
    /// attempts to reconnect when disconnected.
    void setReconnectDelay(int reconnectDelay_)
    {
      getBody().setReconnectDelay((unsigned int)reconnectDelay_);
    }

    ///
    /// The current delay in milliseconds between reconnect attempts.
    /// \return The number of milliseconds to delay.
    int getReconnectDelay() const
    {
      return (int)(getBody().getReconnectDelay());
    }

    ///
    /// Set the ReconnectDelayStrategy used to control delay behavior
    /// when connecting and reconnecting to servers.
    ///
    /// \param strategy_   The ReconnectDelayStrategy to use when connecting
    ///                    and reconnecting to AMPS instances.
    ///
    void setReconnectDelayStrategy(const ReconnectDelayStrategy& strategy_)
    {
      getBody().setReconnectDelayStrategy(strategy_);
    }

    ///
    /// Get the ReconnectDelayStrategy used to control delay behavior
    /// when connecting and reconnecting to servers.
    ///
    /// \return  The ReconnectDelayStrategy used when connecting and
    ///          reconnecting to AMPS instances.
    ///
    ReconnectDelayStrategy getReconnectDelayStrategy(void) const
    {
      return getBody().getReconnectDelayStrategy();
    }

    ///
    /// Get the options passed to the server during logon.
    ///
    /// \return  The options passed to the server during logon.
    ///
    std::string getLogonOptions(void) const
    {
      return getBody().getLogonOptions();
    }

    ///
    /// Set the options passed to the server during logon.
    ///
    void setLogonOptions(const char* logonOptions_)
    {
      getBody().setLogonOptions(logonOptions_);
    }

    ///
    /// Set the options passed to the server during logon.
    ///
    void setLogonOptions(const std::string& logonOptions_)
    {
      getBody().setLogonOptions(logonOptions_);
    }

    ///
    /// Return the ServerChooser currently used by the HAClient.
    /// \return The current ServerChooser.
    ServerChooser getServerChooser() const
    {
      return getBody().getServerChooser();
    }

    ///
    /// Set the ServerChooser used to determine the URI used when trying to
    /// connect and logon to AMPS.
    /// \param serverChooser_ The ServerChooser instance to use.
    void setServerChooser(const ServerChooser& serverChooser_)
    {
      getBody().setServerChooser(serverChooser_);
    }

    ///
    /// Creates an HAClient with the given name that uses a memory-based Store
    /// for publish messages and a memory-based BookmarkStore for tracking
    /// messages received from bookmark subscriptions.
    /// This function is provided for convenience in cases where a
    /// given connection will both publish and use bookmark subscriptions.
    /// The client returned uses MemoryPublishStore and MemoryBookmarkStore, and is equivalent
    /// to constructing an HAClient and then setting those stores on the
    /// client. If the application is not both publishing and using a bookmark
    /// subscription, it is recommended that you create the client and set
    /// whichever store is appropriate. The MemoryPublishStore created by
    /// by this method has a capacity of 10000 blocks: if this is not a
    /// reasonable value for your application, create an HAClient and
    /// set the stores explicitly with the appropriate capacity.
    /// \param name_ Name for the client. This name is used for duplicate
    /// message detection and should be unique. AMPS does not enforce
    /// specific restrictions on the character set used, however some
    /// protocols (for example, XML) may not allow specific characters.
    /// 60East recommends that the client name be meaningful, short, human
    /// readable, and avoids using control characters, newline characters,
    /// or square brackets.
    /// \return The HAClient created with the name and the memory stores.
    static HAClient createMemoryBacked(const std::string& name_)
    {
      HAClient client(name_);
      client.setBookmarkStore(BookmarkStore(new MemoryBookmarkStore()));
      client.setPublishStore(Store(new MemoryPublishStore(10000)));
      return client;
    }

    /// Creates an HAClient with the given name that uses a file-based Store
    /// for publish messages that is named publishLogName_ and a file-based
    /// BookmarkStore for tracking messages received for bookmark subscriptions
    //  that is named subscribeLogName_.
    /// This function is provided for convenience in cases where a given
    /// connection will both publish to AMPS and use bookmark subscriptions.
    /// The client returned uses PublishStore and MMapBookmarkStore, and is
    /// equivalent to constructing an HAClient and then setting those stores
    /// on the client. If the application is not both publishing and using
    /// bookmark subscriptions, it is recommended that you create the
    /// client and set whichever store is appropriate.
    /// \param name_ Name for the client. This name is used for duplicate
    /// message detection and should be distinct for this set of messages,
    /// and consistent across invocations of the application that use the
    /// same publish store file. AMPS does not enforce
    /// specific restrictions on the character set used, however some
    /// protocols (for example, XML) may not allow specific characters.
    /// 60East recommends that the client name be meaningful, short, human
    /// readable, and avoid using control characters, newline characters,
    /// or square brackets.
    /// \param publishLogName_ The name for file used by the PublishStore.
    /// \param subscribeLogName_ The name for the file used by the
    /// LoggedBookmarkStore.
    /// \return The HAClient created with given values.
    static HAClient createFileBacked(const std::string& name_,
                                     const std::string& publishLogName_,
                                     const std::string& subscribeLogName_)
    {
      HAClient client(name_);
      client.setBookmarkStore(BookmarkStore(
                                new MMapBookmarkStore(subscribeLogName_)));
      client.setPublishStore(Store(new PublishStore(publishLogName_)));
      return client;
    }

    ///
    /// Connect and logon to AMPS using the server(s) from the ServerChooser
    /// set on this HAClient and the Authenticator set on this HAClient to
    /// to provide logon information. Will continue attempting to connect
    /// and logon until successful or ServerChooser returns an empty URI.
    void connectAndLogon()
    {
      getBody().connectAndLogon();
    }

    ///
    /// Return whether or not the client is disconnected.
    /// \return true if disconnected, false if connected.
    bool disconnected() const
    {
      return getBody().disconnected();
    }

    ///
    /// Get the connection information for the current connection.
    /// \return A ConnectionInfo object with the information describing the
    /// the current connection.
    ConnectionInfo getConnectionInfo() const
    {
      return getBody().getConnectionInfo();
    }

    ///
    /// \deprecated Use getConnectionInfo().
    /// Get the connection information for the current connection. This is
    /// a deprecated alias to getConnectionInfo.
    /// \return A ConnectionInfo object with the information describing the
    /// the current connection.
    ConnectionInfo gatherConnectionInfo() const // -V524
    {
      return getBody().getConnectionInfo();
    }

  private:
    // Must make disconnect handler a friend to call the internal version
    // of connectAndLogon so that the _disconnected flag won't get reset.
    friend void HAClientImpl::HADisconnectHandler::invoke(Client&, void*);

    ///
    /// Copy from a Client that is really an HAClient.
    /// \param rhs The Client to copy.
    HAClient(const Client& rhs)
      : Client(rhs)
    {
      assert(typeid(HAClientImpl) == typeid(_body.get()));
    }

    void connectAndLogonInternal()
    {
      getBody().connectAndLogonInternal();
    }

    const HAClientImpl& getBody() const
    {
      return dynamic_cast<const HAClientImpl&>(_body.get());
    }

    HAClientImpl& getBody()
    {
      return dynamic_cast<HAClientImpl&>(_body.get());
    }

  }; // class HAClient
  inline void HAClientImpl::HADisconnectHandler::invoke(Client& client, void*)
  {
    HAClient haClient(client);
    // Intentional disconnect, bail
    if (haClient.disconnected())
    {
      return;
    }
    DisconnectedException de("Disconnected");
    haClient.getServerChooser().reportFailure((AMPSException&)de,
                                              haClient.gatherConnectionInfo());
    try
    {
      haClient.connectAndLogonInternal();
    }
    catch ( AMPSException& )
    {
      // -V565
    }
  }

}// namespace AMPS

#endif //_HACLIENT_H_

