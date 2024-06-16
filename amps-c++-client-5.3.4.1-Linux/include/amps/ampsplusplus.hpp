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
#ifndef _AMPSPLUSPLUS_H_
#define _AMPSPLUSPLUS_H_
extern "C"{
  #include "amps/amps.h"
  #include "amps/ampsver.h"
  #include <string>
  #include <map>
  #include <sstream>
  #include <iostream>
  #include <memory>
  #include <stdexcept>
  #include <limits.h>
  #include <list>
  #include <memory>
  #include <set>
  #include <deque>
  #include <vector>
  #include <assert.h>
}

#ifndef _WIN32
  #include <inttypes.h>
#endif
#if defined(sun)
  #include <sys/atomic.h>
#endif
#include "amps/BookmarkStore.hpp"
#include "amps/MessageRouter.hpp"
#include "amps/util.hpp"
#include "amps/ampscrc.hpp"
#if __cplusplus >= 201100L || _MSC_VER >= 1900
#include <atomic>
#endif

#ifndef AMPS_TESTING_SLOW_MESSAGE_STREAM
  #define AMPS_TESTING_SLOW_MESSAGE_STREAM
#endif

///
/// @file ampsplusplus.hpp
///  @brief Core type, function, and class declarations for the AMPS C++ client.
///


///
/// @defgroup sync Synchronous Message Processing
///
/// These functions provide messages to the calling thread by using
/// the \ref AMPS::MessageStream class to return messages.
///

///
/// @defgroup async Asynchronous Message Processing
///
/// These functions process messages on a background thread by
/// calling the provided message handler.
/// \warning Notice that the AMPS client reuses the Message object provided to
///       the handler, so the contents of the object are reset between calls to
///       the handler. If you need to use the data outside of the call to the
///       message handler, you must make a copy of the data.
///

// For StoreBuffer implementations
#define AMPS_MEMORYBUFFER_DEFAULT_BUFFERS              10
#define AMPS_MEMORYBUFFER_DEFAULT_LENGTH            40960
#define AMPS_SUBSCRIPTION_MANAGER_DEFAULT_TIMEOUT       0
#define AMPS_HACLIENT_TIMEOUT_DEFAULT               10000
#define AMPS_HACLIENT_RECONNECT_DEFAULT               200
#define AMPS_DEFAULT_COMMAND_TIMEOUT                 5000
#define AMPS_DEFAULT_TOP_N                             -1
#define AMPS_DEFAULT_BATCH_SIZE                        10
#define AMPS_NUMBER_BUFFER_LEN                         20
#define AMPS_DEFAULT_QUEUE_ACK_TIMEOUT               1000

#if defined(_M_X64) || defined(__x86_64) || defined(_WIN64)
  #define AMPS_X64 1
#endif

#ifdef _WIN32
  static __declspec ( thread ) AMPS::Message* publishStoreMessage = 0;
#else
  static __thread AMPS::Message* publishStoreMessage = 0;
#endif

namespace AMPS
{

  typedef std::map<std::string, std::string> ConnectionInfo;

  class PerThreadMessageTracker
  {
    std::vector<AMPS::Message*> _messages;
  public:
    PerThreadMessageTracker() {}
    ~PerThreadMessageTracker()
    {
      for (size_t i = 0; i < _messages.size(); ++i)
      {
        delete _messages[i];
      }
    }
    void addMessage(AMPS::Message* message)
    {
      _messages.push_back(message);
    }
    static void addMessageToCleanupList(AMPS::Message* message)
    {
      static AMPS::Mutex _lock;
      AMPS::Lock<Mutex> l(_lock);
      _addMessageToCleanupList(message);
    }
    static void _addMessageToCleanupList(AMPS::Message* message)
    {
      static PerThreadMessageTracker tracker;
      tracker.addMessage(message);
    }
  };

  template<class Type>
  inline std::string asString(Type x_)
  {
    std::ostringstream os;
    os << x_;
    return os.str();
  }

  inline
  size_t convertToCharArray(char* buf_, amps_uint64_t seqNo_)
  {
    size_t pos = AMPS_NUMBER_BUFFER_LEN;
    for (int i = 0; i < AMPS_NUMBER_BUFFER_LEN; ++i)
    {
      if (seqNo_ > 0)
      {
        buf_[--pos] = (char)(seqNo_ % 10 + '0');
        seqNo_ /= 10;
      }
    }
    return pos;
  }

#ifdef _WIN32
  inline
  size_t convertToCharArray(char* buf_, unsigned long seqNo_)
  {
    size_t pos = AMPS_NUMBER_BUFFER_LEN;
    for (int i = 0; i < AMPS_NUMBER_BUFFER_LEN; ++i)
    {
      if (seqNo_ > 0)
      {
        buf_[--pos] = (char)(seqNo_ % 10 + '0');
        seqNo_ /= 10;
      }
    }
    return pos;
  }
#endif

///
/// Class to hold string versions of failure reasons
///
  class Reason
  {
  public:
    static const char* duplicate()
    {
      return "duplicate";
    }
    static const char* badFilter()
    {
      return "bad filter";
    }
    static const char* badRegexTopic()
    {
      return "bad regex topic";
    }
    static const char* subscriptionAlreadyExists()
    {
      return "subscription already exists";
    }
    static const char* nameInUse()
    {
      return "name in use";
    }
    static const char* authFailure()
    {
      return "auth failure";
    }
    static const char* notEntitled()
    {
      return "not entitled";
    }
    static const char* authDisabled()
    {
      return "authentication disabled";
    }
    static const char* subidInUse()
    {
      return "subid in use";
    }
    static const char* noTopic()
    {
      return "no topic";
    }
  };

///
/// Exception listener for unhandled exceptions.
///
/// When set for a client, the exceptionThrown method is called when an
/// exception occurs on the receive thread, and that exception is not
/// handled by the AMPS client. Typically, these are exceptions from
/// user-provided callbacks.
///
  class ExceptionListener
  {
  public:
    virtual ~ExceptionListener() {;}
    virtual void exceptionThrown(const std::exception&) const {;}
  };

  typedef ExceptionListener DefaultExceptionListener;


#define AMPS_CALL_EXCEPTION_WRAPPER(x) \
  try\
  {\
    x;\
  }\
  catch (std::exception& ex_)\
  {\
    try\
    {\
      _exceptionListener->exceptionThrown(ex_);\
    }\
    catch(...)\
    {\
      ;\
    }\
  }
  /*
   *  Note : we don't attempt to trap non std::exception exceptions
   *  here because doing so interferes with pthread_exit on some OSes.
  catch (...)\
  {\
    try\
    {\
      _exceptionListener->exceptionThrown(AMPS::AMPSException(\
          "An unhandled exception of unknown type was thrown by "\
          "the registered handler.", AMPS_E_USAGE));\
    }\
    catch(...)\
    {\
      ;\
    }\
  }
  */
#ifdef _WIN32
#define AMPS_CALL_EXCEPTION_WRAPPER_2(me,x) \
  try\
  {\
    while(me->_connected)\
    {\
      try\
      {\
        x;\
        break;\
      }\
      catch(MessageStreamFullException&)\
      {\
        me->checkAndSendHeartbeat(false);\
      }\
    }\
  }\
  catch (std::exception& ex_)\
  {\
    try\
    {\
      me->_exceptionListener->exceptionThrown(ex_);\
    }\
    catch(...)\
    {\
      ;\
    }\
  }
  /*
   *  Note : we don't attempt to trap non std::exception exceptions
   *  here because doing so interferes with pthread_exit on some OSes.
  catch (...)\
  {\
    try\
    {\
      me->_exceptionListener->exceptionThrown(AMPS::AMPSException(\
          "An unhandled exception of unknown type was thrown by "\
          "the registered handler.", AMPS_E_USAGE));\
    }\
    catch(...)\
    {\
      ;\
    }\
  }*/

#define AMPS_CALL_EXCEPTION_WRAPPER_STREAM_FULL_2(me, x)\
  while(me->_connected)\
  {\
    try\
    {\
      x;\
      break;\
    }\
    catch(MessageStreamFullException&)\
    {\
      me->checkAndSendHeartbeat(false);\
    }\
  }
#else
#define AMPS_CALL_EXCEPTION_WRAPPER_2(me,x) \
  try\
  {\
    while(me->_connected)\
    {\
      try\
      {\
        x;\
        break;\
      }\
      catch(MessageStreamFullException& ex_)\
      {\
        me->checkAndSendHeartbeat(false);\
      }\
    }\
  }\
  catch (std::exception& ex_)\
  {\
    try\
    {\
      me->_exceptionListener->exceptionThrown(ex_);\
    }\
    catch(...)\
    {\
      ;\
    }\
  }
  /*
   *  Note : we don't attempt to trap non std::exception exceptions
   *  here because doing so interferes with pthread_exit on some OSes.
  catch (...)\
  {\
    try\
    {\
      me->_exceptionListener->exceptionThrown(AMPS::AMPSException(\
          "An unhandled exception of unknown type was thrown by "\
          "the registered handler.", AMPS_E_USAGE));\
    }\
    catch(...)\
    {\
      ;\
    }\
  }*/

#define AMPS_CALL_EXCEPTION_WRAPPER_STREAM_FULL_2(me, x)\
  while(me->_connected)\
  {\
    try\
    {\
      x;\
      break;\
    }\
    catch(MessageStreamFullException& ex_)\
    {\
      me->checkAndSendHeartbeat(false);\
    }\
  }
#endif

#define AMPS_UNHANDLED_EXCEPTION(ex) \
  try\
  {\
    _exceptionListener->exceptionThrown(ex);\
  }\
  catch(...)\
  {;}

#define AMPS_UNHANDLED_EXCEPTION_2(me,ex) \
  try\
  {\
    me->_exceptionListener->exceptionThrown(ex);\
  }\
  catch(...)\
  {;}


  class Client;

///
///
/// Command is an encapsulation of a single AMPS command sent by the client.
/// Using Command you can build  valid commands to be executed to process
/// messages synchronously or asynchronously via the {@link Client} execute()
/// and executeAsync() methods.
/// Command is designed to be used as a "builder" enabling AMPS commands
/// to be built easily, for example:
///
/// <pre><code>
/// Client client(...);
/// // connect and logon
/// for(Message m : client.execute(Command("sow").setTopic("topic"))) { ... }
/// </code></pre>
///
/// The AMPS client is designed so that, in general, applications use the
/// Command object to build commands sent to AMPS. The client provides
/// instances of the Message object in return.
///
/// Command does not attempt to validate the command before it is submitted
/// to the AMPS server. Any header can be set on any Command, and the AMPS
/// client allows any value for any header.  See the AMPS %Command Reference
/// for details on how the AMPS Server interprets commands and which
/// headers and options are available on a given command type.

  class Command
  {
    Message _message;
    unsigned _timeout;
    unsigned _batchSize;
    unsigned _flags;
    static const unsigned Subscribe           = 1;
    static const unsigned SOW                 = 2;
    static const unsigned NeedsSequenceNumber = 4;
    static const unsigned ProcessedAck        = 8;
    static const unsigned StatsAck            = 16;
    void init(Message::Command::Type command_)
    {
      _timeout = 0;
      _batchSize = 0;
      _flags = 0;
      _message.reset();
      _message.setCommandEnum(command_);
      _setIds();
    }
    void init(const std::string& command_)
    {
      _timeout = 0;
      _batchSize = 0;
      _flags = 0;
      _message.reset();
      _message.setCommand(command_);
      _setIds();
    }
    void init(const char* command_, size_t commandLen_)
    {
      _timeout = 0;
      _batchSize = 0;
      _flags = 0;
      _message.reset();
      _message.setCommand(command_, commandLen_);
      _setIds();
    }
    void _setIds(void)
    {
      Message::Command::Type command = _message.getCommandEnum();
      if (!(command & Message::Command::NoDataCommands))
      {
        _message.newCommandId();
        if (command == Message::Command::Subscribe ||
            command == Message::Command::SOWAndSubscribe ||
            command == Message::Command::DeltaSubscribe ||
            command == Message::Command::SOWAndDeltaSubscribe)
        {
          _message.setSubscriptionId(_message.getCommandId());
          _flags |= Subscribe;
        }
        if (command == Message::Command::SOW
            || command == Message::Command::SOWAndSubscribe
            || command == Message::Command::SOWAndDeltaSubscribe)
        {
          _message.setQueryID(_message.getCommandId());
          if (_batchSize == 0)
          {
            setBatchSize(AMPS_DEFAULT_BATCH_SIZE);
          }
          if (command == Message::Command::SOW)
          {
            _flags |= SOW;
          }
        }
        _flags |= ProcessedAck;
      }
      else if (command == Message::Command::SOWDelete)
      {
        _message.newCommandId();
        _flags |= ProcessedAck;
        _flags |= NeedsSequenceNumber;
      }
      else if (command == Message::Command::Publish
               || command == Message::Command::DeltaPublish)
      {
        _flags |= NeedsSequenceNumber;
      }
      else if (command == Message::Command::StopTimer)
      {
        _message.newCommandId();
      }
    }
  public:
    ///
    /// Creates an object to represent the given AMPS command, such as "sow" or "subscribe".
    /// \param command_ The AMPS command to be created.
    Command(const std::string& command_)
    {
      init(command_);
    }
    ///
    /// Creates an object to represent the given AMPS command, such as "sow" or "subscribe".
    /// \param command_ The AMPS command to be created.
    /// \param commandLen_ The length of the new AMPS command to be created.
    Command(const char* command_, size_t commandLen_)
    {
      init(command_, commandLen_);
    }
    ///
    /// Creates an object to represent the given AMPS command, such as "sow" or "subscribe".
    /// \param command_ The AMPS command to be created.
    Command(Message::Command::Type command_)
    {
      init(command_);
    }

    ///
    /// Resets the fields of self, and sets the command to command_.
    /// \param command_ The new AMPS command to be created.
    Command& reset(const std::string& command_)
    {
      init(command_);
      return *this;
    }
    ///
    /// Resets the fields of self, and sets the command to command_.
    /// \param command_ The new AMPS command to be created.
    /// \param commandLen_ The length of the new AMPS command to be created.
    Command& reset(const char* command_, size_t commandLen_)
    {
      init(command_, commandLen_);
      return *this;
    }
    ///
    /// Resets the fields of self, and sets the command to command_.
    /// \param command_ The new AMPS command to be created.
    Command& reset(Message::Command::Type command_)
    {
      init(command_);
      return *this;
    }
    /// Sets the SowKey field of the command, typically used for a publish
    /// command to a topic in the state of the world when that topic is
    /// configured to require the publisher to set the SowKey. For other
    /// commands (for example, "sow" or "sow_delete"), use the setSowKeys
    /// function to set the SowKeys header.  See the AMPS %Command Reference for
    /// details.
    /// \param sowKey_ the SowKey field of this command.
    Command& setSowKey(const std::string& sowKey_)
    {
      _message.setSowKey(sowKey_);
      return *this;
    }
    /// Sets the SowKey field of the command, typically used for a publish
    /// command to a topic in the state of the world when that topic is
    /// configured to require the publisher to set the SowKey. For other
    /// commands (for example, "sow" or "sow_delete"), use the setSowKeys
    /// function to set the SowKeys header.  See the AMPS %Command Reference for
    /// details.
    /// \param sowKey_ the SowKey field of this command.
    /// \param sowKeyLen_ the length of the SowKey field of this command.
    Command& setSowKey(const char* sowKey_, size_t sowKeyLen_)
    {
      _message.setSowKey(sowKey_, sowKeyLen_);
      return *this;
    }
    /// Sets the SowKeys for the command. When applicable, setting the
    /// SowKeys for a command limits the results of the command to only
    /// messages with one of the specified SowKeys. The SowKeys for a command
    /// are a comma-separated list of the keys that AMPS assigns to SOW messages.
    ///
    ///  The SowKey for a message is available through the
    ///  {@link Message.getSowKey } function on a message.
    ///
    /// See the AMPS User Guide and AMPS %Command Reference for details
    /// on when AMPS will interpret this header on a command.
    ///
    /// \param sowKeys_ the SOWKeys field of this command.
    Command& setSowKeys(const std::string& sowKeys_)
    {
      _message.setSowKeys(sowKeys_);
      return *this;
    }
    /// Sets the SowKeys for the command. When applicable, setting the
    /// SowKeys for a command limits the results of the command to only
    /// messages with one of the specified SowKeys. The SowKeys for a command
    /// are a comma-separated list of the keys that AMPS assigns to SOW messages.
    ///
    ///  The SowKey for a message is available through the
    ///  {@link Message.getSowKey } function on a message.
    ///
    /// See the AMPS User Guide and AMPS %Command Reference for details
    /// on when AMPS will interpret this header on a command.
    ///
    /// \param sowKeys_ the SOWKeys field of this command.
    /// \param sowKeysLen_ the length of the SOWKeys field of this command.
    Command& setSowKeys(const char* sowKeys_, size_t sowKeysLen_)
    {
      _message.setSowKeys(sowKeys_, sowKeysLen_);
      return *this;
    }
    /// \param cmdId_ the command ID for this command. For relevant commands, one is generated for you if you do not supply one.
    Command& setCommandId(const std::string& cmdId_)
    {
      _message.setCommandId(cmdId_);
      return *this;
    }
    /// \param cmdId_ the command ID for this command. For relevant commands, one is generated for you if you do not supply one.
    /// \param cmdIdLen_ the length of the command ID for this command.
    Command& setCommandId(const char* cmdId_, size_t cmdIdLen_)
    {
      _message.setCommandId(cmdId_, cmdIdLen_);
      return *this;
    }
    /// \param topic_ the topic for this command.
    Command& setTopic(const std::string& topic_)
    {
      _message.setTopic(topic_);
      return *this;
    }
    /// \param topic_ the topic for this command.
    /// \param topicLen_ the length of the topic for this command.
    Command& setTopic(const char* topic_, size_t topicLen_)
    {
      _message.setTopic(topic_, topicLen_);
      return *this;
    }
    /// \param filter_ the filter for this command.
    Command& setFilter(const std::string& filter_)
    {
      _message.setFilter(filter_);
      return *this;
    }
    /// \param filter_ the filter for this command.
    /// \param filterLen_ the length of the filter for this command.
    Command& setFilter(const char* filter_, size_t filterLen_)
    {
      _message.setFilter(filter_, filterLen_);
      return *this;
    }
    /// \param orderBy_ the order by clause for this command.
    Command& setOrderBy(const std::string& orderBy_)
    {
      _message.setOrderBy(orderBy_);
      return *this;
    }
    /// \param orderBy_ the order by clause for this command.
    /// \param orderByLen_ the length of the order by clause for this command.
    Command& setOrderBy(const char* orderBy_, size_t orderByLen_)
    {
      _message.setOrderBy(orderBy_, orderByLen_);
      return *this;
    }
    /// \param subId_ the subscription ID for this command. For relevant commands, one is generated for you if you do not supply one.
    Command& setSubId(const std::string& subId_)
    {
      _message.setSubscriptionId(subId_);
      return *this;
    }
    /// \param subId_ the subscription ID for this command. For relevant commands, one is generated for you if you do not supply one.
    /// \param subIdLen_ the length of the subscription ID for this command.
    Command& setSubId(const char* subId_, size_t subIdLen_)
    {
      _message.setSubscriptionId(subId_, subIdLen_);
      return *this;
    }
    /// \param queryId_ the query ID for this command. For relevant commands, one is generated for you if you do not supply one.
    Command& setQueryId(const std::string& queryId_)
    {
      _message.setQueryId(queryId_);
      return *this;
    }
    /// \param queryId_ the query ID for this command. For relevant commands, one is generated for you if you do not supply one.
    /// \param queryIdLen_ the length of the query ID for this command.
    Command& setQueryId(const char* queryId_, size_t queryIdLen_)
    {
      _message.setQueryId(queryId_, queryIdLen_);
      return *this;
    }
    /// Set the bookmark to be used this command. For a subscription, this can be either a specific
    /// bookmark, a bookmark list, or one of the @ref specialBookmarks. For an acknowledgement
    /// of a queue message, this can be a specific bookmark or a bookmark list.
    /// See the AMPS %Command Reference for details on when a bookmark is applicable.
    /// \param bookmark_ the bookmark for this command.
    Command& setBookmark(const std::string& bookmark_)
    {
      _message.setBookmark(bookmark_);
      return *this;
    }
    /// Set the bookmark to be used this command. For a subscription, this can be either a specific
    /// bookmark, a bookmark list, or one of the @ref specialBookmarks. For an acknowledgement
    /// of a queue message, this can be a specific bookmark or a bookmark list.
    /// See the AMPS %Command Reference for details on when a bookmark is applicable.
    /// \param bookmark_ the bookmark for this command.
    /// \param bookmarkLen_ the length of the bookmark for this command.
    Command& setBookmark(const char* bookmark_, size_t bookmarkLen_)
    {
      _message.setBookmark(bookmark_, bookmarkLen_);
      return *this;
    }
    /// Set the correlation ID for this command. The correlation ID is an
    /// arbitrary string that can be used for whatever purpose the application
    /// needs. It is not interpreted or used by AMPS. However, the
    /// correlation ID must only contain characters that are valid base64
    /// encoded characters.
    /// \param correlationId_ the correlation ID for this command.
    Command& setCorrelationId(const std::string& correlationId_)
    {
      _message.setCorrelationId(correlationId_);
      return *this;
    }
    /// Set the correlation ID for this command. The correlation ID is an
    /// arbitrary string that can be used for whatever purpose the application
    /// needs. It is not interpreted or used by AMPS. However, the
    /// correlation ID must only contain characters that are valid base64
    /// encoded characters.
    /// \param correlationId_ the correlation ID for this command.
    /// \param correlationIdLen_ the length of the correlation ID for this command.
    Command& setCorrelationId(const char* correlationId_, size_t correlationIdLen_)
    {
      _message.setCorrelationId(correlationId_, correlationIdLen_);
      return *this;
    }
    /// Sets the options string for this command: see {@link Message.Options } for a helper class for constructing the string.
    /// \param options_ the options string for this command.
    Command& setOptions(const std::string& options_)
    {
      _message.setOptions(options_);
      return *this;
    }
    /// Sets the options string for this command: see {@link Message.Options } for a helper class for constructing the string.
    /// \param options_ the options string for this command.
    /// \param optionsLen_ the length of the options string for this command.
    Command& setOptions(const char* options_, size_t optionsLen_)
    {
      _message.setOptions(options_, optionsLen_);
      return *this;
    }
    /// \param seq_ the sequence for this command. If the client has a publish store, the sequence from the store will be used regardless of if you set this.
    Command& setSequence(const std::string& seq_)
    {
      _message.setSequence(seq_);
      return *this;
    }
    /// \param seq_ the sequence for this command. If the client has a publish store, the sequence from the store will be used regardless of if you set this.
    /// \param seqLen_ the length of the sequence for this command.
    Command& setSequence(const char* seq_, size_t seqLen_)
    {
      _message.setSequence(seq_, seqLen_);
      return *this;
    }
    /// \param seq_ the sequence for this command. If the client has a publish store, the sequence from the store will be used regardless of if you set this.
    Command& setSequence(const amps_uint64_t seq_)
    {
      std::ostringstream os;
      os << seq_;
      _message.setSequence(os.str());
      return *this;
    }
    amps_uint64_t getSequence() const
    {
      return amps_message_get_field_uint64(_message.getMessage(), AMPS_Sequence);
    }
    /// Sets the data for this command from an existing string.
    /// \param data_ the data for this command.
    Command& setData(const std::string& data_)
    {
      _message.setData(data_);
      return *this;
    }
    /// Sets the data for this command.
    /// \param data_ the data for this command.
    /// \param dataLen_ the length, in bytes, of your data (excluding any null-terminator)
    Command& setData(const char* data_, size_t dataLen_)
    {
      _message.setData(data_, dataLen_);
      return *this;
    }
    /// Sets the client-side timeout for this command. If the client does not
    /// process an acknowledgement for the command within the specified
    /// timeout, the client throws an exception. Notice that the timeout
    /// is monitored on the AMPS client, not on the server, and the
    /// acknowledgement is processed on the client receive thread.
    ///
    /// A timeout value of 0 indicates that the client should wait for
    /// a "processed" ack to be returned from the server without timing out.
    /// \param timeout_ the timeout for this command in milliseconds. By default, the timeout is 0, indicating that the command will block until a "processed" ack is returned and consumed by the client receive thread.
    Command& setTimeout(unsigned timeout_)
    {
      _timeout = timeout_;
      return *this;
    }
    /// \param topN_ the "top N" field for this command. This value limits the number of records returned from a SOW query to the first N records.
    Command& setTopN(unsigned topN_)
    {
      _message.setTopNRecordsReturned(topN_);
      return *this;
    }
    /// Sets the batch size for this command, which controls how many records
    /// are sent together in the results of a SOW query. See the AMPS User
    /// Guide for details.
    /// \param batchSize_ the batch size for this command
    Command& setBatchSize(unsigned batchSize_)
    {
      _message.setBatchSize(batchSize_);
      _batchSize = batchSize_;
      return *this;
    }
    /// Set the expiration time for a publish command. For publishes to a
    /// Topic in the state of the world that has expiration enabled,
    /// this specifies how long to retain this publish in the state of the
    /// world (if expiration is
    /// not enabled for that topic, this value is ignored for that topic).
    /// For a message that will be delivered from a queue, this value
    /// sets the length of time that the message will be available to be
    /// delivered from the queue. Notice that this value does not affect
    /// storage in the transaction log.
    /// \param expiration_ the expiration time for this publish, in seconds
    Command& setExpiration(unsigned expiration_)
    {
      _message.setExpiration(expiration_);
      return *this;
    }
    /// \param ackType_ add an ack type to this command, such as "stats" or "completed". Additional acks are returned in the message stream.
    Command& addAckType(const std::string& ackType_)
    {
      _message.setAckType(_message.getAckType() + "," + ackType_);
      if (ackType_ == "processed")
      {
        _flags |= ProcessedAck;
      }
      else if (ackType_ == "stats")
      {
        _flags |= StatsAck;
      }
      return *this;
    }
    /// \param ackType_ set the ack type for this command, such as "stats,completed" or "processed,completed". Additional acks are returned in the message stream.
    Command& setAckType(const std::string& ackType_)
    {
      _message.setAckType(ackType_);
      if (ackType_.find("processed") != std::string::npos)
      {
        _flags |= ProcessedAck;
      }
      else
      {
        _flags &= ~ProcessedAck;
      }
      if (ackType_.find("stats") != std::string::npos)
      {
        _flags |= StatsAck;
      }
      else
      {
        _flags &= ~StatsAck;
      }
      return *this;
    }
    /// \param ackType_ set the ack type for this command, such as `Message::AckType::Stats``|``Message::AckType::Completed` or `Message::AckType::Processed``|``Message::AckType::Completed`. Additional acks are returned in the message stream.
    Command& setAckType(unsigned ackType_)
    {
      _message.setAckTypeEnum(ackType_);
      if (ackType_ & Message::AckType::Processed)
      {
        _flags |= ProcessedAck;
      }
      else
      {
        _flags &= ~ProcessedAck;
      }
      if (ackType_ & Message::AckType::Stats)
      {
        _flags |= StatsAck;
      }
      else
      {
        _flags &= ~StatsAck;
      }
      return *this;
    }
    /// \return The ack type set on this command.
    std::string getAckType() const
    {
      return (std::string)(_message.getAckType());
    }
    /// \return The ack type enum set on this command.
    unsigned getAckTypeEnum() const
    {
      return _message.getAckTypeEnum();
    }

    Message& getMessage(void)
    {
      return _message;
    }
    unsigned getTimeout(void) const
    {
      return _timeout;
    }
    unsigned getBatchSize(void) const
    {
      return _batchSize;
    }
    bool isSubscribe(void) const
    {
      return _flags & Subscribe;
    }
    bool isSow(void) const
    {
      return (_flags & SOW) != 0;
    }
    bool hasProcessedAck(void) const
    {
      return (_flags & ProcessedAck) != 0;
    }
    bool hasStatsAck(void) const
    {
      return (_flags & StatsAck) != 0;
    }
    bool needsSequenceNumber(void) const
    {
      return (_flags & NeedsSequenceNumber) != 0;
    }
  };

///
/// A function pointer type for disconnect-handler functions.
  typedef void(*DisconnectHandlerFunc)(Client&, void* userData);

  class Message;
  typedef Handler<DisconnectHandlerFunc, Client&> DisconnectHandler;

///
/// The interface for handling authentication with the AMPS server.
  class Authenticator
  {
  public:
    virtual ~Authenticator() {;}

    ///
    /// Called by Client just before the logon command is sent.
    /// \param userName_ The current value of the user name from the URI.
    /// \param password_ The current value of the password from the URI.
    /// \return The value that should be placed into the Password header field of the logon command.
    virtual std::string authenticate(const std::string& userName_, const std::string& password_) = 0;
    ///
    /// Called by Client when a logon ack is received with a status of retry.
    /// Client will continue trying to logon as long as the server returns
    /// retry and this method succeeds.
    /// \param userName_ The user name returned in the server's ACK message.
    /// \param password_ The password or token returned in the server's ACK message.
    /// \return The value that should be placed into the Password header field of the next logon command.
    virtual std::string retry(const std::string& userName_, const std::string& password_) = 0;
    ///
    /// Called by Client once a logon completes successfully.
    /// \param userName_ The user name that successfully logged on to the server.
    /// \param password_ The password that successfully logged on to the server.
    /// \param reason_ The reason for successful completion, taken from the
    /// message returned by the server.
    virtual void completed(const std::string& userName_, const std::string& password_, const std::string& reason_) = 0;
  };

///
/// A default implementation of Authenticator that only uses an unchanged
/// password and does not implement retry.
  class DefaultAuthenticator : public Authenticator
  {
  public:
    virtual ~DefaultAuthenticator() {;}
    ///
    /// A simple implementation that returns an unmodified password.
    std::string authenticate(const std::string& /*userName_*/, const std::string& password_)
    {
      return password_;
    }

    ///
    /// Throws an AuthenticationException because retry is not implemented.
    std::string retry(const std::string& /*userName_*/, const std::string& /*password_*/)
    {
      throw AuthenticationException("retry not implemented by DefaultAuthenticator.");
    }

    void completed(const std::string& /*userName_*/, const std::string& /* password_ */, const std::string& /* reason */) {;}

    ///
    /// Static function to return a static instance used when no Authenticator
    /// is supplied to a Client.
    static Authenticator& instance()
    {
      static DefaultAuthenticator d;
      return d;
    }
  };

///
/// Abstract base class for replaying a publish message
  class StoreReplayer
  {
  public:

    ///
    /// Called by implementations of Store to replay a message from the store.
    /// \param message_ The Message to be replayed.
    virtual void execute(Message& message_) = 0;

    virtual ~StoreReplayer() {;}
  };

  class Store;

///
/// Function type for PublishStore resize events
/// The store_ param is store which is resizing.
/// The size_ is the number of bytes being requested for the new size.
/// The userData_ is the userData_ that was set when the handler was set on the store.
/// The return value should be true if resize should proceed and false if the
/// the size should be unchanged. A false value should only be returned if some
/// other action was taken to free up space within the Store.
  typedef bool (*PublishStoreResizeHandler)(Store store_,
                                            size_t size_,
                                            void* userData_);

///
/// Abstract base class for storing published messages for an HA publisher client.
  class StoreImpl : public RefBody
  {
  public:
    ///
    /// Default constructor
    /// \param errorOnPublishGap_ If true, PublishStoreGapException can be
    /// thrown by the store if the client logs onto a server that appears
    /// to be missing messages no longer held in the store.
    StoreImpl(bool errorOnPublishGap_ = false)
      : _resizeHandler(NULL)
      , _resizeHandlerData(NULL)
      , _errorOnPublishGap(errorOnPublishGap_)
    {;}

    ///
    /// Called by Client to store a message being published.
    /// \param message_ The message to store.
    /// \return The sequence number for the message.
    virtual amps_uint64_t store(const Message& message_) = 0;

    ///
    /// Called by Client to indicate that all messages up to and including
    /// \param index_ have been successfully published to the server and are no longer
    /// required to be in the store.
    /// \throw PublishStoreGapException If index_ < getLastPersisted() which could
    /// leave a gap on the server of missing messages from this Client.
    virtual void discardUpTo(amps_uint64_t index_) = 0;

    ///
    /// Called by Client to get all stored and non-discarded messages replayed
    /// by the store onto the StoreReplayer.
    /// \param replayer_ The StoreReplayer to be used to replay the messages.
    virtual void replay(StoreReplayer& replayer_) = 0;

    ///
    /// Called by Client to get a single message replayed by the store onto
    /// the StoreReplayer.
    /// \param replayer_ The StoreReplayer to be used to replay the messages.
    /// \param index_ The index of the message to replay.
    /// \return Returns true for success, false for failure such as an
    /// invalid index.
    virtual bool replaySingle(StoreReplayer& replayer_, amps_uint64_t index_) = 0;

    ///
    /// Method to return how many messages are in the store that have not been discarded,
    /// indicating that they are not yet persisted by the AMPS server.
    /// \return The number of messages not yet persisted by the AMPS server.
    virtual size_t unpersistedCount() const = 0;

    virtual ~StoreImpl() {;}

    ///
    /// Method to wait for the Store to discard everything that has been
    /// stored up to the point in time when flush is called. It will get
    /// the current max and wait up to timeout for that message to be discarded
    /// \param timeout_ The number of milliseconds to wait for the messages to be acknowledged by AMPS and discarded by the Store. 0 indicates no timeout.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException The publish command was not acked in the allowed time.
    virtual void flush(long timeout_) = 0;

    ///
    /// Method to return the value used to represent not found or unset.
    static inline size_t getUnsetPosition()
    {
      return AMPS_UNSET_INDEX;
    }

    ///
    /// Method to return the value used to represent no such sequence.
    static inline amps_uint64_t getUnsetSequence()
    {
      return AMPS_UNSET_SEQUENCE;
    }

    ///
    /// Get the oldest unpersisted message sequence in the store.
    /// \return The sequence of the oldest message.
    virtual amps_uint64_t getLowestUnpersisted() const = 0;

    ///
    /// Get the last persisted sequence number.
    /// \return The sequence of the last persisted message.
    virtual amps_uint64_t getLastPersisted() = 0;

    ///
    /// Set a handler to be called if the Store needs to resize in order to keep
    /// storing messages. Resize could be caused by not being connected to the server,
    /// publishing messages faster than the network or AMPS can keep up, not
    /// properly discarding  messages when receiving persisted acks, or AMPS
    /// being unable to complete persistence such as when its connection to a sync
    /// replication destination is unavailable.
    /// \param handler_ The handler to be called when a resize event is required.
    /// \param userData_ The data to pass to the handler each time it is called.
    inline virtual void setResizeHandler(PublishStoreResizeHandler handler_,
                                         void* userData_)
    {
      _resizeHandler = handler_;
      _resizeHandlerData = userData_;
    }

    inline virtual PublishStoreResizeHandler getResizeHandler() const
    {
      return _resizeHandler;
    }

    bool callResizeHandler(size_t newSize_);

    inline virtual void setErrorOnPublishGap(bool errorOnPublishGap_)
    {
      _errorOnPublishGap = errorOnPublishGap_;
    }

    inline virtual bool getErrorOnPublishGap() const
    {
      return _errorOnPublishGap;
    }

  private:
    PublishStoreResizeHandler _resizeHandler;
    void*                     _resizeHandlerData;
    bool                      _errorOnPublishGap;
  };

///
/// Handle class for StoreImpl classes that track publish messages.
  class Store
  {
    RefHandle<StoreImpl> _body;
  public:
    Store() {;}
    Store(StoreImpl* body_) : _body(body_) {;}
    Store(const Store& rhs) : _body(rhs._body) {;}
    Store& operator=(const Store& rhs)
    {
      _body = rhs._body;
      return *this;
    }

    ///
    /// Called by Client to store a message being published.
    /// \param message_ The message to store.
    amps_uint64_t store(const Message& message_)
    {
      return _body.get().store(message_);
    }

    ///
    /// Called by Client to indicate that all messages up to and including
    /// \param index_ have been successfully published to the server and are no longer
    /// required to be in the store.
    /// \throw PublishStoreGapException If index_ < getLastPersisted() which could
    /// leave a gap on the server of missing messages from this Client.
    void discardUpTo(amps_uint64_t index_)
    {
      _body.get().discardUpTo(index_);
    }

    ///
    /// Called by Client to get all stored and non-discarded messages replayed
    /// by the store onto the StoreReplayer.
    /// \param replayer_ The StoreReplayer to be used to replay the messages.
    void replay(StoreReplayer& replayer_)
    {
      _body.get().replay(replayer_);
    }

    ///
    /// Called by Client to get a single message replayed by the store onto
    /// the StoreReplayer.
    /// \param replayer_ The StoreReplayer to be used to replay the messages.
    /// \param index_ The index of the message to replay.
    /// \return Returns true for success, false for failure such as an
    /// invalid index.
    bool replaySingle(StoreReplayer& replayer_, amps_uint64_t index_)
    {
      return _body.get().replaySingle(replayer_, index_);
    }

    ///
    /// Method to return how many messages are in the store that have not been discarded,
    /// indicating that they are not yet persisted by the AMPS server.
    /// \return The number of messages not yet persisted by the AMPS server.
    size_t unpersistedCount() const
    {
      return _body.get().unpersistedCount();
    }

    ///
    /// Method to return if there is an underlying implementation for the Store.
    /// \return Returns true if there is an implementation or false if an empty handle.
    bool isValid() const
    {
      return _body.isValid();
    }

    ///
    /// Method to wait for the Store to discard everything that has been
    /// stored up to the point in time when flush is called. It will get
    /// the current max and wait up to timeout for that message to be discarded
    /// \param timeout_ The number of milliseconds to wait for the messages to be acknowledged by AMPS and discarded by the Store. 0 indicates no timeout.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException The publish command was not acked in the allowed time.
    void flush(long timeout_ = 0)
    {
      return _body.get().flush(timeout_);
    }

    ///
    /// Get the oldest unpersisted message sequence in the store.
    /// \return The sequence of the oldest message.
    amps_uint64_t getLowestUnpersisted()
    {
      return _body.get().getLowestUnpersisted();
    }

    ///
    /// Get the last persisted message sequence in the store.
    /// \return The sequence of the last persisted message.
    amps_uint64_t getLastPersisted()
    {
      return _body.get().getLastPersisted();
    }

    ///
    /// Set a handler to be called if the Store needs to resize in order to keep
    /// storing messages. Resize could be caused by not being connected to the server,
    /// publishing messages faster than the network or AMPS can keep up, not
    /// properly discarding  messages when receiving persisted acks, or AMPS
    /// being unable to complete persistence such as when its connection to a sync
    /// replication destination is unavailable.
    /// \param handler_ The handler to be called when a resize event is required.
    /// \param userData_ The data to pass to the handler each time it is called.
    void setResizeHandler(PublishStoreResizeHandler handler_,
                          void* userData_)
    {
      _body.get().setResizeHandler(handler_, userData_);
    }

    PublishStoreResizeHandler getResizeHandler() const
    {
      return _body.get().getResizeHandler();
    }

    /// Called to enable or disable throwing PublishStoreGapException.
    /// \param errorOnPublishGap_ If true, PublishStoreGapException can be
    /// thrown by the store if the client logs onto a server that appears
    /// to be missing messages no longer held in the store.
    inline void setErrorOnPublishGap(bool errorOnPublishGap_)
    {
      _body.get().setErrorOnPublishGap(errorOnPublishGap_);
    }

    /// Called to check if the Store will throw PublishStoreGapException.
    /// \return If true, PublishStoreGapException can be
    /// thrown by the store if the client logs onto a server that appears
    /// to be missing messages no longer held in the store.
    inline bool getErrorOnPublishGap() const
    {
      return _body.get().getErrorOnPublishGap();
    }

    ///
    /// Used to get a pointer to the implementation.
    /// \return The StoreImpl* for this store's implementation.
    StoreImpl* get()
    {
      if (_body.isValid())
      {
        return &_body.get();
      }
      else
      {
        return NULL;
      }
    }

  };

///
///
/// Class to handle when a client receives a duplicate publish message, or not entitled message.
/// The default implementation just ignores the duplicates.
  class FailedWriteHandler
  {
  public:
    virtual ~FailedWriteHandler() {;}
    ///
    /// Called when the server indicates a message could not be written.
    /// If the message is present in the local Store, it is included.
    /// \param message_       The message from the store, or an unset message.
    /// \param reason_        The reason for the failure as returned by AMPS.
    /// \param reasonLength_  The length, in bytes, of the reason.
    virtual void failedWrite(const Message& message_,
                             const char* reason_, size_t reasonLength_) = 0;
  };


  inline bool StoreImpl::callResizeHandler(size_t newSize_)
  {
    if (_resizeHandler)
    {
      return _resizeHandler(Store(this), newSize_, _resizeHandlerData);
    }
    return true;
  }

///
/// PublishStoreResizeHandler that will block up to the timeout specified in
/// user data milliseconds trying to flush the store.
/// WARNING: This is not a good practice! At a minimum, these resize events should
/// be getting logged. You have no guarantee that the store size will shrink
/// during the call to flush, so the store could continue to grow infinitely.
  inline bool DangerousFlushPublishStoreResizeHandler(Store store_, size_t /*size_*/,
                                                      void* data_)
  {
    long* timeoutp = (long*)data_;
    size_t count = store_.unpersistedCount();
    if (count == 0)
    {
      return false;
    }
    try
    {
      store_.flush(*timeoutp);
    }
#ifdef _WIN32
    catch (const TimedOutException&)
#else
    catch (const TimedOutException& e)
#endif
    {
      return true;
    }
    return (count == store_.unpersistedCount());
  }

///
/// Abstract base class where you can implement handling of exceptions that
/// occur when a SubscriptionManager implementation is attempting to restart
/// subscriptions following a connection failure.
class FailedResubscribeHandler
{
public:
  /// Implement this function to return true if the subscription should be
  /// removed from the SubscriptionManager and false if it should be left and
  /// retried after the next failover. If any return false, then the current
  /// connection where the failure occured will be dropped and the reconnect
  /// process restarted.
  /// \param message_ The Message used to place the subscription.
  /// \param handler_ The MessageHandler for the subscription.
  /// \param requestedAckTypes_ The requested ack types for the handler.
  /// \param exception_ The exception returned with the failure.
  /// \return True to remove the subscription from the SubscriptionManager
  /// or false to leave it in place for the next attempt.
  virtual bool failure(const Message& message_, const MessageHandler& handler_,
                       unsigned requestedAckTypes_,
                       const AMPSException& exception_) = 0;
};

///
/// Abstract base class to manage all subscriptions placed on a client so that they
/// can be re-established if the client is disconnected and then reconnected.
  class SubscriptionManager
  {
  public:
    virtual ~SubscriptionManager() {;}
    ///
    /// Called by Client when a subscription is placed. Not all parameters are
    /// appropriate for all subscriptions, so the operation type determines
    /// which ones are required and/or useful.
    /// \param messageHandler_ The MessageHandler for the subscription.
    /// \param message_ The Message containing the subscription to reissue.
    /// \param requestedAckTypes_ The ack types requested for the handler.
    virtual void subscribe(MessageHandler messageHandler_, const Message& message_,
                           unsigned requestedAckTypes_) = 0;
    ///
    /// Called by Client when a subscription is unsubscribed.
    /// \param subId_ The identifier of the subscription being unsubscribed.
    virtual void unsubscribe(const Message::Field& subId_) = 0;
    ///
    /// Clear subscriptions and reset to the initial state.
    virtual void clear() = 0;
    ///
    /// Called by Client to get all subscriptions placed again.
    /// \param client_ The Client on which to place all subscriptions.
    virtual void resubscribe(Client& client_) = 0;
    ///
    /// Set a handler to deal with failing subscriptions after a failover
    /// event.
    /// \param handler_ The handler on which to call `failure`.
    virtual void setFailedResubscribeHandler(std::shared_ptr<FailedResubscribeHandler> handler_)
    {
      _failedResubscribeHandler = handler_;
    }
  protected:
    std::shared_ptr<FailedResubscribeHandler> _failedResubscribeHandler;
  };

/// \brief Abstract base class for connection state listeners.
/// To implement a connection state listener, derive from this
/// class and implement the connectionStateChanged method.

  class ConnectionStateListener
  {
  public:
    /// Constants for the state of the connection.
    typedef enum { Disconnected = 0,
                   Shutdown = 1,
                   Connected = 2,
                   LoggedOn = 4,
                   PublishReplayed = 8,
                   HeartbeatInitiated = 16,
                   Resubscribed = 32,
                   UNKNOWN = 16384
                 } State;

    /// Pure virtual method for receiving the change in connection state.
    /// Override this in a derived class to receive notification of
    /// changes in connection state. Notice that this method is called
    /// as soon as the client detects the change of state. This means that,
    /// for example, when the method is called because the client is
    /// connected, the client has not yet logged on or completed any
    /// recovery (such as replaying the publish store, re-entering
    /// subscriptions, and so on). The listener should not issue commands
    /// on the client while recovery is in progress.
    virtual void connectionStateChanged(State newState_) = 0;
    virtual ~ConnectionStateListener()  {;};
  };


  class MessageStreamImpl;
  class MessageStream;

  typedef void(*DeferredExecutionFunc)(void*);

  class ClientImpl : public RefBody // -V553
  {
    // Class to wrap turning of Nagle for things like flush and logon
    class NoDelay
    {
      private:
        AMPS_SOCKET _socket;
        int _noDelay;
        char* _valuePtr;
#ifdef _WIN32
        int _valueLen;
#else
        socklen_t _valueLen;
#endif
      public:
        NoDelay(amps_handle client_)
        : _socket(AMPS_INVALID_SOCKET), _noDelay(0), _valueLen(sizeof(int))
        {
          _valuePtr = (char*)&_noDelay;
          _socket = amps_client_get_socket(client_);
          if (_socket != AMPS_INVALID_SOCKET)
          {
            getsockopt(_socket, IPPROTO_TCP, TCP_NODELAY, _valuePtr, &_valueLen);
            if (!_noDelay)
            {
              _noDelay = 1;
              setsockopt(_socket, IPPROTO_TCP, TCP_NODELAY, _valuePtr, _valueLen);
            }
            else
            {
              _socket = AMPS_INVALID_SOCKET;
            }
          }
        }

        ~NoDelay()
        {
          if (_socket != AMPS_INVALID_SOCKET)
          {
            _noDelay = 0;
            setsockopt(_socket, IPPROTO_TCP, TCP_NODELAY, _valuePtr, _valueLen);
          }
        }
    };

    friend class Client;
  protected:
    amps_handle _client;
    DisconnectHandler _disconnectHandler;
    enum GlobalCommandTypeHandlers : size_t
    {
      Publish = 0,
      SOW = 1,
      GroupBegin = 2,
      GroupEnd = 3,
      Heartbeat = 4,
      OOF = 5,
      Ack = 6,
      LastChance = 7,
      DuplicateMessage = 8,
      COUNT = 9
    };
    std::vector<MessageHandler> _globalCommandTypeHandlers;
    Message _message, _readMessage, _publishMessage, _deltaMessage, _beatMessage;
    MessageRouter _routes;
    MessageRouter::RouteCache _routeCache;
    mutable Mutex _lock;
    std::string _name, _nameHash, _lastUri, _logonCorrelationData;
    amps_uint64_t _nameHashValue;
    BookmarkStore _bookmarkStore;
    Store _publishStore;
    bool _isRetryOnDisconnect;
    amps_unique_ptr<FailedWriteHandler> _failedWriteHandler;
#if __cplusplus >= 201100L || _MSC_VER >= 1900
    std::atomic<amps_uint64_t> _lastSentHaSequenceNumber;
#else
    volatile amps_uint64_t _lastSentHaSequenceNumber;
#endif
    AMPS_ATOMIC_TYPE_8 _logonInProgress;
    AMPS_ATOMIC_TYPE_8 _badTimeToHASubscribe;
    VersionInfo _serverVersion;
    Timer _heartbeatTimer;
    amps_unique_ptr<MessageStream> _pEmptyMessageStream;

    // queue data
    int           _queueAckTimeout;
    bool          _isAutoAckEnabled;
    unsigned      _ackBatchSize;
    unsigned      _queuedAckCount;
    unsigned      _defaultMaxDepth;
    struct QueueBookmarks
    {
      QueueBookmarks(const std::string& topic_)
        : _topic(topic_)
        , _oldestTime(0)
        , _bookmarkCount(0)
      {;}
      std::string   _topic;
      std::string   _data;
      amps_uint64_t _oldestTime;
      unsigned      _bookmarkCount;
    };
    typedef amps_uint64_t topic_hash;
    typedef std::map<topic_hash, QueueBookmarks> TopicHashMap;
    TopicHashMap _topicHashMap;

    class ClientStoreReplayer : public StoreReplayer
    {
      ClientImpl* _client;
    public:
      unsigned _version;
      amps_result _res;

      ClientStoreReplayer()
        : _client(NULL), _version(0), _res(AMPS_E_OK)
      {}

      ClientStoreReplayer(ClientImpl* client_)
        : _client(client_), _version(0), _res(AMPS_E_OK)
      {}

      void setClient(ClientImpl* client_)
      {
        _client = client_;
      }

      void execute(Message& message_)
      {
        if (!_client)
        {
          throw CommandException("Can't replay without a client.");
        }
        amps_uint64_t index = amps_message_get_field_uint64(message_.getMessage(),
                                                            AMPS_Sequence);
        if (index > _client->_lastSentHaSequenceNumber)
        {
          _client->_lastSentHaSequenceNumber = index;
        }

        _res = AMPS_E_OK;
        // Don't replay a queue cancel message after a reconnect.
        // Currently, the only messages that will have anything in options
        // are cancel messages.
        if (!message_.getCommand().empty() &&
            (!_client->_logonInProgress ||
             message_.getOptions().len() < 6))
        {
          _res = amps_client_send_with_version(_client->_client,
                                               message_.getMessage(),
                                               &_version);
          if (_res != AMPS_E_OK)
          {
            throw DisconnectedException("AMPS Server disconnected during replay");
          }
        }
      }

    };
    ClientStoreReplayer _replayer;

    class FailedWriteStoreReplayer : public StoreReplayer
    {
      ClientImpl* _parent;
      const char* _reason;
      size_t      _reasonLength;
      size_t      _replayCount;
    public:
      FailedWriteStoreReplayer(ClientImpl* parent, const char* reason_, size_t reasonLength_)
        : _parent(parent),
          _reason(reason_),
          _reasonLength(reasonLength_),
          _replayCount(0)
      {;}
      void execute(Message& message_)
      {
        if (_parent->_failedWriteHandler)
        {
          ++_replayCount;
          _parent->_failedWriteHandler->failedWrite(message_,
                                                    _reason, _reasonLength);
        }
      }
      size_t replayCount(void) const
      {
        return _replayCount;
      }
    };

    struct AckResponseImpl : public RefBody
    {
      std::string username, password, reason, status, bookmark, options;
      amps_uint64_t sequenceNo;
      amps_uint64_t nameHashValue;
      VersionInfo serverVersion;
#if __cplusplus >= 201100L || _MSC_VER >= 1900
      std::atomic<bool> responded;
      std::atomic<bool> abandoned;
#else
      volatile bool responded;
      volatile bool abandoned;
#endif
      unsigned connectionVersion;
      AckResponseImpl() :
        RefBody(),
        sequenceNo((amps_uint64_t)0),
        serverVersion(),
        responded(false),
        abandoned(false),
        connectionVersion(0)
      {
      }
    };

    class AckResponse
    {
      RefHandle<AckResponseImpl> _body;
    public:
      AckResponse() : _body(NULL) {;}
      AckResponse(const AckResponse& rhs) : _body(rhs._body) {;}
      static AckResponse create()
      {
        AckResponse r;
        r._body = new AckResponseImpl();
        return r;
      }

      const std::string& username()
      {
        return _body.get().username;
      }
      void setUsername(const char* data_, size_t len_)
      {
        if (data_)
        {
          _body.get().username.assign(data_, len_);
        }
        else
        {
          _body.get().username.clear();
        }
      }
      const std::string& password()
      {
        return _body.get().password;
      }
      void setPassword(const char* data_, size_t len_)
      {
        if (data_)
        {
          _body.get().password.assign(data_, len_);
        }
        else
        {
          _body.get().password.clear();
        }
      }
      const std::string& reason()
      {
        return _body.get().reason;
      }
      void setReason(const char* data_, size_t len_)
      {
        if (data_)
        {
          _body.get().reason.assign(data_, len_);
        }
        else
        {
          _body.get().reason.clear();
        }
      }
      const std::string& status()
      {
        return _body.get().status;
      }
      void setStatus(const char* data_, size_t len_)
      {
        if (data_)
        {
          _body.get().status.assign(data_, len_);
        }
        else
        {
          _body.get().status.clear();
        }
      }
      const std::string& bookmark()
      {
        return _body.get().bookmark;
      }
      void setBookmark(const Field& bookmark_)
      {
        if (!bookmark_.empty())
        {
          _body.get().bookmark.assign(bookmark_.data(), bookmark_.len());
          Field::parseBookmark(bookmark_, _body.get().nameHashValue,
                               _body.get().sequenceNo);
        }
        else
        {
          _body.get().bookmark.clear();
          _body.get().sequenceNo = (amps_uint64_t)0;
          _body.get().nameHashValue = (amps_uint64_t)0;
        }
      }
      amps_uint64_t sequenceNo() const
      {
        return _body.get().sequenceNo;
      }
      amps_uint64_t nameHashValue() const
      {
        return _body.get().nameHashValue;
      }
      void setSequenceNo(const char* data_, size_t len_)
      {
        amps_uint64_t result = (amps_uint64_t)0;
        if (data_)
        {
          for (size_t i = 0; i < len_; ++i)
          {
            result *= (amps_uint64_t)10;
            result += (amps_uint64_t)(data_[i] - '0');
          }
        }
        _body.get().sequenceNo = result;
      }
      VersionInfo serverVersion() const
      {
        return _body.get().serverVersion;
      }
      void setServerVersion(const char* data_, size_t len_)
      {
        if (data_)
        {
          _body.get().serverVersion.setVersion(std::string(data_, len_));
        }
      }
      bool responded()
      {
        return _body.get().responded;
      }
      void setResponded()
      {
        _body.get().responded = true;
      }
      bool abandoned()
      {
        return _body.get().abandoned;
      }
      void setAbandoned()
      {
        if (_body.isValid())
        {
          _body.get().abandoned = true;
        }
      }

      void setConnectionVersion(unsigned connectionVersion)
      {
        _body.get().connectionVersion = connectionVersion;
      }

      unsigned getConnectionVersion()
      {
        return _body.get().connectionVersion;
      }
      void setOptions(const char* data_, size_t len_)
      {
        if (data_)
        {
          _body.get().options.assign(data_, len_);
        }
        else
        {
          _body.get().options.clear();
        }
      }

      const std::string& options()
      {
        return _body.get().options;
      }

      AckResponse& operator=(const AckResponse& rhs)
      {
        _body = rhs._body;
        return *this;
      }
    };


    typedef std::map<std::string, AckResponse> AckMap;
    AckMap _ackMap;
    Mutex  _ackMapLock;
    DefaultExceptionListener _defaultExceptionListener;
  protected:

    struct DeferredExecutionRequest
    {
      DeferredExecutionRequest(DeferredExecutionFunc func_,
                               void*                 userData_)
        : _func(func_),
          _userData(userData_)
      {;}

      DeferredExecutionFunc _func;
      void*                 _userData;
    };
    const ExceptionListener* _exceptionListener;
    std::shared_ptr<const ExceptionListener> _pExceptionListener;
    amps_unique_ptr<SubscriptionManager> _subscriptionManager;
    volatile bool _connected;
    std::string _username;
    typedef std::set<ConnectionStateListener*> ConnectionStateListeners;
    ConnectionStateListeners _connectionStateListeners;
    typedef std::vector<DeferredExecutionRequest> DeferredExecutionList;
    Mutex _deferredExecutionLock;
    DeferredExecutionList _deferredExecutionList;
    unsigned _heartbeatInterval;
    unsigned _readTimeout;

    void broadcastConnectionStateChanged(ConnectionStateListener::State newState_)
    {
      // If we disconnected before we got to notification, don't notify.
      // This should only be able to happen for Resubscribed, since the lock
      // is released to let the subscription manager run resubscribe so a
      // disconnect could be called before the change is broadcast.
      if (!_connected && newState_ > ConnectionStateListener::Connected)
      {
        return;
      }
      for (ConnectionStateListeners::iterator it = _connectionStateListeners.begin(); it != _connectionStateListeners.end(); ++it)
      {
        AMPS_CALL_EXCEPTION_WRAPPER(
          (*it)->connectionStateChanged(newState_));
      }
    }
    unsigned processedAck(Message& message);
    unsigned persistedAck(Message& meesage);
    void lastChance(Message& message);
    void checkAndSendHeartbeat(bool force = false);
    virtual ConnectionInfo getConnectionInfo() const;
    static amps_result
    ClientImplMessageHandler(amps_handle message, void* userData);
    static void
    ClientImplPreDisconnectHandler(amps_handle client, unsigned failedConnectionVersion, void* userData);
    static amps_result
    ClientImplDisconnectHandler(amps_handle client, void* userData);

    void unsubscribeInternal(const std::string& id)
    {
      if (id.empty())
      {
        return;
      }
      // remove the handler first to avoid any more message delivery
      Message::Field subId;
      subId.assign(id.data(), id.length());
      _routes.removeRoute(subId);
      // Lock is already acquired
      if (_subscriptionManager)
      {
        // Have to unlock before calling into sub manager to avoid deadlock
        Unlock<Mutex> unlock(_lock);
        _subscriptionManager->unsubscribe(subId);
      }
      _message.reset();
      _message.setCommandEnum(Message::Command::Unsubscribe);
      _message.newCommandId();
      _message.setSubscriptionId(id);
      _sendWithoutRetry(_message);
      deferredExecution(&amps_noOpFn, NULL);
    }

    AckResponse syncAckProcessing(long timeout_, Message& message_,
                                  bool isHASubscribe_)
    {
      return syncAckProcessing(timeout_, message_,
                               (amps_uint64_t)0, isHASubscribe_);
    }

    AckResponse syncAckProcessing(long timeout_, Message& message_,
                                  amps_uint64_t haSeq = (amps_uint64_t)0,
                                  bool isHASubscribe_ = false)
    {
      // inv: we already have _lock locked up.
      AckResponse ack = AckResponse::create();
      if (1)
      {
        Lock<Mutex> guard(_ackMapLock);
        _ackMap[message_.getCommandId()] = ack;
      }
      ack.setConnectionVersion((unsigned)_send(message_, haSeq, isHASubscribe_));
      if (ack.getConnectionVersion() == 0)
      {
        // Send failed
        throw DisconnectedException("Connection closed while waiting for response.");
      }
      bool timedOut = false;
      AMPS_START_TIMER(timeout_)
      while (!timedOut && !ack.responded() && !ack.abandoned() && _connected)
      {
        if (timeout_)
        {
          timedOut = !_lock.wait(timeout_);
          // May have woken up early, check real time
          if (timedOut)
          {
            AMPS_RESET_TIMER(timedOut, timeout_);
          }
        }
        else
        {
          // Using a timeout version to ensure python can interrupt
          _lock.wait(1000);
          Unlock<Mutex> unlck(_lock);
          amps_invoke_waiting_function();
        }
      }
      if (ack.responded())
      {
        if (ack.status() != "failure")
        {
          if (message_.getCommand() == "logon")
          {
            amps_uint64_t ackSequence = ack.sequenceNo();
            if (_lastSentHaSequenceNumber < ackSequence)
            {
              _lastSentHaSequenceNumber = ackSequence;
            }
            if (_publishStore.isValid())
            {
              // If this throws, logon will fail and eitehr be
              // handled in HAClient/ServerChooser or by the caller
              // of logon.
              _publishStore.discardUpTo(ackSequence);
              if (_lastSentHaSequenceNumber < _publishStore.getLastPersisted())
              {
                _lastSentHaSequenceNumber = _publishStore.getLastPersisted();
              }
            }
            _nameHash = ack.bookmark().substr(0, ack.bookmark().find('|'));
            _nameHashValue = ack.nameHashValue();
            _serverVersion = ack.serverVersion();
            if (_bookmarkStore.isValid())
            {
              _bookmarkStore.setServerVersion(_serverVersion);
            }
          }
          if (_ackBatchSize)
          {
            const std::string& options = ack.options();
            size_t index = options.find_first_of("max_backlog=");
            if (index != std::string::npos)
            {
              unsigned data = 0;
              const char* c = options.c_str() + index + 12;
              while (*c && *c != ',')
              {
                data = (data * 10) + (unsigned)(*c++ -48);
              }
              if (_ackBatchSize > data)
              {
                _ackBatchSize = data;
              }
            }
          }
          return ack;
        }
        const size_t NotEntitled = 12;
        std::string ackReason = ack.reason();
        if (ackReason.length() == 0)
        {
          return ack;  // none
        }
        if (ackReason.length() == NotEntitled &&
            ackReason[0] == 'n' &&
            message_.getUserId().len() == 0)
        {
          message_.assignUserId(_username);
        }
        message_.throwFor(_client, ackReason);
      }
      else     // !ack.responded()
      {
        if (!ack.abandoned())
        {
          throw TimedOutException("timed out waiting for operation.");
        }
        else
        {
          throw DisconnectedException("Connection closed while waiting for response.");
        }
      }
      return ack;
    }

    void _cleanup(void)
    {
      if (!_client)
      {
        return;
      }
      amps_client_set_predisconnect_handler(_client, NULL, 0L);
      amps_client_set_disconnect_handler(_client, NULL, 0L);
      AMPS_CALL_EXCEPTION_WRAPPER(ClientImpl::disconnect());
      _pEmptyMessageStream.reset(NULL);
      amps_client_destroy(_client);
      _client = NULL;
    }

  public:

    ClientImpl(const std::string& clientName)
      : _client(NULL), _name(clientName)
      , _isRetryOnDisconnect(true)
      , _lastSentHaSequenceNumber((amps_uint64_t)0), _logonInProgress(0)
      , _badTimeToHASubscribe(0), _serverVersion()
      , _queueAckTimeout(AMPS_DEFAULT_QUEUE_ACK_TIMEOUT)
      , _isAutoAckEnabled(false)
      , _ackBatchSize(0)
      , _queuedAckCount(0)
      , _defaultMaxDepth(0)
      , _connected(false)
      , _heartbeatInterval(0)
      , _readTimeout(0)
    {
      _replayer.setClient(this);
      _client = amps_client_create(clientName.c_str());
      amps_client_set_message_handler(_client,
                                      (amps_handler)ClientImpl::ClientImplMessageHandler,
                                      this);
      amps_client_set_predisconnect_handler(_client,
                                            (amps_predisconnect_handler)ClientImpl::ClientImplPreDisconnectHandler,
                                            this);
      amps_client_set_disconnect_handler(_client,
                                         (amps_handler)ClientImpl::ClientImplDisconnectHandler,
                                         this);
      _exceptionListener = &_defaultExceptionListener;
      for (size_t i = 0; i < GlobalCommandTypeHandlers::COUNT; ++i)
      {
#ifdef AMPS_USE_EMPLACE
        _globalCommandTypeHandlers.emplace_back(MessageHandler());
#else
        _globalCommandTypeHandlers.push_back(MessageHandler());
#endif
      }
    }

    virtual ~ClientImpl()
    {
      _cleanup();
    }

    const std::string& getName() const
    {
      return _name;
    }

    const std::string& getNameHash() const
    {
      return _nameHash;
    }

    const amps_uint64_t getNameHashValue() const
    {
      return _nameHashValue;
    }

    void setName(const std::string& name)
    {
      // This operation will fail if the client's
      // name is already set.
      amps_result result = amps_client_set_name(_client, name.c_str());
      if (result != AMPS_E_OK)
      {
        AMPSException::throwFor(_client, result);
      }
      _name = name;
    }

    const std::string& getLogonCorrelationData() const
    {
      return _logonCorrelationData;
    }

    void setLogonCorrelationData(const std::string& logonCorrelationData_)
    {
      _logonCorrelationData = logonCorrelationData_;
    }

    size_t getServerVersion() const
    {
      return _serverVersion.getOldStyleVersion();
    }

    VersionInfo getServerVersionInfo() const
    {
      return _serverVersion;
    }

    const std::string& getURI() const
    {
      return _lastUri;
    }

    virtual void connect(const std::string& uri)
    {
      Lock<Mutex> l(_lock);
      _connect(uri);
    }

    virtual void _connect(const std::string& uri)
    {
      _lastUri = uri;
      amps_result result = amps_client_connect(_client, uri.c_str());
      if (result != AMPS_E_OK)
      {
        AMPSException::throwFor(_client, result);
      }
      _message.reset();
      _deltaMessage.setCommandEnum(Message::Command::DeltaPublish);
      _publishMessage.setCommandEnum(Message::Command::Publish);
      _beatMessage.setCommandEnum(Message::Command::Heartbeat);
      _beatMessage.setOptions("beat");
      _readMessage.setClientImpl(this);
      if (_queueAckTimeout)
      {
        result = amps_client_set_idle_time(_client, _queueAckTimeout);
        if (result != AMPS_E_OK)
        {
          AMPSException::throwFor(_client, result);
        }
      }
      _connected = true;
      broadcastConnectionStateChanged(ConnectionStateListener::Connected);
    }

    void setDisconnected()
    {
      {
        Lock<Mutex> l(_lock);
        if (_connected)
        {
          AMPS_CALL_EXCEPTION_WRAPPER(broadcastConnectionStateChanged(ConnectionStateListener::Disconnected));
        }
        _connected = false;
        _heartbeatTimer.setTimeout(0.0);
      }
      clearAcks(INT_MAX);
      amps_client_disconnect(_client);
      _routes.clear();
    }

    virtual void disconnect()
    {
      AMPS_CALL_EXCEPTION_WRAPPER(flushAcks());
      setDisconnected();
      AMPS_CALL_EXCEPTION_WRAPPER(processDeferredExecutions());
      Lock<Mutex> l(_lock);
      broadcastConnectionStateChanged(ConnectionStateListener::Shutdown);
    }

    void clearAcks(unsigned failedVersion)
    {
      // Have to lock to prevent race conditions
      Lock<Mutex> guard(_ackMapLock);
      {
        // Go ahead and signal any waiters if they are around...
        std::vector<std::string> worklist;
        for (AckMap::iterator i = _ackMap.begin(), e = _ackMap.end(); i != e; ++i)
        {
          if (i->second.getConnectionVersion() <= failedVersion)
          {
            i->second.setAbandoned();
            worklist.push_back(i->first);
          }
        }

        for (std::vector<std::string>::iterator j = worklist.begin(), e = worklist.end(); j != e; ++j)
        {
          _ackMap.erase(*j);
        }
      }

      _lock.signalAll();
    }

    int send(const Message& message)
    {
      Lock<Mutex> l(_lock);
      return _send(message);
    }

    void sendWithoutRetry(const Message& message_)
    {
      Lock<Mutex> l(_lock);
      // If we got here while logon was in progress, then we tried to send
      // while we were disconnected so throw DisconnectedException
      if (_logonInProgress)
      {
        throw DisconnectedException("The client has been disconnected.");
      }
      _sendWithoutRetry(message_);
    }

    void _sendWithoutRetry(const Message& message_)
    {
      amps_result result = amps_client_send(_client, message_.getMessage());
      if (result != AMPS_E_OK)
      {
        AMPSException::throwFor(_client, result);
      }
    }

    int _send(const Message& message, amps_uint64_t haSeq = (amps_uint64_t)0,
              bool isHASubscribe_ = false)
    {
      // Lock is already acquired
      amps_result result = AMPS_E_RETRY;

      // Create a local reference to this message, as we'll need to hold on
      // to a reference to it in case reconnect occurs.
      Message localMessage = message;
      unsigned version = 0;

      while (result == AMPS_E_RETRY)
      {
        if (haSeq && _logonInProgress)
        {
          // If retrySend is disabled, do not wait for the reconnect
          // to finish, just throw.
          if (!_isRetryOnDisconnect)
          {
            AMPSException::throwFor(_client, AMPS_E_RETRY);
          }
          if (!_lock.wait(1000))
          {
            amps_invoke_waiting_function();
          }
        }
        else
        {
          if ((haSeq && haSeq <= _lastSentHaSequenceNumber) ||
              (isHASubscribe_ && _badTimeToHASubscribe))
          {
            return (int)version;
          }
          // It's possible to get here out of order, but this way we'll
          // always send in order.
          if (haSeq > _lastSentHaSequenceNumber)
          {
            while (haSeq > _lastSentHaSequenceNumber + 1)
            {
              try
              {
                // Replayer updates _lastSentHaSsequenceNumber
                if (!_publishStore.replaySingle(_replayer,
                                                _lastSentHaSequenceNumber + 1))
                {
                  //++_lastSentHaSequenceNumber;
                  continue;
                }
                result = AMPS_E_OK;
                version = _replayer._version;
              }
#ifdef _WIN32
              catch (const DisconnectedException&)
#else
              catch (const DisconnectedException& e)
#endif
              {
                result = _replayer._res;
                break;
              }
            }
            result = amps_client_send_with_version(_client,
                                                   localMessage.getMessage(),
                                                   &version);
            ++_lastSentHaSequenceNumber;
          }
          else
          {
            if (_logonInProgress && localMessage.getCommand().data()[0] != 'l')
            {
              while (_logonInProgress)
              {
                if (!_lock.wait(1000))
                {
                  amps_invoke_waiting_function();
                }
              }
            }
            result = amps_client_send_with_version(_client,
                                                   localMessage.getMessage(),
                                                   &version);
          }
          if (result != AMPS_E_OK)
          {
            if (!isHASubscribe_ && !haSeq &&
                localMessage.getMessage() == message.getMessage())
            {
              localMessage = message.deepCopy();
            }
            if (_isRetryOnDisconnect)
            {
              Unlock<Mutex> u(_lock);
              result = amps_client_attempt_reconnect(_client, version);
              // If this is an HA publish or subscribe command, it was
              // stored first and will have already been replayed by the
              // store or sub manager after reconnect, so just return.
              if ((isHASubscribe_ || haSeq) &&
                  result == AMPS_E_RETRY)
              {
                return (int)version;
              }
            }
            else
            {
              // retrySend is disabled so throw the error
              // from the send as an exception, do not retry.
              AMPSException::throwFor(_client, result);
            }
          }
        }
        if (result == AMPS_E_RETRY)
        {
          amps_invoke_waiting_function();
        }
      }

      if (result != AMPS_E_OK)
      {
        AMPSException::throwFor(_client, result);
      }
      return (int)version;
    }

    void addMessageHandler(const Field& commandId_,
                           const AMPS::MessageHandler& messageHandler_,
                           unsigned requestedAcks_, bool isSubscribe_)
    {
      Lock<Mutex> lock(_lock);
      _routes.addRoute(commandId_, messageHandler_, requestedAcks_,
                       0, isSubscribe_);
    }

    bool removeMessageHandler(const Field& commandId_)
    {
      Lock<Mutex> lock(_lock);
      return _routes.removeRoute(commandId_);
    }

    std::string send(const MessageHandler& messageHandler_, Message& message_, int timeout_ = 0)
    {
      Field id = message_.getCommandId();
      Field subId = message_.getSubscriptionId();
      Field qid = message_.getQueryId();
      bool isSubscribe = false;
      bool isSubscribeOnly = false;
      bool replace = false;
      unsigned requestedAcks = message_.getAckTypeEnum();
      unsigned systemAddedAcks = Message::AckType::None;

      switch (message_.getCommandEnum())
      {
      case Message::Command::Subscribe:
      case Message::Command::DeltaSubscribe:
        replace = message_.getOptions().operator std::string().find(AMPS_OPTIONS_REPLACE, 0, strlen(AMPS_OPTIONS_REPLACE) - 1) != std::string::npos;
        isSubscribeOnly = true;
      // fall through
      case Message::Command::SOWAndSubscribe:
      case Message::Command::SOWAndDeltaSubscribe:
        if (id.empty())
        {
          id = message_.newCommandId().getCommandId();
        }
        else
        {
          while (!replace && id != subId && _routes.hasRoute(id))
          {
            id = message_.newCommandId().getCommandId();
          }
        }
        if (subId.empty())
        {
          message_.setSubscriptionId(id);
          subId = id;
        }
        isSubscribe = true;
        if (!message_.getBookmark().empty() && _bookmarkStore.isValid())
        {
          systemAddedAcks |= Message::AckType::Persisted;
        }
      // fall through
      case Message::Command::SOW:
        if (id.empty())
        {
          id = message_.newCommandId().getCommandId();
        }
        else
        {
          while (!replace && id != subId && _routes.hasRoute(id))
          {
            message_.newCommandId();
            if (qid == id)
            {
              qid = message_.getCommandId();
              message_.setQueryId(qid);
            }
            id = message_.getCommandId();
          }
        }
        if (!isSubscribeOnly)
        {
          if (qid.empty())
          {
            message_.setQueryID(id);
            qid = id;
          }
          else
          {
            while (!replace && qid != subId && qid != id
                   && _routes.hasRoute(qid))
            {
              qid = message_.newQueryId().getQueryId();
            }
          }
        }
        systemAddedAcks |= Message::AckType::Processed;
        // for SOW only, we get a completed ack so we know when to remove the handler.
        if (!isSubscribeOnly)
        {
          systemAddedAcks |= Message::AckType::Completed;
        }
        message_.setAckTypeEnum(requestedAcks | systemAddedAcks);
        {
          int routesAdded = 0;
          Lock<Mutex> l(_lock);
          if (!subId.empty() && messageHandler_.isValid())
          {
            if (!_routes.hasRoute(subId))
            {
              ++routesAdded;
            }
            // This can replace a non-subscribe with a matching id
            // with a subscription but not another subscription.
            _routes.addRoute(subId, messageHandler_, requestedAcks,
                             systemAddedAcks, isSubscribe);
          }
          if (!isSubscribeOnly && !qid.empty()
              && messageHandler_.isValid() && qid != subId)
          {
            if (routesAdded == 0)
            {
              _routes.addRoute(qid, messageHandler_,
                               requestedAcks, systemAddedAcks, false);
            }
            else
            {
              void* data = NULL;
              {
                Unlock<Mutex> u(_lock);
                data = amps_invoke_copy_route_function(
                         messageHandler_.userData());
              }
              if (!data)
              {
                _routes.addRoute(qid, messageHandler_, requestedAcks,
                                 systemAddedAcks, false);
              }
              else
              {
                _routes.addRoute(qid,
                                 MessageHandler(messageHandler_.function(),
                                                data),
                                 requestedAcks, systemAddedAcks, false);
              }
            }
            ++routesAdded;
          }
          if (!id.empty() && messageHandler_.isValid()
              && requestedAcks & ~Message::AckType::Persisted
              && id != subId && id != qid)
          {
            if (routesAdded == 0)
            {
              _routes.addRoute(id, messageHandler_, requestedAcks,
                               systemAddedAcks, false);
            }
            else
            {
              void* data = NULL;
              {
                Unlock<Mutex> u(_lock);
                data = amps_invoke_copy_route_function(
                         messageHandler_.userData());
              }
              if (!data)
              {
                _routes.addRoute(id, messageHandler_, requestedAcks,
                                 systemAddedAcks, false);
              }
              else
              {
                _routes.addRoute(id,
                                 MessageHandler(messageHandler_.function(),
                                                data),
                                 requestedAcks,
                                 systemAddedAcks, false);
              }
            }
            ++routesAdded;
          }
          try
          {
            // We aren't adding to subscription manager, so this isn't
            // an HA subscribe.
            syncAckProcessing(timeout_, message_, 0, false);
            message_.setAckTypeEnum(requestedAcks);
          }
          catch (...)
          {
            _routes.removeRoute(message_.getQueryID());
            _routes.removeRoute(message_.getSubscriptionId());
            _routes.removeRoute(id);
            message_.setAckTypeEnum(requestedAcks);
            throw;
          }
        }
        break;
      // These are valid commands that are used as-is
      case Message::Command::Unsubscribe:
      case Message::Command::Heartbeat:
      case Message::Command::Logon:
      case Message::Command::StartTimer:
      case Message::Command::StopTimer:
      case Message::Command::SOWDelete:
      {
        Lock<Mutex> l(_lock);
        // if an ack is requested, it'll need a command ID.
        if (message_.getAckTypeEnum() != Message::AckType::None)
        {
          if (id.empty())
          {
            message_.newCommandId();
            id = message_.getCommandId();
          }
          if (messageHandler_.isValid())
          {
            _routes.addRoute(id, messageHandler_, requestedAcks,
                             Message::AckType::None, false);
          }
        }
        _send(message_);
      }
      break;
      case Message::Command::DeltaPublish:
      case Message::Command::Publish:
      {
        bool useSync = message_.getFilter().len() > 0;
        Lock<Mutex> l(_lock);
        // if an ack is requested, it'll need a command ID.
        unsigned ackType = message_.getAckTypeEnum();
        if (ackType != Message::AckType::None
            || useSync)
        {
          if (id.empty())
          {
            message_.newCommandId();
            id = message_.getCommandId();
          }
          if (messageHandler_.isValid())
          {
            _routes.addRoute(id, messageHandler_, requestedAcks,
                             Message::AckType::None, false);
          }
        }
        if (useSync)
        {
          message_.setAckTypeEnum(ackType | Message::AckType::Processed);
          syncAckProcessing(timeout_, message_, 0, false);
        }
        else
        {
          _send(message_);
        }
      }
      break;
      // These are things that shouldn't be sent (not meaningful)
      case Message::Command::GroupBegin:
      case Message::Command::GroupEnd:
      case Message::Command::OOF:
      case Message::Command::Ack:
      case Message::Command::Unknown:
      default:
        throw CommandException("Command type " + message_.getCommand() + " can not be sent directly to AMPS");
      }
      message_.setAckTypeEnum(requestedAcks);
      return id;
    }

    void setDisconnectHandler(const DisconnectHandler& disconnectHandler)
    {
      Lock<Mutex> l(_lock);
      _disconnectHandler = disconnectHandler;
    }

    void setGlobalCommandTypeMessageHandler(const std::string& command_, const MessageHandler& handler_)
    {
      switch (command_[0])
      {
#if 0 // Not currently implemented to avoid an extra branch in delivery
      case 'p':
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::Publish] = handler_;
        break;
      case 's':
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::SOW] = handler_;
        break;
#endif
      case 'h':
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::Heartbeat] = handler_;
        break;
#if 0 // Not currently implemented to avoid an extra branch in delivery
      case 'g':
        if (command_[6] == 'b')
        {
          _globalCommandTypeHandlers[GlobalCommandTypeHandlers::GroupBegin] = handler_;
        }
        else if (command_[6] == 'e')
        {
          _globalCommandTypeHandlers[GlobalCommandTypeHandlers::GroupEnd] = handler_;
        }
        else
        {
          std::ostringstream os;
          os << "Invalid command '" << command_ << "' passed to setGlobalCommandTypeHandler";
          throw CommandException(os.str());
        }
        break;
      case 'o':
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::OOF] = handler_;
        break;
#endif
      case 'a':
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::Ack] = handler_;
        break;
      case 'l':
      case 'L':
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::LastChance] = handler_;
        break;
      case 'd':
      case 'D':
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::DuplicateMessage] = handler_;
        break;
      default:
        std::ostringstream os;
        os << "Invalid command '" << command_ << "' passed to setGlobalCommandTypeHandler";
        throw CommandException(os.str());
        break;
      }
    }

    void setGlobalCommandTypeMessageHandler(const Message::Command::Type command_, const MessageHandler& handler_)
    {
      switch (command_)
      {
#if 0 // Not currently implemented to avoid an extra branch in delivery
      case Message::Command::Publish:
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::Publish] = handler_;
        break;
      case Message::Command::SOW:
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::SOW] = handler_;
        break;
#endif
      case Message::Command::Heartbeat:
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::Heartbeat] = handler_;
        break;
#if 0 // Not currently implemented to avoid an extra branch in delivery
      case Message::Command::GroupBegin:
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::GroupBegin] = handler_;
        break;
      case Message::Command::GroupEnd:
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::GroupEnd] = handler_;
        break;
      case Message::Command::OOF:
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::OOF] = handler_;
        break;
#endif
      case Message::Command::Ack:
        _globalCommandTypeHandlers[GlobalCommandTypeHandlers::Ack] = handler_;
        break;
      default:
        unsigned bits = 0;
        unsigned command = command_;
        while (command > 0)
        {
          ++bits;
          command >>= 1;
        }
        char errBuf[128];
        AMPS_snprintf(errBuf, sizeof(errBuf),
                      "Invalid command '%.*s' passed to setGlobalCommandTypeHandler",
                      CommandConstants<0>::Lengths[bits],
                      CommandConstants<0>::Values[bits]);
        throw CommandException(errBuf);
        break;
      }
    }

    void setGlobalCommandTypeMessageHandler(const GlobalCommandTypeHandlers handlerType_, const MessageHandler& handler_)
    {
      _globalCommandTypeHandlers[handlerType_] = handler_;
    }

    void setFailedWriteHandler(FailedWriteHandler* handler_)
    {
      Lock<Mutex> l(_lock);
      _failedWriteHandler.reset(handler_);
    }

    void setPublishStore(const Store& publishStore_)
    {
      Lock<Mutex> l(_lock);
      if (_connected)
      {
        throw AlreadyConnectedException("Setting a publish store on a connected client is undefined behavior");
      }
      _publishStore = publishStore_;
    }

    void setBookmarkStore(const BookmarkStore& bookmarkStore_)
    {
      Lock<Mutex> l(_lock);
      if (_connected)
      {
        throw AlreadyConnectedException("Setting a bookmark store on a connected client is undefined behavior");
      }
      _bookmarkStore = bookmarkStore_;
    }

    void setSubscriptionManager(SubscriptionManager* subscriptionManager_)
    {
      Lock<Mutex> l(_lock);
      _subscriptionManager.reset(subscriptionManager_);
    }

    SubscriptionManager* getSubscriptionManager() const
    {
      return const_cast<SubscriptionManager*>(_subscriptionManager.get());
    }

    DisconnectHandler getDisconnectHandler() const
    {
      return _disconnectHandler;
    }

    MessageHandler getDuplicateMessageHandler() const
    {
      return _globalCommandTypeHandlers[GlobalCommandTypeHandlers::DuplicateMessage];
    }

    FailedWriteHandler* getFailedWriteHandler() const
    {
      return const_cast<FailedWriteHandler*>(_failedWriteHandler.get());
    }

    Store getPublishStore() const
    {
      return _publishStore;
    }

    BookmarkStore getBookmarkStore() const
    {
      return _bookmarkStore;
    }

    amps_uint64_t publish(const char* topic_, size_t topicLen_, const char* data_, size_t dataLen_)
    {
      if (!_publishStore.isValid())
      {
        Lock<Mutex> l(_lock);
        _publishMessage.assignTopic(topic_, topicLen_);
        _publishMessage.assignData(data_, dataLen_);
        _send(_publishMessage);
        return 0;
      }
      else
      {
        if (!publishStoreMessage)
        {
          publishStoreMessage = new Message();
          PerThreadMessageTracker::addMessageToCleanupList(publishStoreMessage);
        }
        publishStoreMessage->reset();
        publishStoreMessage->setCommandEnum(Message::Command::Publish);
        return _publish(topic_, topicLen_, data_, dataLen_);
      }
    }

    amps_uint64_t publish(const char* topic_, size_t topicLen_, const char* data_,
                          size_t dataLen_, unsigned long expiration_)
    {
      if (!_publishStore.isValid())
      {
        Lock<Mutex> l(_lock);
        _publishMessage.assignTopic(topic_, topicLen_);
        _publishMessage.assignData(data_, dataLen_);
        char exprBuf[AMPS_NUMBER_BUFFER_LEN];
        size_t pos = convertToCharArray(exprBuf, expiration_);
        _publishMessage.assignExpiration(exprBuf + pos, AMPS_NUMBER_BUFFER_LEN - pos);
        _send(_publishMessage);
        _publishMessage.assignExpiration(NULL, 0);
        return 0;
      }
      else
      {
        if (!publishStoreMessage)
        {
          publishStoreMessage = new Message();
          PerThreadMessageTracker::addMessageToCleanupList(publishStoreMessage);
        }
        publishStoreMessage->reset();
        char exprBuf[AMPS_NUMBER_BUFFER_LEN];
        size_t exprPos = convertToCharArray(exprBuf, expiration_);
        publishStoreMessage->setCommandEnum(Message::Command::Publish)
        .assignExpiration(exprBuf + exprPos,
                          AMPS_NUMBER_BUFFER_LEN - exprPos);
        return _publish(topic_, topicLen_, data_, dataLen_);
      }
    }

    class FlushAckHandler : ConnectionStateListener
    {
    private:
      ClientImpl* _pClient;
      Field _cmdId;
#if __cplusplus >= 201100L || _MSC_VER >= 1900
      std::atomic<bool> _acked;
      std::atomic<bool> _disconnected;
#else
      volatile bool _acked;
      volatile bool _disconnected;
#endif
    public:
      FlushAckHandler(ClientImpl* pClient_)
        : _pClient(pClient_), _cmdId(), _acked(false), _disconnected(false)
      {
        pClient_->addConnectionStateListener(this);
      }
      ~FlushAckHandler()
      {
        _pClient->removeConnectionStateListener(this);
        _pClient->removeMessageHandler(_cmdId);
        _cmdId.clear();
      }
      void setCommandId(const Field& cmdId_)
      {
        _cmdId.deepCopy(cmdId_);
      }
      void invoke(const Message&)
      {
        _acked = true;
      }
      void connectionStateChanged(State state_)
      {
        if (state_ <= Shutdown)
        {
          _disconnected = true;
        }
      }
      bool acked()
      {
        return _acked;
      }
      bool done()
      {
        return _acked || _disconnected;
      }
    };

    void publishFlush(long timeout_, unsigned ackType_)
    {
      static const char* processed = "processed";
      static const size_t processedLen = strlen(processed);
      static const char* persisted = "persisted";
      static const size_t persistedLen = strlen(persisted);
      static const char* flush = "flush";
      static const size_t flushLen = strlen(flush);
      static VersionInfo minPersisted("5.3.3.0");
      static VersionInfo minFlush("4");
      if (ackType_ != Message::AckType::Processed
          && ackType_ != Message::AckType::Persisted)
      {
        throw CommandException("Flush can only be used with processed or persisted acks.");
      }
      FlushAckHandler flushHandler(this);
      if (_serverVersion >= minFlush)
      {
        Lock<Mutex> l(_lock);
        if (!_connected)
        {
          throw DisconnectedException("Not connected trying to flush");
        }
        _message.reset();
        _message.newCommandId();
        _message.assignCommand(flush, flushLen);
        if (_serverVersion < minPersisted
            || ackType_ == Message::AckType::Processed)
        {
          _message.assignAckType(processed, processedLen);
        }
        else
        {
          _message.assignAckType(persisted, persistedLen);
        }
        flushHandler.setCommandId(_message.getCommandId());
        addMessageHandler(_message.getCommandId(),
                          std::bind(&FlushAckHandler::invoke,
                                    std::ref(flushHandler),
                                    std::placeholders::_1),
                          ackType_, false);
        NoDelay noDelay(_client);
        if (_send(_message) == -1)
        {
          throw DisconnectedException("Disconnected trying to flush");
        }
      }
      if (_publishStore.isValid())
      {
        try
        {
          _publishStore.flush(timeout_);
        }
        catch (const AMPSException& ex)
        {
          AMPS_UNHANDLED_EXCEPTION(ex);
          throw;
        }
      }
      else if (_serverVersion < minFlush)
      {
        if (timeout_ > 0)
        {
          AMPS_USLEEP(timeout_ * 1000);
        }
        else
        {
          AMPS_USLEEP(1000 * 1000);
        }
        return;
      }
      if (timeout_)
      {
        Timer timer((double)timeout_);
        timer.start();
        while (!timer.check() && !flushHandler.done())
        {
          AMPS_USLEEP(10000);
          amps_invoke_waiting_function();
        }
      }
      else
      {
        while (!flushHandler.done())
        {
          AMPS_USLEEP(10000);
          amps_invoke_waiting_function();
        }
      }
      // No response or disconnect in timeout interval
      if (!flushHandler.done())
      {
        throw TimedOutException("Timed out waiting for flush");
      }
      // We got disconnected and there is no publish store
      if (!flushHandler.acked() && !_publishStore.isValid())
      {
        throw DisconnectedException("Disconnected waiting for flush");
      }
    }

    amps_uint64_t deltaPublish(const char* topic_, size_t topicLength_,
                               const char* data_, size_t dataLength_)
    {
      if (!_publishStore.isValid())
      {
        Lock<Mutex> l(_lock);
        _deltaMessage.assignTopic(topic_, topicLength_);
        _deltaMessage.assignData(data_, dataLength_);
        _send(_deltaMessage);
        return 0;
      }
      else
      {
        if (!publishStoreMessage)
        {
          publishStoreMessage = new Message();
          PerThreadMessageTracker::addMessageToCleanupList(publishStoreMessage);
        }
        publishStoreMessage->reset();
        publishStoreMessage->setCommandEnum(Message::Command::DeltaPublish);
        return _publish(topic_, topicLength_, data_, dataLength_);
      }
    }

    amps_uint64_t deltaPublish(const char* topic_, size_t topicLength_,
                               const char* data_, size_t dataLength_,
                               unsigned long expiration_)
    {
      if (!_publishStore.isValid())
      {
        Lock<Mutex> l(_lock);
        _deltaMessage.assignTopic(topic_, topicLength_);
        _deltaMessage.assignData(data_, dataLength_);
        char exprBuf[AMPS_NUMBER_BUFFER_LEN];
        size_t pos = convertToCharArray(exprBuf, expiration_);
        _deltaMessage.assignExpiration(exprBuf + pos, AMPS_NUMBER_BUFFER_LEN - pos);
        _send(_deltaMessage);
        _deltaMessage.assignExpiration(NULL, 0);
        return 0;
      }
      else
      {
        if (!publishStoreMessage)
        {
          publishStoreMessage = new Message();
          PerThreadMessageTracker::addMessageToCleanupList(publishStoreMessage);
        }
        publishStoreMessage->reset();
        char exprBuf[AMPS_NUMBER_BUFFER_LEN];
        size_t exprPos = convertToCharArray(exprBuf, expiration_);
        publishStoreMessage->setCommandEnum(Message::Command::DeltaPublish)
        .assignExpiration(exprBuf + exprPos,
                          AMPS_NUMBER_BUFFER_LEN - exprPos);
        return _publish(topic_, topicLength_, data_, dataLength_);
      }
    }

    amps_uint64_t _publish(const char* topic_, size_t topicLength_,
                           const char* data_, size_t dataLength_)
    {
      publishStoreMessage->assignTopic(topic_, topicLength_)
      .setAckTypeEnum(Message::AckType::Persisted)
      .assignData(data_, dataLength_);
      amps_uint64_t haSequenceNumber = _publishStore.store(*publishStoreMessage);
      char buf[AMPS_NUMBER_BUFFER_LEN];
      size_t pos = convertToCharArray(buf, haSequenceNumber);
      publishStoreMessage->assignSequence(buf + pos, AMPS_NUMBER_BUFFER_LEN - pos);
      {
        Lock<Mutex> l(_lock);
        _send(*publishStoreMessage, haSequenceNumber);
      }
      return haSequenceNumber;
    }

    virtual std::string logon(long timeout_, Authenticator& authenticator_,
                              const char* options_ = NULL)
    {
      Lock<Mutex> l(_lock);
      return _logon(timeout_, authenticator_, options_);
    }

    virtual std::string _logon(long timeout_, Authenticator& authenticator_,
                               const char* options_ = NULL)
    {
      _message.reset();
      _message.newCommandId();
      std::string newCommandId = _message.getCommandId();
      _message.setCommandEnum(Message::Command::Logon);
      _message.setClientName(_name);
#ifdef AMPS_CLIENT_VERSION_WITH_LANGUAGE
      _message.assignVersion(AMPS_CLIENT_VERSION_WITH_LANGUAGE,
                             strlen(AMPS_CLIENT_VERSION_WITH_LANGUAGE));
#endif
      URI uri(_lastUri);
      if (uri.user().size())
      {
        _message.setUserId(uri.user());
      }
      if (uri.password().size())
      {
        _message.setPassword(uri.password());
      }
      if (uri.protocol() == "amps" && uri.messageType().size())
      {
        _message.setMessageType(uri.messageType());
      }
      if (uri.isTrue("pretty"))
      {
        _message.setOptions("pretty");
      }

      _message.setPassword(authenticator_.authenticate(_message.getUserId(), _message.getPassword()));
      if (!_logonCorrelationData.empty())
      {
        _message.assignCorrelationId(_logonCorrelationData);
      }
      if (options_)
      {
        _message.setOptions(options_);
      }
      _username = _message.getUserId();
      try
      {
        AtomicFlagFlip pubFlip(&_logonInProgress);
        NoDelay noDelay(_client);
        while (true)
        {
          _message.setAckTypeEnum(Message::AckType::Processed);
          AckResponse ack = syncAckProcessing(timeout_, _message);
          if (ack.status() == "retry")
          {
            _message.setPassword(authenticator_.retry(ack.username(), ack.password()));
            _username = ack.username();
            _message.setUserId(_username);
          }
          else
          {
            authenticator_.completed(ack.username(), ack.password(), ack.reason());
            break;
          }
        }
        broadcastConnectionStateChanged(ConnectionStateListener::LoggedOn);

        // Now re-send the heartbeat command if configured
        _sendHeartbeat();
        // Signal any threads waiting for _logonInProgress
        _lock.signalAll();
      }
      catch (const AMPSException& ex)
      {
        _lock.signalAll();
        AMPS_UNHANDLED_EXCEPTION(ex);
        throw;
      }
      catch (...)
      {
        _lock.signalAll();
        throw;
      }

      if (_publishStore.isValid())
      {
        try
        {
          _publishStore.replay(_replayer);
          broadcastConnectionStateChanged(ConnectionStateListener::PublishReplayed);
        }
        catch (const PublishStoreGapException& ex)
        {
          _lock.signalAll();
          AMPS_UNHANDLED_EXCEPTION(ex);
          throw;
        }
        catch (const StoreException& ex)
        {
          _lock.signalAll();
          std::ostringstream os;
          os << "A local store exception occurred while logging on."
             << ex.toString();
          throw ConnectionException(os.str());
        }
        catch (const AMPSException& ex)
        {
          _lock.signalAll();
          AMPS_UNHANDLED_EXCEPTION(ex);
          throw;
        }
        catch (const std::exception& ex)
        {
          _lock.signalAll();
          AMPS_UNHANDLED_EXCEPTION(ex);
          throw;
        }
        catch (...)
        {
          _lock.signalAll();
          throw;
        }
      }
      _lock.signalAll();
      return newCommandId;
    }

    std::string subscribe(const MessageHandler& messageHandler_,
                          const std::string& topic_,
                          long timeout_,
                          const std::string& filter_,
                          const std::string& bookmark_,
                          const std::string& options_,
                          const std::string& subId_,
                          bool isHASubscribe_ = true)
    {
      isHASubscribe_ &= (bool)_subscriptionManager;
      Lock<Mutex> l(_lock);
      _message.reset();
      _message.setCommandEnum(Message::Command::Subscribe);
      _message.newCommandId();
      std::string subId(subId_);
      if (subId.empty())
      {
        if (options_.find(AMPS_OPTIONS_REPLACE, 0, strlen(AMPS_OPTIONS_REPLACE) - 1) != std::string::npos)
        {
          throw ConnectionException("Cannot issue a replacement subscription; a valid subscription id is required.");
        }

        subId = _message.getCommandId();
      }
      _message.setSubscriptionId(subId);
      // we need to deep copy this before sending the message; while we are
      // waiting for a response, the fields in _message may get blown away for
      // other operations.
      AMPS::Message::Field subIdField(subId);
      unsigned ackTypes = Message::AckType::Processed;

      if (!bookmark_.empty() && _bookmarkStore.isValid())
      {
        ackTypes |= Message::AckType::Persisted;
      }
      _message.setTopic(topic_);

      if (filter_.length())
      {
        _message.setFilter(filter_);
      }
      if (bookmark_.length())
      {
        if (bookmark_ == AMPS_BOOKMARK_RECENT)
        {
          Message::Field mostRecent = _bookmarkStore.getMostRecent(subIdField);
          _message.setBookmark(mostRecent);
        }
        else
        {
          _message.setBookmark(bookmark_);
          if (_bookmarkStore.isValid())
          {
            if (bookmark_ != AMPS_BOOKMARK_NOW &&
                bookmark_ != AMPS_BOOKMARK_EPOCH)
            {
              _bookmarkStore.log(_message);
              _bookmarkStore.discard(_message);
              _bookmarkStore.persisted(subIdField, _message.getBookmark());
            }
          }
        }
      }
      if (options_.length())
      {
        _message.setOptions(options_);
      }

      Message message = _message;
      if (isHASubscribe_)
      {
        message = _message.deepCopy();
        Unlock<Mutex> u(_lock);
        _subscriptionManager->subscribe(messageHandler_, message,
                                        Message::AckType::None);
        if (_badTimeToHASubscribe)
        {
          return subId;
        }
      }
      if (!_routes.hasRoute(_message.getSubscriptionId()))
      {
        _routes.addRoute(_message.getSubscriptionId(), messageHandler_,
                         Message::AckType::None, ackTypes, true);
      }
      message.setAckTypeEnum(ackTypes);
      if (!options_.empty())
      {
        message.setOptions(options_);
      }
      try
      {
        syncAckProcessing(timeout_, message, isHASubscribe_);
      }
      catch (const DisconnectedException&)
      {
        if (!isHASubscribe_)
        {
          _routes.removeRoute(subIdField);
          throw;
        }
        else
        {
          AMPS_CALL_EXCEPTION_WRAPPER(unsubscribeInternal(subIdField));
          throw;
        }
      }
      catch (const TimedOutException&)
      {
        AMPS_CALL_EXCEPTION_WRAPPER(unsubscribeInternal(subIdField));
        throw;
      }
      catch (...)
      {
        if (isHASubscribe_)
        {
          // Have to unlock before calling into sub manager to avoid deadlock
          Unlock<Mutex> unlock(_lock);
          _subscriptionManager->unsubscribe(subIdField);
        }
        _routes.removeRoute(subIdField);
        throw;
      }

      return subId;
    }
    std::string deltaSubscribe(const MessageHandler& messageHandler_,
                               const std::string& topic_,
                               long timeout_,
                               const std::string& filter_,
                               const std::string& bookmark_,
                               const std::string& options_,
                               const std::string& subId_ = "",
                               bool isHASubscribe_ = true)
    {
      isHASubscribe_ &= (bool)_subscriptionManager;
      Lock<Mutex> l(_lock);
      _message.reset();
      _message.setCommandEnum(Message::Command::DeltaSubscribe);
      _message.newCommandId();
      std::string subId(subId_);
      if (subId.empty())
      {
        subId = _message.getCommandId();
      }
      _message.setSubscriptionId(subId);
      // we need to deep copy this before sending the message; while we are
      // waiting for a response, the fields in _message may get blown away for
      // other operations.
      AMPS::Message::Field subIdField(subId);
      unsigned ackTypes = Message::AckType::Processed;

      if (!bookmark_.empty() && _bookmarkStore.isValid())
      {
        ackTypes |= Message::AckType::Persisted;
      }
      _message.setTopic(topic_);
      if (filter_.length())
      {
        _message.setFilter(filter_);
      }
      if (bookmark_.length())
      {
        if (bookmark_ == AMPS_BOOKMARK_RECENT)
        {
          Message::Field mostRecent = _bookmarkStore.getMostRecent(subIdField);
          _message.setBookmark(mostRecent);
        }
        else
        {
          _message.setBookmark(bookmark_);
          if (_bookmarkStore.isValid())
          {
            if (bookmark_ != AMPS_BOOKMARK_NOW &&
                bookmark_ != AMPS_BOOKMARK_EPOCH)
            {
              _bookmarkStore.log(_message);
              _bookmarkStore.discard(_message);
              _bookmarkStore.persisted(subIdField, _message.getBookmark());
            }
          }
        }
      }
      if (options_.length())
      {
        _message.setOptions(options_);
      }
      Message message = _message;
      if (isHASubscribe_)
      {
        message = _message.deepCopy();
        Unlock<Mutex> u(_lock);
        _subscriptionManager->subscribe(messageHandler_, message,
                                        Message::AckType::None);
        if (_badTimeToHASubscribe)
        {
          return subId;
        }
      }
      if (!_routes.hasRoute(_message.getSubscriptionId()))
      {
        _routes.addRoute(_message.getSubscriptionId(), messageHandler_,
                         Message::AckType::None, ackTypes, true);
      }
      message.setAckTypeEnum(ackTypes);
      if (!options_.empty())
      {
        message.setOptions(options_);
      }
      try
      {
        syncAckProcessing(timeout_, message, isHASubscribe_);
      }
      catch (const DisconnectedException&)
      {
        if (!isHASubscribe_)
        {
          _routes.removeRoute(subIdField);
          throw;
        }
      }
      catch (const TimedOutException&)
      {
        AMPS_CALL_EXCEPTION_WRAPPER(unsubscribeInternal(subIdField));
        throw;
      }
      catch (...)
      {
        if (isHASubscribe_)
        {
          // Have to unlock before calling into sub manager to avoid deadlock
          Unlock<Mutex> unlock(_lock);
          _subscriptionManager->unsubscribe(subIdField);
        }
        _routes.removeRoute(subIdField);
        throw;
      }
      return subId;
    }

    void unsubscribe(const std::string& id)
    {
      Lock<Mutex> l(_lock);
      unsubscribeInternal(id);
    }

    void unsubscribe(void)
    {
      if (_subscriptionManager)
      {
        _subscriptionManager->clear();
      }
      {
        _routes.unsubscribeAll();
        Lock<Mutex> l(_lock);
        _message.reset();
        _message.setCommandEnum(Message::Command::Unsubscribe);
        _message.newCommandId();
        _message.setSubscriptionId("all");
        _sendWithoutRetry(_message);
      }
      deferredExecution(&amps_noOpFn, NULL);
    }

    std::string sow(const MessageHandler& messageHandler_,
                    const std::string& topic_,
                    const std::string& filter_ = "",
                    const std::string& orderBy_ = "",
                    const std::string& bookmark_ = "",
                    int batchSize_ = AMPS_DEFAULT_BATCH_SIZE,
                    int topN_ = AMPS_DEFAULT_TOP_N,
                    const std::string& options_ = "",
                    long timeout_ = AMPS_DEFAULT_COMMAND_TIMEOUT)
    {
      Lock<Mutex> l(_lock);
      _message.reset();
      _message.setCommandEnum(Message::Command::SOW);
      _message.newCommandId();
      // need to keep our own copy of the command ID.
      std::string commandId = _message.getCommandId();
      _message.setQueryID(_message.getCommandId());
      unsigned ackTypes = Message::AckType::Processed | Message::AckType::Completed;
      _message.setAckTypeEnum(ackTypes);
      _message.setTopic(topic_);
      if (filter_.length())
      {
        _message.setFilter(filter_);
      }
      if (orderBy_.length())
      {
        _message.setOrderBy(orderBy_);
      }
      if (bookmark_.length())
      {
        _message.setBookmark(bookmark_);
      }
      _message.setBatchSize(AMPS::asString(batchSize_));
      if (topN_ != AMPS_DEFAULT_TOP_N)
      {
        _message.setTopNRecordsReturned(AMPS::asString(topN_));
      }
      if (options_.length())
      {
        _message.setOptions(options_);
      }

      _routes.addRoute(_message.getQueryID(), messageHandler_,
                       Message::AckType::None, ackTypes, false);

      try
      {
        syncAckProcessing(timeout_, _message);
      }
      catch (...)
      {
        AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(commandId));
        throw;
      }

      return commandId;
    }

    std::string sow(const MessageHandler& messageHandler_,
                    const std::string& topic_,
                    long timeout_,
                    const std::string& filter_ = "",
                    int batchSize_ = AMPS_DEFAULT_BATCH_SIZE,
                    int topN_ = AMPS_DEFAULT_TOP_N)
    {
      std::string notSet;
      return sow(messageHandler_,
                 topic_,
                 filter_,
                 notSet,   // orderBy
                 notSet,   // bookmark
                 batchSize_,
                 topN_,
                 notSet,
                 timeout_);
    }

    std::string sowAndSubscribe(const MessageHandler& messageHandler_,
                                const std::string& topic_,
                                const std::string& filter_ = "",
                                const std::string& orderBy_ = "",
                                const std::string& bookmark_ = "",
                                int batchSize_ = AMPS_DEFAULT_BATCH_SIZE,
                                int topN_ = AMPS_DEFAULT_TOP_N,
                                const std::string& options_ = "",
                                long timeout_ = AMPS_DEFAULT_COMMAND_TIMEOUT,
                                bool isHASubscribe_ = true)
    {
      isHASubscribe_ &= (bool)_subscriptionManager;
      unsigned ackTypes = Message::AckType::Processed;
      Lock<Mutex> l(_lock);
      _message.reset();
      _message.setCommandEnum(Message::Command::SOWAndSubscribe);
      _message.newCommandId();
      Field cid = _message.getCommandId();
      std::string subId = cid;
      _message.setQueryID(cid).setSubscriptionId(cid).setTopic(topic_);
      if (filter_.length())
      {
        _message.setFilter(filter_);
      }
      if (orderBy_.length())
      {
        _message.setOrderBy(orderBy_);
      }
      if (bookmark_.length())
      {
        _message.setBookmark(bookmark_);
        Message::Field bookmark = _message.getBookmark();
        if (_bookmarkStore.isValid())
        {
          ackTypes |= Message::AckType::Persisted;
          if (bookmark == AMPS_BOOKMARK_RECENT)
          {
            _message.setBookmark(_bookmarkStore.getMostRecent(_message.getSubscriptionId()));
          }
          else if (bookmark != AMPS_BOOKMARK_NOW &&
                   bookmark != AMPS_BOOKMARK_EPOCH)
          {
            _bookmarkStore.log(_message);
            if (!BookmarkRange::isRange(bookmark))
            {
              _bookmarkStore.discard(_message);
              _bookmarkStore.persisted(_message.getSubscriptionId(),
                                       bookmark);
            }
          }
        }
        else if (bookmark == AMPS_BOOKMARK_RECENT)
        {
          _message.setBookmark(AMPS_BOOKMARK_EPOCH);
        }
      }
      _message.setBatchSize(AMPS::asString(batchSize_));
      if (topN_ != AMPS_DEFAULT_TOP_N)
      {
        _message.setTopNRecordsReturned(AMPS::asString(topN_));
      }
      if (options_.length())
      {
        _message.setOptions(options_);
      }

      Message message = _message;
      if (isHASubscribe_)
      {
        message = _message.deepCopy();
        Unlock<Mutex> u(_lock);
        _subscriptionManager->subscribe(messageHandler_, message,
                                        Message::AckType::None);
        if (_badTimeToHASubscribe)
        {
          return subId;
        }
      }
      _routes.addRoute(cid, messageHandler_,
                       Message::AckType::None, ackTypes, true);
      message.setAckTypeEnum(ackTypes);
      if (!options_.empty())
      {
        message.setOptions(options_);
      }
      try
      {
        syncAckProcessing(timeout_, message, isHASubscribe_);
      }
      catch (const DisconnectedException&)
      {
        if (!isHASubscribe_)
        {
          _routes.removeRoute(subId);
          throw;
        }
      }
      catch (const TimedOutException&)
      {
        AMPS_CALL_EXCEPTION_WRAPPER(unsubscribeInternal(subId));
        throw;
      }
      catch (...)
      {
        if (isHASubscribe_)
        {
          // Have to unlock before calling into sub manager to avoid deadlock
          Unlock<Mutex> unlock(_lock);
          _subscriptionManager->unsubscribe(cid);
        }
        _routes.removeRoute(subId);
        throw;
      }
      return subId;
    }

    std::string sowAndSubscribe(const MessageHandler& messageHandler_,
                                const std::string& topic_,
                                long timeout_,
                                const std::string& filter_ = "",
                                int batchSize_ = AMPS_DEFAULT_BATCH_SIZE,
                                bool oofEnabled_ = false,
                                int topN_ = AMPS_DEFAULT_TOP_N,
                                bool isHASubscribe_ = true)
    {
      std::string notSet;
      return sowAndSubscribe(messageHandler_,
                             topic_,
                             filter_,
                             notSet,   // orderBy
                             notSet,   // bookmark
                             batchSize_,
                             topN_,
                             (oofEnabled_ ? "oof" : ""),
                             timeout_,
                             isHASubscribe_);
    }

    std::string sowAndDeltaSubscribe(const MessageHandler& messageHandler_,
                                     const std::string& topic_,
                                     const std::string& filter_ = "",
                                     const std::string& orderBy_ = "",
                                     int batchSize_ = AMPS_DEFAULT_BATCH_SIZE,
                                     int topN_ = AMPS_DEFAULT_TOP_N,
                                     const std::string& options_ = "",
                                     long timeout_ = AMPS_DEFAULT_COMMAND_TIMEOUT,
                                     bool isHASubscribe_ = true)
    {
      isHASubscribe_ &= (bool)_subscriptionManager;
      Lock<Mutex> l(_lock);
      _message.reset();
      _message.setCommandEnum(Message::Command::SOWAndDeltaSubscribe);
      _message.newCommandId();
      _message.setQueryID(_message.getCommandId());
      _message.setSubscriptionId(_message.getCommandId());
      std::string subId = _message.getSubscriptionId();
      _message.setTopic(topic_);
      if (filter_.length())
      {
        _message.setFilter(filter_);
      }
      if (orderBy_.length())
      {
        _message.setOrderBy(orderBy_);
      }
      _message.setBatchSize(AMPS::asString(batchSize_));
      if (topN_ != AMPS_DEFAULT_TOP_N)
      {
        _message.setTopNRecordsReturned(AMPS::asString(topN_));
      }
      if (options_.length())
      {
        _message.setOptions(options_);
      }
      Message message = _message;
      if (isHASubscribe_)
      {
        message = _message.deepCopy();
        Unlock<Mutex> u(_lock);
        _subscriptionManager->subscribe(messageHandler_, message,
                                        Message::AckType::None);
        if (_badTimeToHASubscribe)
        {
          return subId;
        }
      }
      _routes.addRoute(message.getQueryID(), messageHandler_,
                       Message::AckType::None, Message::AckType::Processed, true);
      message.setAckTypeEnum(Message::AckType::Processed);
      if (!options_.empty())
      {
        message.setOptions(options_);
      }
      try
      {
        syncAckProcessing(timeout_, message, isHASubscribe_);
      }
      catch (const DisconnectedException&)
      {
        if (!isHASubscribe_)
        {
          _routes.removeRoute(subId);
          throw;
        }
      }
      catch (const TimedOutException&)
      {
        AMPS_CALL_EXCEPTION_WRAPPER(unsubscribeInternal(subId));
        throw;
      }
      catch (...)
      {
        if (isHASubscribe_)
        {
          // Have to unlock before calling into sub manager to avoid deadlock
          Unlock<Mutex> unlock(_lock);
          _subscriptionManager->unsubscribe(Field(subId));
        }
        _routes.removeRoute(subId);
        throw;
      }
      return subId;
    }

    std::string sowAndDeltaSubscribe(const MessageHandler& messageHandler_,
                                     const std::string& topic_,
                                     long timeout_,
                                     const std::string& filter_ = "",
                                     int batchSize_ = AMPS_DEFAULT_BATCH_SIZE,
                                     bool oofEnabled_ = false,
                                     bool sendEmpties_ = false,
                                     int topN_ = AMPS_DEFAULT_TOP_N,
                                     bool isHASubscribe_ = true)
    {
      std::string notSet;
      Message::Options options;
      if (oofEnabled_)
      {
        options.setOOF();
      }
      if (sendEmpties_ == false)
      {
        options.setNoEmpties();
      }
      return sowAndDeltaSubscribe(messageHandler_,
                                  topic_,
                                  filter_,
                                  notSet,   // orderBy
                                  batchSize_,
                                  topN_,
                                  options,
                                  timeout_,
                                  isHASubscribe_);
    }

    std::string sowDelete(const MessageHandler& messageHandler_,
                          const std::string& topic_,
                          const std::string& filter_,
                          long timeout_,
                          Message::Field commandId_ = Message::Field())
    {
      if (_publishStore.isValid())
      {
        unsigned ackType = Message::AckType::Processed |
                           Message::AckType::Stats |
                           Message::AckType::Persisted;
        if (!publishStoreMessage)
        {
          publishStoreMessage = new Message();
          PerThreadMessageTracker::addMessageToCleanupList(publishStoreMessage);
        }
        publishStoreMessage->reset();
        if (commandId_.empty())
        {
          publishStoreMessage->newCommandId();
          commandId_ = publishStoreMessage->getCommandId();
        }
        else
        {
          publishStoreMessage->setCommandId(commandId_.data(), commandId_.len());
        }
        publishStoreMessage->setCommandEnum(Message::Command::SOWDelete)
        .assignSubscriptionId(commandId_.data(), commandId_.len())
        .assignQueryID(commandId_.data(), commandId_.len())
        .setAckTypeEnum(ackType)
        .assignTopic(topic_.c_str(), topic_.length())
        .assignFilter(filter_.c_str(), filter_.length());
        amps_uint64_t haSequenceNumber = _publishStore.store(*publishStoreMessage);
        char buf[AMPS_NUMBER_BUFFER_LEN];
        size_t pos = convertToCharArray(buf, haSequenceNumber);
        publishStoreMessage->assignSequence(buf + pos, AMPS_NUMBER_BUFFER_LEN - pos);
        {
          try
          {
            Lock<Mutex> l(_lock);
            _routes.addRoute(commandId_, messageHandler_,
                             Message::AckType::Stats,
                             Message::AckType::Processed | Message::AckType::Persisted,
                             false);
            syncAckProcessing(timeout_, *publishStoreMessage,
                              haSequenceNumber);
          }
          catch (const DisconnectedException&)
          {
            // -V565
            // Pass - it will get replayed upon reconnect
          }
          catch (...)
          {
            AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(commandId_));
            throw;
          }
        }
        return (std::string)commandId_;
      }
      else
      {
        Lock<Mutex> l(_lock);
        _message.reset();
        if (commandId_.empty())
        {
          _message.newCommandId();
          commandId_ = _message.getCommandId();
        }
        else
        {
          _message.setCommandId(commandId_.data(), commandId_.len());
        }
        _message.setCommandEnum(Message::Command::SOWDelete)
        .assignSubscriptionId(commandId_.data(), commandId_.len())
        .assignQueryID(commandId_.data(), commandId_.len())
        .setAckTypeEnum(Message::AckType::Processed |
                        Message::AckType::Stats)
        .assignTopic(topic_.c_str(), topic_.length())
        .assignFilter(filter_.c_str(), filter_.length());
        _routes.addRoute(commandId_, messageHandler_,
                         Message::AckType::Stats,
                         Message::AckType::Processed,
                         false);
        try
        {
          syncAckProcessing(timeout_, _message);
        }
        catch (...)
        {
          AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(commandId_));
          throw;
        }
        return (std::string)commandId_;
      }
    }

    std::string sowDeleteByData(const MessageHandler& messageHandler_,
                                const std::string& topic_,
                                const std::string& data_,
                                long timeout_,
                                Message::Field commandId_ = Message::Field())
    {
      if (_publishStore.isValid())
      {
        unsigned ackType = Message::AckType::Processed |
                           Message::AckType::Stats |
                           Message::AckType::Persisted;
        if (!publishStoreMessage)
        {
          publishStoreMessage = new Message();
          PerThreadMessageTracker::addMessageToCleanupList(publishStoreMessage);
        }
        publishStoreMessage->reset();
        if (commandId_.empty())
        {
          publishStoreMessage->newCommandId();
          commandId_ = publishStoreMessage->getCommandId();
        }
        else
        {
          publishStoreMessage->setCommandId(commandId_.data(), commandId_.len());
        }
        publishStoreMessage->setCommandEnum(Message::Command::SOWDelete)
        .assignSubscriptionId(commandId_.data(), commandId_.len())
        .assignQueryID(commandId_.data(), commandId_.len())
        .setAckTypeEnum(ackType)
        .assignTopic(topic_.c_str(), topic_.length())
        .assignData(data_.c_str(), data_.length());
        amps_uint64_t haSequenceNumber = _publishStore.store(*publishStoreMessage);
        char buf[AMPS_NUMBER_BUFFER_LEN];
        size_t pos = convertToCharArray(buf, haSequenceNumber);
        publishStoreMessage->assignSequence(buf + pos, AMPS_NUMBER_BUFFER_LEN - pos);
        {
          try
          {
            Lock<Mutex> l(_lock);
            _routes.addRoute(commandId_, messageHandler_,
                             Message::AckType::Stats,
                             Message::AckType::Processed | Message::AckType::Persisted,
                             false);
            syncAckProcessing(timeout_, *publishStoreMessage,
                              haSequenceNumber);
          }
          catch (const DisconnectedException&)
          {
            // -V565
            // Pass - it will get replayed upon reconnect
          }
          catch (...)
          {
            AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(commandId_));
            throw;
          }
        }
        return (std::string)commandId_;
      }
      else
      {
        Lock<Mutex> l(_lock);
        _message.reset();
        if (commandId_.empty())
        {
          _message.newCommandId();
          commandId_ = _message.getCommandId();
        }
        else
        {
          _message.setCommandId(commandId_.data(), commandId_.len());
        }
        _message.setCommandEnum(Message::Command::SOWDelete)
        .assignSubscriptionId(commandId_.data(), commandId_.len())
        .assignQueryID(commandId_.data(), commandId_.len())
        .setAckTypeEnum(Message::AckType::Processed |
                        Message::AckType::Stats)
        .assignTopic(topic_.c_str(), topic_.length())
        .assignData(data_.c_str(), data_.length());
        _routes.addRoute(commandId_, messageHandler_,
                         Message::AckType::Stats,
                         Message::AckType::Processed,
                         false);
        try
        {
          syncAckProcessing(timeout_, _message);
        }
        catch (...)
        {
          AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(commandId_));
          throw;
        }
        return (std::string)commandId_;
      }
    }

    std::string sowDeleteByKeys(const MessageHandler& messageHandler_,
                                const std::string& topic_,
                                const std::string& keys_,
                                long timeout_,
                                Message::Field commandId_ = Message::Field())
    {
      if (_publishStore.isValid())
      {
        unsigned ackType = Message::AckType::Processed |
                           Message::AckType::Stats |
                           Message::AckType::Persisted;
        if (!publishStoreMessage)
        {
          publishStoreMessage = new Message();
          PerThreadMessageTracker::addMessageToCleanupList(publishStoreMessage);
        }
        publishStoreMessage->reset();
        if (commandId_.empty())
        {
          publishStoreMessage->newCommandId();
          commandId_ = publishStoreMessage->getCommandId();
        }
        else
        {
          publishStoreMessage->setCommandId(commandId_.data(), commandId_.len());
        }
        publishStoreMessage->setCommandEnum(Message::Command::SOWDelete)
        .assignSubscriptionId(commandId_.data(), commandId_.len())
        .assignQueryID(commandId_.data(), commandId_.len())
        .setAckTypeEnum(ackType)
        .assignTopic(topic_.c_str(), topic_.length())
        .assignSowKeys(keys_.c_str(), keys_.length());
        amps_uint64_t haSequenceNumber = _publishStore.store(*publishStoreMessage);
        char buf[AMPS_NUMBER_BUFFER_LEN];
        size_t pos = convertToCharArray(buf, haSequenceNumber);
        publishStoreMessage->assignSequence(buf + pos, AMPS_NUMBER_BUFFER_LEN - pos);
        {
          try
          {
            Lock<Mutex> l(_lock);
            _routes.addRoute(commandId_, messageHandler_,
                             Message::AckType::Stats,
                             Message::AckType::Processed | Message::AckType::Persisted,
                             false);
            syncAckProcessing(timeout_, *publishStoreMessage,
                              haSequenceNumber);
          }
          catch (const DisconnectedException&)
          {
            // -V565
            // Pass - it will get replayed upon reconnect
          }
          catch (...)
          {
            AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(commandId_));
            throw;
          }
        }
        return (std::string)commandId_;
      }
      else
      {
        Lock<Mutex> l(_lock);
        _message.reset();
        if (commandId_.empty())
        {
          _message.newCommandId();
          commandId_ = _message.getCommandId();
        }
        else
        {
          _message.setCommandId(commandId_.data(), commandId_.len());
        }
        _message.setCommandEnum(Message::Command::SOWDelete)
        .assignSubscriptionId(commandId_.data(), commandId_.len())
        .assignQueryID(commandId_.data(), commandId_.len())
        .setAckTypeEnum(Message::AckType::Processed |
                        Message::AckType::Stats)
        .assignTopic(topic_.c_str(), topic_.length())
        .assignSowKeys(keys_.c_str(), keys_.length());
        _routes.addRoute(commandId_, messageHandler_,
                         Message::AckType::Stats,
                         Message::AckType::Processed,
                         false);
        try
        {
          syncAckProcessing(timeout_, _message);
        }
        catch (...)
        {
          AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(commandId_));
          throw;
        }
        return (std::string)commandId_;
      }
    }

    void startTimer(void)
    {
      if (_serverVersion >= "5.3.2.0")
      {
        throw CommandException("The start_timer command is deprecated.");
      }
      Lock<Mutex> l(_lock);
      _message.reset();
      _message.setCommandEnum(Message::Command::StartTimer);

      _send(_message);
    }

    std::string stopTimer(MessageHandler messageHandler_)
    {
      if (_serverVersion >= "5.3.2.0")
      {
        throw CommandException("The stop_timer command is deprecated.");
      }
      return executeAsync(Command("stop_timer").addAckType("completed"), messageHandler_);
    }

    amps_handle getHandle(void)
    {
      return _client;
    }

    /// Set an exception listener on this adapter that will be notified of
    /// all exceptions that occur rather than silently absorbing them. The
    /// exception listener will be ignored if the adapter is constructed with
    /// updateFailureThrows_ true; in that case exceptions will leave this
    /// adapter to be handled elsewhere.
    /// \param pListener_ A shared pointer to the ExceptionListener that
    /// should be notified.
    void setExceptionListener(const std::shared_ptr<const ExceptionListener>& pListener_)
    {
      _pExceptionListener = pListener_;
      _exceptionListener = _pExceptionListener.get();
    }

    void setExceptionListener(const ExceptionListener& listener_)
    {
      _exceptionListener = &listener_;
    }

    const ExceptionListener& getExceptionListener(void) const
    {
      return *_exceptionListener;
    }

    void setHeartbeat(unsigned heartbeatInterval_, unsigned readTimeout_)
    {
      if (readTimeout_ < heartbeatInterval_)
      {
        throw UsageException("The socket read timeout must be >= the heartbeat interval.");
      }
      Lock<Mutex> l(_lock);
      if (_heartbeatInterval != heartbeatInterval_ ||
          _readTimeout       != readTimeout_)
      {
        _heartbeatInterval = heartbeatInterval_;
        _readTimeout = readTimeout_;
        _sendHeartbeat();
      }
    }

    void _sendHeartbeat(void)
    {
      if (_connected && _heartbeatInterval != 0)
      {
        std::ostringstream options;
        options << "start," << _heartbeatInterval;
        _beatMessage.setOptions(options.str());

        _heartbeatTimer.setTimeout(_heartbeatInterval * 1000.0);
        _heartbeatTimer.start();
        try
        {
          _sendWithoutRetry(_beatMessage);
          broadcastConnectionStateChanged(ConnectionStateListener::HeartbeatInitiated);
        }
        catch (ConnectionException& ex_)
        {
          // If we are disconnected when we attempt to send, that's OK;
          //   we'll send this message after we re-connect (if we do).
          AMPS_UNHANDLED_EXCEPTION(ex_);
        }
        _beatMessage.setOptions("beat");
      }
      amps_result result = AMPS_E_OK;
      if (_readTimeout && _connected)
      {
        result = amps_client_set_read_timeout(_client, (int)_readTimeout);
      }
      if (result != AMPS_E_OK && result != AMPS_E_DISCONNECTED)
      {
        AMPSException::throwFor(_client, result);
      }
    }

    void addConnectionStateListener(ConnectionStateListener* listener_)
    {
      Lock<Mutex> lock(_lock);
      _connectionStateListeners.insert(listener_);
    }

    void removeConnectionStateListener(ConnectionStateListener* listener_)
    {
      Lock<Mutex> lock(_lock);
      _connectionStateListeners.erase(listener_);
    }

    void clearConnectionStateListeners()
    {
      Lock<Mutex> lock(_lock);
      _connectionStateListeners.clear();
    }

    void _registerHandler(Command& command_, Message::Field& cid_,
                          MessageHandler& handler_, unsigned requestedAcks_,
                          unsigned systemAddedAcks_, bool isSubscribe_)
    {
      Message message = command_.getMessage();
      Message::Command::Type commandType = message.getCommandEnum();
      Message::Field subid = message.getSubscriptionId();
      Message::Field qid = message.getQueryID();
      // If we have an id, we're good, even if it's an existing route
      bool added = qid.len() || subid.len() || cid_.len();
      bool cidIsQid = cid_ == qid;
      bool cidUnique = !cidIsQid && cid_.len() > 0 && cid_ != subid;
      int addedCount = 0;
      if (subid.len() > 0)
      {
        // This can replace a non-subscribe with a matching id
        // with a subscription but not another subscription.
        addedCount += _routes.addRoute(subid, handler_, requestedAcks_,
                                       systemAddedAcks_, isSubscribe_);
        if (!cidUnique
            && (commandType == Message::Command::Subscribe
                || commandType == Message::Command::DeltaSubscribe))
        {
          // We don't need to do anything else
          cid_ = subid;
          return;
        }
      }
      if (qid.len() > 0 && qid != subid
          && (commandType == Message::Command::SOW
              || commandType == Message::Command::SOWDelete
              || commandType == Message::Command::SOWAndSubscribe
              || commandType == Message::Command::SOWAndDeltaSubscribe))
      {
        while (_routes.hasRoute(qid))
        {
          message.newQueryId();
          if (cidIsQid)
          {
            cid_ = message.getQueryId();
          }
          qid = message.getQueryId();
        }
        if (addedCount == 0)
        {
          _routes.addRoute(qid, handler_, requestedAcks_,
                           systemAddedAcks_, isSubscribe_);
        }
        else
        {
          void* data = NULL;
          {
            Unlock<Mutex> u(_lock);
            data = amps_invoke_copy_route_function(handler_.userData());
          }
          if (!data)
          {
            _routes.addRoute(qid, handler_, requestedAcks_,
                             systemAddedAcks_, false);
          }
          else
          {
            _routes.addRoute(qid,
                             MessageHandler(handler_.function(),
                                            data),
                             requestedAcks_,
                             systemAddedAcks_, false);
          }
        }
        ++addedCount;
      }
      if (cidUnique && requestedAcks_ & ~Message::AckType::Persisted)
      {
        while (_routes.hasRoute(cid_))
        {
          cid_ = message.newCommandId().getCommandId();
        }
        if (addedCount == 0)
        {
          _routes.addRoute(cid_, handler_, requestedAcks_,
                           systemAddedAcks_, false);
        }
        else
        {
          void* data = NULL;
          {
            Unlock<Mutex> u(_lock);
            data = amps_invoke_copy_route_function(handler_.userData());
          }
          if (!data)
          {
            _routes.addRoute(cid_, handler_, requestedAcks_,
                             systemAddedAcks_, false);
          }
          else
          {
            _routes.addRoute(cid_,
                             MessageHandler(handler_.function(),
                                            data),
                             requestedAcks_,
                             systemAddedAcks_, false);
          }
        }
      }
      else if ((commandType == Message::Command::Publish ||
                commandType == Message::Command::DeltaPublish)
               && requestedAcks_ & ~Message::AckType::Persisted)
      {
        cid_ = command_.getMessage().newCommandId().getCommandId();
        _routes.addRoute(cid_, handler_, requestedAcks_,
                         systemAddedAcks_, false);
        added = true;
      }
      if (!added)
      {
        throw UsageException("To use a messagehandler, you must also supply a command or subscription ID.");
      }
    }

    std::string executeAsyncNoLock(Command& command_, MessageHandler& handler_,
                                   bool isHASubscribe_ = true)
    {
      isHASubscribe_ &= (bool)_subscriptionManager;
      Message& message = command_.getMessage();
      unsigned systemAddedAcks = (handler_.isValid() || command_.hasProcessedAck()) ?
                                 Message::AckType::Processed : Message::AckType::None;
      unsigned requestedAcks = message.getAckTypeEnum();
      bool isPublishStore = _publishStore.isValid() && command_.needsSequenceNumber();
      Message::Command::Type commandType = message.getCommandEnum();
      if (commandType == Message::Command::SOW
          || commandType == Message::Command::SOWAndSubscribe
          || commandType == Message::Command::SOWAndDeltaSubscribe
          || commandType == Message::Command::StopTimer)
      {
        systemAddedAcks |= Message::AckType::Completed;
      }
      Message::Field cid = message.getCommandId();
      if (handler_.isValid() && cid.empty())
      {
        cid = message.newCommandId().getCommandId();
      }
      if (message.getBookmark().len() > 0)
      {
        if (command_.isSubscribe())
        {
          Message::Field bookmark = message.getBookmark();
          if (_bookmarkStore.isValid())
          {
            systemAddedAcks |= Message::AckType::Persisted;
            if (bookmark == AMPS_BOOKMARK_RECENT)
            {
              message.setBookmark(_bookmarkStore.getMostRecent(message.getSubscriptionId()));
            }
            else if (bookmark != AMPS_BOOKMARK_NOW &&
                     bookmark != AMPS_BOOKMARK_EPOCH)
            {
              _bookmarkStore.log(message);
              if (!BookmarkRange::isRange(bookmark))
              {
                _bookmarkStore.discard(message);
                _bookmarkStore.persisted(message.getSubscriptionId(),
                                         bookmark);
              }
            }
          }
          else if (bookmark == AMPS_BOOKMARK_RECENT)
          {
            message.setBookmark(AMPS_BOOKMARK_EPOCH);
          }
        }
      }
      if (isPublishStore)
      {
        systemAddedAcks |= Message::AckType::Persisted;
      }
      bool isSubscribe = command_.isSubscribe();
      if (handler_.isValid() && !isSubscribe)
      {
        _registerHandler(command_, cid, handler_,
                         requestedAcks, systemAddedAcks, isSubscribe);
      }
      bool useSyncSend = cid.len() > 0 && command_.hasProcessedAck();
      if (isPublishStore)
      {
        amps_uint64_t haSequenceNumber = (amps_uint64_t)0;
        message.setAckTypeEnum(requestedAcks | systemAddedAcks);
        {
          Unlock<Mutex> u(_lock);
          haSequenceNumber = _publishStore.store(message);
        }
        message.setSequence(haSequenceNumber);
        try
        {
          if (useSyncSend)
          {
            syncAckProcessing((long)command_.getTimeout(), message,
                              haSequenceNumber);
          }
          else
          {
            _send(message, haSequenceNumber);
          }
        }
        catch (const DisconnectedException&)
        {
          // -V565
          // Pass - message will get replayed when reconnected
        }
        catch (...)
        {
          AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(cid));
          throw;
        }
      }
      else
      {
        if (isSubscribe)
        {
          const Message::Field& subId = message.getSubscriptionId();
          if (isHASubscribe_)
          {
            Unlock<Mutex> u(_lock);
            _subscriptionManager->subscribe(handler_,
                                            message.deepCopy(),
                                            requestedAcks);
            if (_badTimeToHASubscribe)
            {
              message.setAckTypeEnum(requestedAcks);
              return std::string(subId.data(), subId.len());
            }
          }
          if (handler_.isValid())
          {
            _registerHandler(command_, cid, handler_,
                             requestedAcks, systemAddedAcks, isSubscribe);
          }
          message.setAckTypeEnum(requestedAcks | systemAddedAcks);
          try
          {
            if (useSyncSend)
            {
              syncAckProcessing((long)command_.getTimeout(), message,
                                isHASubscribe_);
            }
            else
            {
              _send(message);
            }
          }
          catch (const DisconnectedException&)
          {
            if (!isHASubscribe_)
            {
              AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(cid));
              AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(subId));
              AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(message.getQueryId()));
              message.setAckTypeEnum(requestedAcks);
              throw;
            }
          }
          catch (const TimedOutException&)
          {
            AMPS_CALL_EXCEPTION_WRAPPER(unsubscribeInternal(cid));
            AMPS_CALL_EXCEPTION_WRAPPER(unsubscribeInternal(subId));
            AMPS_CALL_EXCEPTION_WRAPPER(unsubscribeInternal(message.getQueryId()));
            throw;
          }
          catch (...)
          {
            if (isHASubscribe_)
            {
              // Have to unlock before calling into sub manager to avoid deadlock
              Unlock<Mutex> unlock(_lock);
              _subscriptionManager->unsubscribe(subId);
            }
            if (message.getQueryID().len() > 0)
            {
              _routes.removeRoute(message.getQueryID());
            }
            _routes.removeRoute(cid);
            _routes.removeRoute(subId);
            throw;
          }
          if (subId.len() > 0)
          {
            message.setAckTypeEnum(requestedAcks);
            return std::string(subId.data(), subId.len());
          }
        }
        else
        {
          message.setAckTypeEnum(requestedAcks | systemAddedAcks);
          try
          {
            if (useSyncSend)
            {
              syncAckProcessing((long)(command_.getTimeout()), message);
            }
            else
            {
              _send(message);
            }
          }
          catch (const DisconnectedException&)
          {
            AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(cid));
            AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(message.getQueryId()));
            message.setAckTypeEnum(requestedAcks);
            throw;
          }
          catch (...)
          {
            AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(cid));
            AMPS_CALL_EXCEPTION_WRAPPER(_routes.removeRoute(message.getQueryId()));
            message.setAckTypeEnum(requestedAcks);
            throw;
          }
        }
      }
      message.setAckTypeEnum(requestedAcks);
      return cid;
    }

    MessageStream getEmptyMessageStream(void);

    std::string executeAsync(Command& command_, MessageHandler& handler_,
                             bool isHASubscribe_ = true)
    {
      Lock<Mutex> lock(_lock);
      return executeAsyncNoLock(command_, handler_, isHASubscribe_);
    }

    // Queue Methods //
    void setAutoAck(bool isAutoAckEnabled_)
    {
      _isAutoAckEnabled = isAutoAckEnabled_;
    }
    bool getAutoAck(void) const
    {
      return _isAutoAckEnabled;
    }
    void setAckBatchSize(const unsigned batchSize_)
    {
      _ackBatchSize = batchSize_;
      if (!_queueAckTimeout)
      {
        _queueAckTimeout = AMPS_DEFAULT_QUEUE_ACK_TIMEOUT;
        amps_client_set_idle_time(_client, _queueAckTimeout);
      }
    }
    unsigned getAckBatchSize(void) const
    {
      return _ackBatchSize;
    }
    int getAckTimeout(void) const
    {
      return _queueAckTimeout;
    }
    void setAckTimeout(const int ackTimeout_)
    {
      amps_client_set_idle_time(_client, ackTimeout_);
      _queueAckTimeout = ackTimeout_;
    }
    size_t _ack(QueueBookmarks& queueBookmarks_)
    {
      if (queueBookmarks_._bookmarkCount)
      {
        if (!publishStoreMessage)
        {
          publishStoreMessage = new Message();
          PerThreadMessageTracker::addMessageToCleanupList(publishStoreMessage);
        }
        publishStoreMessage->reset();
        publishStoreMessage->setCommandEnum(Message::Command::SOWDelete)
        .setTopic(queueBookmarks_._topic)
        .setBookmark(queueBookmarks_._data)
        .setCommandId("AMPS-queue-ack");
        amps_uint64_t haSequenceNumber = 0;
        if (_publishStore.isValid())
        {
          haSequenceNumber = _publishStore.store(*publishStoreMessage);
          publishStoreMessage->setAckType("persisted")
          .setSequence(haSequenceNumber);
          queueBookmarks_._data.erase();
          queueBookmarks_._bookmarkCount = 0;
        }
        _send(*publishStoreMessage, haSequenceNumber);
        if (!_publishStore.isValid())
        {
          queueBookmarks_._data.erase();
          queueBookmarks_._bookmarkCount = 0;
        }
        return 1;
      }
      return 0;
    }
    void ack(const Field& topic_, const Field& bookmark_, const char* options_ = NULL)
    {
      if (_isAutoAckEnabled)
      {
        return;
      }
      _ack(topic_, bookmark_, options_);
    }
    void _ack(const Field& topic_, const Field& bookmark_, const char* options_ = NULL)
    {
      if (bookmark_.len() == 0)
      {
        return;
      }
      Lock<Mutex> lock(_lock);
      if (_ackBatchSize < 2 || options_ != NULL)
      {
        if (!publishStoreMessage)
        {
          publishStoreMessage = new Message();
          PerThreadMessageTracker::addMessageToCleanupList(publishStoreMessage);
        }
        publishStoreMessage->reset();
        publishStoreMessage->setCommandEnum(Message::Command::SOWDelete)
        .setCommandId("AMPS-queue-ack")
        .setTopic(topic_).setBookmark(bookmark_);
        if (options_)
        {
          publishStoreMessage->setOptions(options_);
        }
        amps_uint64_t haSequenceNumber = 0;
        if (_publishStore.isValid())
        {
          haSequenceNumber = _publishStore.store(*publishStoreMessage);
          publishStoreMessage->setAckType("persisted")
          .setSequence(haSequenceNumber);
        }
        _send(*publishStoreMessage, haSequenceNumber);
        return;
      }
      // have we acked anything for this hash
      topic_hash hash = CRC<0>::crcNoSSE(topic_.data(), topic_.len());
      TopicHashMap::iterator it = _topicHashMap.find(hash);
      if (it == _topicHashMap.end())
      {
        // add a new one to the map
#ifdef AMPS_USE_EMPLACE
        it = _topicHashMap.emplace(TopicHashMap::value_type(hash, QueueBookmarks(topic_))).first;
#else
        it = _topicHashMap.insert(TopicHashMap::value_type(hash, QueueBookmarks(topic_))).first;
#endif
      }
      QueueBookmarks& queueBookmarks = it->second;
      if (queueBookmarks._data.length())
      {
        queueBookmarks._data.append(",");
      }
      else
      {
        queueBookmarks._oldestTime = amps_now();
      }
      queueBookmarks._data.append(bookmark_);
      if (++queueBookmarks._bookmarkCount >= _ackBatchSize)
      {
        _ack(queueBookmarks);
      }
    }
    void flushAcks(void)
    {
      size_t sendCount = 0;
      if (!_connected)
      {
        return;
      }
      else
      {
        Lock<Mutex> lock(_lock);
        typedef TopicHashMap::iterator iterator;
        for (iterator it = _topicHashMap.begin(), end = _topicHashMap.end(); it != end; ++it)
        {
          QueueBookmarks& queueBookmarks = it->second;
          sendCount += _ack(queueBookmarks);
        }
      }
      if (sendCount && _connected)
      {
        publishFlush(0, Message::AckType::Processed);
      }
    }
    // called when there's idle time, to see if we need to flush out any "acks"
    void checkQueueAcks(void)
    {
      if (!_topicHashMap.size())
      {
        return;
      }
      Lock<Mutex> lock(_lock);
      try
      {
        amps_uint64_t threshold = amps_now()
                                  - (amps_uint64_t)_queueAckTimeout;
        typedef TopicHashMap::iterator iterator;
        for (iterator it = _topicHashMap.begin(), end = _topicHashMap.end(); it != end; ++it)
        {
          QueueBookmarks& queueBookmarks = it->second;
          if (queueBookmarks._bookmarkCount && queueBookmarks._oldestTime < threshold)
          {
            _ack(queueBookmarks);
          }
        }
      }
      catch (std::exception& ex)
      {
        AMPS_UNHANDLED_EXCEPTION(ex);
      }
    }

    void deferredExecution(DeferredExecutionFunc func_, void* userData_)
    {
      Lock<Mutex> lock(_deferredExecutionLock);
#ifdef AMPS_USE_EMPLACE
      _deferredExecutionList.emplace_back(
        DeferredExecutionRequest(func_, userData_));
#else
      _deferredExecutionList.push_back(
        DeferredExecutionRequest(func_, userData_));
#endif
    }

    inline void processDeferredExecutions(void)
    {
      if (_deferredExecutionList.size())
      {
        Lock<Mutex> lock(_deferredExecutionLock);
        DeferredExecutionList::iterator it = _deferredExecutionList.begin();
        DeferredExecutionList::iterator end = _deferredExecutionList.end();
        for (; it != end; ++it)
        {
          try
          {
            it->_func(it->_userData);
          }
          catch (...)
          {
            // -V565
            // Intentionally ignore errors
          }
        }
        _deferredExecutionList.clear();
        _routes.invalidateCache();
        _routeCache.invalidateCache();
      }
    }

    bool getRetryOnDisconnect(void) const
    {
      return _isRetryOnDisconnect;
    }

    void setRetryOnDisconnect(bool isRetryOnDisconnect_)
    {
      _isRetryOnDisconnect = isRetryOnDisconnect_;
    }

    void setDefaultMaxDepth(unsigned maxDepth_)
    {
      _defaultMaxDepth = maxDepth_;
    }

    unsigned getDefaultMaxDepth(void) const
    {
      return _defaultMaxDepth;
    }

    void setTransportFilterFunction(amps_transport_filter_function filter_,
                                    void* userData_)
    {
      amps_client_set_transport_filter_function(_client, filter_, userData_);
    }

    void setThreadCreatedCallback(amps_thread_created_callback callback_,
                                  void* userData_)
    {
      amps_client_set_thread_created_callback(_client, callback_, userData_);
    }
  }; // class ClientImpl
///
/// An iterable object representing the results of an AMPS subscription and/or
/// query.
/// Objects of MesageStream type are returned by the MessageStream-returning overloads of methods
/// on class AMPS::Client, such as \link Client::sow(const std::string&,const std::string&,const std::string&,const std::string&,int,int,const std::string&,long) AMPS::Client::sow() \endlink
/// and \link Client::subscribe(const std::string&,long,const std::string&, const std::string&, const std::string&) AMPS::Client::subscribe() \endlink.
/// Use this object via the begin() and end() iterators returned, for example:
/// \code
/// MessageStream myTopic = client.sow("orders");
/// for(auto i = myTopic.begin(), e = myTopic.end(); i!=e; ++i)
/// {
///   Message m = *i;
///   ... // (work with the contents of m)
/// }
/// \endcode
/// For MessageStream objects returned by calls to methods other than sow(),
/// note that the "end" is never reached, unless the server is disconnected.
/// In this case, you may choose to stop processing messages by simply exiting the loop.
/// For example, assuming that the `isDone` flag will be set when the application
/// has processed the last message in a job:
///
/// \code
/// MessageStream myTopic = client.subscribe("aTopic");
/// for(auto i = myTopic.begin();; ++i)
/// {
///   Message m = *i;
///   ... // do work with *i
///   if(imDone) break;
/// }
/// \endcode
///
/// By default, a MessageStream for a subscription will block until a message
/// is available or the connection is closed. You can also configure a
/// MessageStream to periodically produce an invalid (empty) message by
/// setting the timeout on the message stream. This can be useful for
/// indicating that the process is still working (for example, pinging a
/// thread monitor) or reporting an error if a subscription pauses for an
/// unexpectedly long period of time.
///
/// For example, the following code configures the message stream to
/// produce an invalid message if no data arrives for this subscription
/// within 1 second (1000 milliseconds). Even though there is no work
/// for the application to do, this gives the application the opportunity
/// to ping a thread monitor. Further, the application keeps track of how
/// long it has been since the last data message arrived. If data flow
/// pauses for more than 4 minutes (240 seconds) continuously, the
/// application reports an error.
///
/// \code
/// {
///    MessageStream ms = client.subscribe("aTopic");
///    ms.timeout(1000);
///    int timeout_counter = 0;
///
///    for (auto m : ms)
///    {
///       if (m.isValid() == false)
///       {
///
///          if(timeout_counter++ > 240) { /* report critical error */}
///
///          /* otherwise ping thread monitor, then */
///         continue;
///        }
///
///        timeout_counter = 0;
///
///       ... process message here, pinging thread monitor as appropriate ...
///    }
///
/// } // MessageStream is unsubscribed and destroyed here
/// \endcode
///
/// \ingroup sync

  class MessageStream
  {
    RefHandle<MessageStreamImpl> _body;
  public:
    /// Represents an iterator over messages in an AMPS topic.
    /// Note that this iteration is performed over the messages in the AMPS server,
    /// meaning the ++ operator may result in an exception if an error occurs
    /// (such as a disconnect from the AMPS instance.)
    class iterator
    {
      MessageStream* _pStream;
      Message _current;
      inline void advance(void);

    public:
      iterator() // end
        : _pStream(NULL)
      {;}
      iterator(MessageStream* pStream_)
        : _pStream(pStream_)
      {
        advance();
      }

      bool operator==(const iterator& rhs) const
      {
        return _pStream == rhs._pStream;
      }
      bool operator!=(const iterator& rhs) const
      {
        return _pStream != rhs._pStream;
      }
      void operator++(void)
      {
        advance();
      }
      Message operator*(void)
      {
        return _current;
      }
      Message* operator->(void)
      {
        return &_current;
      }
    };
    /// Returns true if self is a valid stream that may be iterated.
    bool isValid() const
    {
      return _body.isValid();
    }

    /// Returns an iterator representing the beginning of the topic or subscription.
    /// \returns An iterator representing the beginning of this topic.
    iterator begin(void)
    {
      if (!_body.isValid())
      {
        throw UsageException("This MessageStream is not valid and cannot be iterated.");
      }
      return iterator(this);
    }
    /// Returns an iterator representing the end of the topic or subscription.
    /// \returns An iterator representing the ending of this topic or subscription.
    //           For non-SOW queries, the end is never reached.
    iterator end(void)
    {
      return iterator();
    }
    inline MessageStream(void);

    /// Sets the maximum time to wait for the next message in milliseconds; if no
    /// message is available within this timeout, return an invalid Message (that
    /// is, a message where isValid() returns false).
    /// \param timeout_ The maximum milliseconds to wait for messages, 0 means no maximum.
    /// \returns This MessageStream
    MessageStream timeout(unsigned timeout_);

    /// Sets self to conflation mode, where a new update for a matching sow key
    /// will replace the previous one in the underlying queue.
    /// \returns This MessageStream
    MessageStream conflate(void);
    /// Sets the maximum number of messages that can be held in the underlying
    /// queue. When that depth is reached, the receive thread will keep trying
    /// to store the next message before receiving any other new messages.
    /// \param maxDepth_ The maximum number of messages, 0 means no maximum.
    /// \returns This MessageStream
    MessageStream maxDepth(unsigned maxDepth_);
    /// Gets the maximum number of messages that can be held in the underlying queue.
    /// \returns The maximum depth allowed.
    unsigned getMaxDepth(void) const;
    /// Gets the current number of messages held in the underlying queue.
    /// \returns The current depth of unprocessed messages.
    unsigned getDepth(void) const;

  private:
    inline MessageStream(const Client& client_);
    inline void setSOWOnly(const std::string& commandId_,
                           const std::string& queryId_ = "");
    inline void setSubscription(const std::string& subId_,
                                const std::string& commandId_ = "",
                                const std::string& queryId_ = "");
    inline void setStatsOnly(const std::string& commandId_,
                             const std::string& queryId_ = "");
    inline void setAcksOnly(const std::string& commandId_, unsigned acks_);

    inline operator MessageHandler(void);

    inline static MessageStream fromExistingHandler(const MessageHandler& handler);

    friend class Client;

  };

///
/// Client represents a connection to an AMPS server, but does
/// not provide failover or reconnection behavior by default. The class
/// provides methods for executing commands against the server,
/// (for example, publishing messages, subscribing, and so on.)
///
/// A Client object has a single connection to an AMPS server at a
/// time. That connection is serviced by a background thread created
/// by the Client. When using the Asynchronous Message Processing
/// interface, messages are delivered to the MessageHandler callback
/// on the background thread.
///
/// A Client object is implemented as a reference-counted handle to
/// an underlying object. This means that a Client can be safely
/// and efficiently passed by value.
///
/// For applications that require automatic failover and reconnection to AMPS,
/// 60East recommends using the HAClient subclass instead of the Client.
///
  class Client // -V553
  {
  protected:
    BorrowRefHandle<ClientImpl> _body;
  public:
    static const int DEFAULT_COMMAND_TIMEOUT = AMPS_DEFAULT_COMMAND_TIMEOUT;
    static const int DEFAULT_BATCH_SIZE      = AMPS_DEFAULT_BATCH_SIZE;
    static const int DEFAULT_TOP_N           = AMPS_DEFAULT_TOP_N;

    /// Constructs a new client with a given client name.
    /// \param clientName Name for the client. This name is used for duplicate
    /// message detection and should be unique. AMPS does not enforce
    /// specific restrictions on the character set used, however some
    /// protocols (for example, XML) may not allow specific characters.
    /// 60East recommends that the client name be meaningful, short, human
    /// readable, and avoids using control characters, newline characters,
    /// or square brackets.
    Client(const std::string& clientName = "")
      : _body(new ClientImpl(clientName), true)
    {;}

    Client(ClientImpl* existingClient)
      : _body(existingClient, true)
    {;}

    Client(ClientImpl* existingClient, bool isRef)
      : _body(existingClient, isRef)
    {;}

    Client(const Client& rhs) : _body(rhs._body) {;}
    virtual ~Client(void) {;}

    Client& operator=(const Client& rhs)
    {
      _body = rhs._body;
      return *this;
    }

    bool isValid()
    {
      return _body.isValid();
    }

    /// Sets the name of this client, assuming no name was
    /// provided previously.  This may only be called on
    /// client objects with no name.
    /// \param name Name for the client. This name is used for duplicate
    /// message detection and should be unique. AMPS does not enforce
    /// specific restrictions on the character set used, however some
    /// protocols (for example, XML) may not allow specific characters.
    /// 60East recommends that the client name be meaningful, short, human
    /// readable, and avoids using control characters, newline characters,
    /// or square brackets.
    /// \throw UsageException An attempt was made to set a name on a client,
    ///   but the client already has a name.
    void setName(const std::string& name)
    {
      _body.get().setName(name);
    }

    /// Returns the name of this client passed in the constructor.
    /// \return The client name passed via the constructor.
    const std::string& getName() const
    {
      return _body.get().getName();
    }

    /// Returns the name hash string of this client as generated by the server
    /// and returned when the client logged on.
    /// \return The client name hash from the server after logon.
    const std::string& getNameHash() const
    {
      return _body.get().getNameHash();
    }

    /// Returns the numeric name hash of this client as generated by the server
    /// and returned when the client logged on.
    /// \return The client name hash from the server after logon.
    const amps_uint64_t getNameHashValue() const
    {
      return _body.get().getNameHashValue();
    }

    /// Sets the logon correlation data for the client. The data should be in
    /// base64 characters. It is not escaped when sent to the server and the
    /// server does not interpret the data. The data can provide some extra
    /// information about the client that is logging into the server that may
    /// not be available from just the client name and user id.
    /// \param logonCorrelationData_ The data to be sent with the logon.
    void setLogonCorrelationData(const std::string& logonCorrelationData_)
    {
      _body.get().setLogonCorrelationData(logonCorrelationData_);
    }

    /// Returns the currently set logon correlation data for the client.
    /// \return The data that will be sent with the logon.
    const std::string& getLogonCorrelationData() const
    {
      return _body.get().getLogonCorrelationData();
    }

    /// Returns the server version retrieved during logon. If the client has
    /// not logged on or is connected to a server whose version is less than
    /// 3.8.0.0 this function will return 0.
    /// The version uses 2 digits each for major, minor, maintenance & hotfix
    /// i.e., 3.8.1.4 will return 3080104
    /// Versions of AMPS prior to 3.8.0.0 do not return the server version to
    /// the client.
    /// \return The version of the server as a size_t.
    size_t getServerVersion() const
    {
      return _body.get().getServerVersion();
    }

    /// Returns the server version retrieved during logon. If the client has
    /// not logged on or is connected to a server whose version is less than
    /// 3.8.0.0 this function will return a default version.
    /// Versions of AMPS prior to 3.8.0.0 do not return the server version to
    /// the client.
    /// \return The version of the server in a VersionInfo.
    VersionInfo getServerVersionInfo() const
    {
      return _body.get().getServerVersionInfo();
    }

    /// Converts a string version, such as "3.8.1.5" into the same numeric
    /// form used internally and returned by getServerVersion. This can
    /// be used to do comparisons such as
    /// client.getServerVersion() >= "3.8"
    /// Conversion works with any string that starts with a number, and will
    /// assume 0 for any portions of the version not present. So "4" will
    /// return 4000000, equivalent to "4.0.0.0"
    /// \param version_ The version string to convert.
    /// \return The size_t equivalent of the version.
    static size_t convertVersionToNumber(const std::string& version_)
    {
      return AMPS::convertVersionToNumber(version_.c_str(), version_.length());
    }

    /// Converts a string version, such as "3.8.1.5" into the same numeric
    /// form used internally and returned by getServerVersion. This can
    /// be used to do comparisons such as
    /// client.getServerVersion() >= "3.8"
    /// Conversion works with any string that starts with a number, and will
    /// assume 0 for any portions of the version not present. So "4" will
    /// return 4000000, equivalent to "4.0.0.0"
    /// \param data_ The pointer the start of the character data to convert.
    /// \param len_ The length of the character data to convert.
    /// \return The size_t equivalent of the version.
    static size_t convertVersionToNumber(const char* data_, size_t len_)
    {
      return AMPS::convertVersionToNumber(data_, len_);
    }

    /// Returns the last URI this client is connected to.
    /// \return The URI passed in to connect.
    const std::string& getURI() const
    {
      return _body.get().getURI();
    }

    /// Connect to an AMPS server.
    /// Connects to the AMPS server specified by the uri parameter.
    /// \param uri The URI of the desired AMPS server, with the format
    /// <c>transport://user:password\@host:port/protocol</c>,
    /// for example, <c>tcp://localhost:9007/amps</c>.
    ///

    /// <p>The elements in the URI string are as follows:

    /// <ul>
    /// <li><i>transport</i> -- the network transport used for the connection</li>
    /// <li><i>user</i> -- <i>(optional)</i> the userId used to authenticate the connection</li>
    /// <li><i>password</i> -- <i>(optional)</i> the password used to authenticate the connection</li>
    /// <li><i>host</i> -- the hostname or IP address of the host where AMPS is running</li>
    /// <li><i>port</i> -- the port to connect to
    /// <li><i>protocol</i> -- the protocol used for the connection
    /// </ul></p>
    /// \throw InvalidURIException The URI passed in is invalid.
    /// \throw ConnectionRefusedException The connection could not be established.
    void connect(const std::string& uri)
    {
      _body.get().connect(uri);
    }

    /// Disconnect from an AMPS server.
    /// Disconnects from the currently-connected AMPS server.
    void disconnect()
    {
      _body.get().disconnect();
    }

    /// Sends a Message to the connected AMPS server, performing only
    /// minimal validation and bypassing client services such as the
    /// publish store. This method is provided for special cases.
    /// The Message must be formatted properly with the appropriate fields
    /// set for the command you wish to issue.  No adjustments are made to
    /// the message before sending. Notice that this function bypasses
    /// validation of the Message contents, does not record the contents of
    /// the Message in the publish store(if one is present) and so on.
    /// In general, applications use a Command object with the execute or
    /// executeAsync functions rather than using this function.
    /// \param message The Message to send to the AMPS server.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    void send(const Message& message)
    {
      _body.get().send(message);
    }

    /// Adds a MessageHandler to be invoked for Messages with the given
    /// CommandId as their command id, sub id, or query id.
    /// \param commandId_ The command, query, or subid used to invoke the
    /// handler.
    /// \param messageHandler_ The message handler to route to
    /// \param requestedAcks_ The actual acks requested from amps for this
    /// command
    /// \param isSubscribe_ True is the handler is for an ongoing subscription.
    void addMessageHandler(const Field& commandId_,
                           const AMPS::MessageHandler& messageHandler_,
                           unsigned requestedAcks_, bool isSubscribe_)
    {
      _body.get().addMessageHandler(commandId_, messageHandler_,
                                    requestedAcks_, isSubscribe_);
    }

    /// Removes a MessageHandler for a given ComandId from self.
    /// \param commandId_ The CommandId for which to remove the handler.
    /// \return True if the handler was removed.
    bool removeMessageHandler(const Field& commandId_)
    {
      return _body.get().removeMessageHandler(commandId_);
    }

    /// Sends a Message to the connected AMPS server, performing only
    /// minimal validation and bypassing client services such as the
    /// publish store. This method is provided for special cases.
    /// The {@link Message} must be formatted properly with the appropriate fields
    /// set for the command you wish to issue.  No adjustments are made to
    /// the message before sending. Notice that this function bypasses
    /// validation of the Message contents, does not record the contents of
    /// the Message in the publish store(if one is present) and so on.
    /// In general, applications use a Command object with the execute or
    /// executeAsync functions rather than using this function.
    /// \param messageHandler The MessageHandler to associate with this message.
    /// \param message The message to send to the AMPS server.
    /// \param timeout When submitting a command, the time to wait for the receive thread to consume a processed ack, in milliseconds. 0 indicates no timeout.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw NameInUseException For a logon command, the specified client name is already in use on the server, and will not allow this connection to replace that connection (for example, if the two connections use different credentials)
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    std::string send(const MessageHandler& messageHandler, Message& message, int timeout = 0)
    {
      return _body.get().send(messageHandler, message, timeout);
    }

    /// Sets the function to be called when the client is unintentionally disconnected.
    /// Sets a callback function that is invoked when a disconnect is detected.
    /// For automatically reconnecting a client, 60East recommends using
    /// the HAClient rather than creating a disconnect handler. Notice that
    /// the reconnect, republish, and resubscribe behavior for the HAClient
    /// class is provided through the disconnect handler, so setting a
    /// disconnect handler for the HAClient will completely replace the
    /// high-availability behavior of that client.
    /// \param disconnectHandler The callback to be invoked on disconnect.
    void setDisconnectHandler(const DisconnectHandler& disconnectHandler)
    {
      _body.get().setDisconnectHandler(disconnectHandler);
    }

    ///
    /// Returns the callback function that is invoked when a disconnect occurs.
    /// \return The current DisconnectHandler callback to be invoked for a disconnect.
    DisconnectHandler getDisconnectHandler(void) const
    {
      return _body.get().getDisconnectHandler();
    }

    ///
    /// Get the connection information for the current connection.
    /// \return A ConnectionInfo object with the information describing the
    /// the current connection.
    virtual ConnectionInfo getConnectionInfo() const
    {
      return _body.get().getConnectionInfo();
    }

    ///
    /// Set the bookmark store to be used by the client. The bookmark
    /// store is used for recovery when an AMPS client requests a replay
    /// from the transaction log. The guarantees provided by the
    /// bookmark store depend on the implementation chosen. See the
    /// Developer Guide for details.
    /// \param bookmarkStore_ The bookmark store to use.
    /// \throws AlreadyConnectedException If called while the client is already connected
    void setBookmarkStore(const BookmarkStore& bookmarkStore_)
    {
      _body.get().setBookmarkStore(bookmarkStore_);
    }

    ///
    /// Get the bookmark store being used by the client.
    /// \return The bookmark store being used.
    BookmarkStore getBookmarkStore()
    {
      return _body.get().getBookmarkStore();
    }

    ///
    /// Get the subscription manager being used by the client.
    /// \return The subscription manager being used.
    SubscriptionManager* getSubscriptionManager()
    {
      return _body.get().getSubscriptionManager();
    }

    ///
    /// Set the subscription manager to be used by the client. It is
    /// the disconnect handler's responsibility to use this subscription
    /// manager to re-establish subscriptions. By default,
    /// an HAClient will set a MemorySubscriptionManager and use this
    /// subscription manager during reconnection.
    /// \param subscriptionManager_ The subscription manager to use.
    void setSubscriptionManager(SubscriptionManager* subscriptionManager_)
    {
      _body.get().setSubscriptionManager(subscriptionManager_);
    }

    ///
    /// Set the publish store to be used by the client. A publish store is
    /// used to help ensure that messages reach AMPS, even in cases of
    /// connection failure or application failure. The client automatically
    /// stores appropriate messages in the publish store when using the
    /// named commands (such as publish) or the execute or executeAsync
    /// functions. With the HAClient, that class's default disconnect handler
    /// automatically republishes messages from the publish store as needed
    /// when a connection is made.
    ///
    /// The precise guarantees provided by a publish store depend on the
    /// publish store implementation. See the Developer
    /// Guide for details on choosing a publish store.
    ///
    /// When a publish store is set on a client, 60East strongly recommends
    /// providing a FailedWriteHandler so that the application is notified
    /// of any failures in publishing messages.
    /// \param publishStore_ The publish store to use.
    /// \throws AlreadyConnectedException If called while the client is already connected
    void setPublishStore(const Store& publishStore_)
    {
      _body.get().setPublishStore(publishStore_);
    }

    ///
    /// Get the publish store used by the client.
    /// \return The publish store being used.
    Store getPublishStore()
    {
      return _body.get().getPublishStore();
    }

    ///
    /// Sets a callback function that is invoked when a duplicate message is detected.
    /// \param duplicateMessageHandler_ The callback to be invoked.
    void setDuplicateMessageHandler(const MessageHandler& duplicateMessageHandler_)
    {
      _body.get().setGlobalCommandTypeMessageHandler(ClientImpl::GlobalCommandTypeHandlers::DuplicateMessage,
                                                     duplicateMessageHandler_);
    }

    ///
    /// Returns the callback function that is invoked when a duplicate message is detected.
    /// Notice that in a failover scenario, a duplicate message does not generally
    /// indicate an error. Instead, during failover, a duplicate message most often
    /// indicates that the publisher established a connection to a failover
    /// server before the original server had finished replicating messages
    /// to the failover server, but that the replicated messages arrived before
    /// the publisher republished the messages.
    /// \return The current MessageHandler callback to be invoked for duplicate messages.
    MessageHandler getDuplicateMessageHandler(void)
    {
      return _body.get().getDuplicateMessageHandler();
    }

    ///
    /// Set the handler that is invoked to report when a publish fails, for
    /// example if the publisher is not entitled, if AMPS attempts to
    /// parse the message and the parse fails, and so on.
    /// The Client takes ownership of the pointer provided.
    /// In order to receive failure acknowledgements, the publish commands
    /// must request an acknowledgement from AMPS. The AMPS client automatically
    /// requests these acknowledgements when a publish store is present.
    /// \param handler_ The handler to invoke for failed writes.
    void setFailedWriteHandler(FailedWriteHandler* handler_)
    {
      _body.get().setFailedWriteHandler(handler_);
    }

    ///
    /// Get the handler that is invoked to report on failed writes.
    /// \return The handler that is invoked for failed writes.
    FailedWriteHandler* getFailedWriteHandler()
    {
      return _body.get().getFailedWriteHandler();
    }


    /// Publish a message to an AMPS topic, returning the sequence number
    /// assigned by the publish store if one is present on the client.
    /// Notice that this function does not wait for acknowledgement from the
    /// AMPS server. To detect failure, set a failed write handler on the
    /// client.
    /// If the client has a publish store set, then the client will automatically
    /// store the message before forwarding the message to AMPS.
    /// If a DisconnectException occurs, the message is still
    /// stored in the publish store and can be forwarded when the connection is
    /// re-established.
    /// \param topic_ The topic to publish on.
    /// \param data_ The data to include in the message.
    /// \throw DisconnectedException The Client is not connected to a server;
    /// the program needs to call connect().
    /// \throw ConnectionException An error occurred while sending the message.
    /// \returns The sequence number of the message when this client uses a
    ///  publish store, 0 otherwise.
    amps_uint64_t publish(const std::string& topic_, const std::string& data_)
    {
      return _body.get().publish(topic_.c_str(), topic_.length(),
                                 data_.c_str(), data_.length());
    }

    /// Publish a message to an AMPS topic, returning the sequence number
    /// assigned by the publish store if one is present on the client.
    /// Notice that this function does not wait for acknowledgement from the
    /// AMPS server. To detect failure, set a failed write handler on the
    /// client.
    /// If the client has a publish store set, then the client will automatically
    /// store the message before forwarding the message to AMPS.
    /// If a DisconnectException occurs, the message is still
    /// stored in the publish store and can be forwarded when the connection is
    /// re-established.
    /// \param topic_ The topic to publish on.
    /// \param topicLength_ The length, in bytes, of the topic string.
    /// \param data_ The data to include in the message.
    /// \param dataLength_ The length, in bytes, of the data.
    /// \throw DisconnectedException The Client is not connected to a server;
    /// the program needs to call connect().
    /// \throw ConnectionException An error occurred while sending the message.
    /// \returns The sequence number of the message when this client uses a
    ///  publish store, 0 otherwise.
    amps_uint64_t publish(const char* topic_, size_t topicLength_,
                          const char* data_, size_t dataLength_)
    {
      return _body.get().publish(topic_, topicLength_, data_, dataLength_);
    }

    /// Publish a message to an AMPS topic, returning the sequence number
    /// assigned by the publish store (if a publish store is present on the client).
    /// Notice that this function does not wait for acknowledgement from the
    /// AMPS server. To detect failure, set a failed write handler on the
    /// client.
    /// If the client has a publish store set, then the client will automatically
    /// store the message before forwarding the message to AMPS.
    /// If a DisconnectException occurs, the message is still
    /// stored in the publish store and can be forwarded when the connection is
    /// re-established.
    /// \param topic_ The topic to publish on.
    /// \param data_ The data to include in the message.
    /// \param expiration_ The number of seconds after which the message should expire
    /// \throw DisconnectedException The Client is not connected to a server;
    /// the program needs to call connect().
    /// \throw ConnectionException An error occurred while sending the message.
    /// \returns The sequence number of the message when this client uses a
    ///  publish store, 0 otherwise.
    amps_uint64_t publish(const std::string& topic_, const std::string& data_,
                          unsigned long expiration_)
    {
      return _body.get().publish(topic_.c_str(), topic_.length(),
                                 data_.c_str(), data_.length(), expiration_);
    }

    /// Publish a message to an AMPS topic, returning the sequence number
    /// assigned by the publish store if one is present on the client.
    /// Notice that this function does not wait for acknowledgement from the
    /// AMPS server. To detect failure, set a failed write handler on the
    /// client.
    /// If the client has a publish store set, then the client will automatically
    /// store the message before forwarding the message to AMPS.
    /// If a DisconnectException occurs, the message is still
    /// stored in the publish store and can be forwarded when the connection is
    /// re-established.
    /// \param topic_ The topic to publish on.
    /// \param topicLength_ The length, in bytes, of the topic string.
    /// \param data_ The data to include in the message.
    /// \param dataLength_ The length, in bytes, of the data.
    /// \param expiration_ The number of seconds after which the message should expire
    /// \throw DisconnectedException The Client is not connected to a server;
    /// the program needs to call connect().
    /// \throw ConnectionException An error occurred while sending the message.
    /// \returns The sequence number of the message when this client uses a
    ///  publish store, 0 otherwise.
    amps_uint64_t publish(const char* topic_, size_t topicLength_,
                          const char* data_, size_t dataLength_,
                          unsigned long expiration_)
    {
      return _body.get().publish(topic_, topicLength_,
                                 data_, dataLength_, expiration_);
    }

    /// Ensure that AMPS messages are sent and have been processed by the
    /// AMPS server. When a client is configured with a publish store,
    /// this function call blocks until all messages that are in the publish
    /// store at the point of the call have been acknowledged by AMPS.
    /// Otherwise, the client sends a message to AMPS and waits for that
    /// message to be processed. The mechanism that the client uses to do
    /// this depends on the version of AMPS that the client is connected to.
    /// When the client is connected to a server that implements
    /// the flush command, the client issues that command and waits for an
    /// acknowledgement. For older versions of AMPS, the client sends a
    /// publish to a dummy topic.
    ///
    /// This function blocks until one of the following is true:
    /// * The client has a publish store, and no messages that were in
    ///   the publish store at the time the function was called are
    ///   unacknowledged by AMPS.
    /// * The client does not have a publish store, and the results of the
    ///   command to the server have been received (indicating that the
    ///   server has processed all previous commands).
    /// * The specified timeout expires (in this case, the function throws
    ///   an exception).
    /// This function is most useful when the application reaches a point at
    /// which it is acceptable to block to ensure that messages are delivered
    /// to the AMPS server. For example, an application might call publishFlush
    /// before exiting.
    ///
    /// One thing to note is that if AMPS is unavailable (HA Client), publishFlush
    /// needs to wait for a connection to come back up which may look like it's hanging.
    /// \param timeout_ The maximum milliseconds to wait for flush to complete.
    /// Default is 0, which is no maximum (wait until the command completes).
    /// \param ackType_ Wait for Message::AckType::Processed or
    /// Message::AckType::Persisted ack.
    /// Default is Message::AckType::Processed, but Message::AckType::Persisted
    /// ensures all previous messages were persisted.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw CommandException An ack type other than Processed or Persisted was requested.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException The command did not complete in the specified time.
    void publishFlush(long timeout_ = 0, unsigned ackType_ = Message::AckType::Processed)
    {
      _body.get().publishFlush(timeout_, ackType_);
    }


    /// Publish the changed fields of a message to an AMPS topic.
    /// Send a delta_publish to the server, indicating that only changed fields
    /// are included in the data portion of this message.
    /// If the client has a publish store set,
    /// then the client will store the message before forwarding the message to AMPS.
    /// This method does not wait for a response from the AMPS server. To detect failure,
    /// install a failed write handler. If a DisconnectException occurs, the message is still
    /// stored in the publish store. See the AMPS User Guide and the Developer
    /// Guide for details on how delta messaging works.
    /// \param topic_ The topic to publish on.
    /// \param data_ The data corresponding to the fields you'd like to change.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \returns The sequence number of the message when this client uses a
    ///  publish store, 0 otherwise.
    amps_uint64_t deltaPublish(const std::string& topic_, const std::string& data_)
    {
      return _body.get().deltaPublish(topic_.c_str(), topic_.length(),
                                      data_.c_str(), data_.length());
    }

    /// Publish the changed fields of a message to an AMPS topic.
    /// Send a delta_publish to the server, indicating that only changed fields
    /// are included in the data portion of this message.
    /// If the client has a publish store set,
    /// then the client will store the message before forwarding the message to AMPS.
    /// This method does not wait for a response from the AMPS server. To detect failure,
    /// install a failed write handler. If a DisconnectException occurs, the message is still
    /// stored in the publish store. See the AMPS User Guide and the Developer
    /// Guide for details on how delta messaging works.
    /// \param topic_ The topic to publish on.
    /// \param topicLength_ The length, in bytes, of the topic string.
    /// \param data_ The data corresponding to the fields you'd like to change.
    /// \param dataLength_ The length, in bytes, of the data.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \returns The sequence number of the message when this client uses a
    ///  publish store, 0 otherwise.
    amps_uint64_t deltaPublish(const char* topic_, size_t topicLength_,
                               const char* data_, size_t dataLength_)
    {
      return _body.get().deltaPublish(topic_, topicLength_,
                                      data_, dataLength_);
    }

    /// Publish the changed fields of a message to an AMPS topic.
    /// Send a delta_publish to the server, indicating that only changed fields
    /// are included in the data portion of this message.
    /// If the client has a publish store set,
    /// then the client will store the message before forwarding the message to AMPS.
    /// This method does not wait for a response from the AMPS server. To detect failure,
    /// install a failed write handler. If a DisconnectException occurs, the message is still
    /// stored in the publish store. See the AMPS User Guide and the Developer
    /// Guide for details on how delta messaging works.
    /// \param topic_ The topic to publish on.
    /// \param data_ The data corresponding to the fields you'd like to change.
    /// \param expiration_ The number of seconds until the message expires.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \returns The sequence number of the message when this client uses a
    ///  publish store, 0 otherwise.
    amps_uint64_t deltaPublish(const std::string& topic_, const std::string& data_,
                               unsigned long expiration_)
    {
      return _body.get().deltaPublish(topic_.c_str(), topic_.length(),
                                      data_.c_str(), data_.length(),
                                      expiration_);
    }

    /// Publish the changed fields of a message to an AMPS topic.
    /// Send a delta_publish to the server, indicating that only changed fields
    /// are included in the data portion of this message.
    /// If the client has a publish store set,
    /// then the client will store the message before forwarding the message to AMPS.
    /// This method does not wait for a response from the AMPS server. To detect failure,
    /// install a failed write handler. If a DisconnectException occurs, the message is still
    /// stored in the publish store. See the AMPS User Guide and the Developer
    /// Guide for details on how delta messaging works.
    /// \param topic_ The topic to publish on.
    /// \param topicLength_ The length, in bytes, of the topic string.
    /// \param data_ The data corresponding to the fields you'd like to change.
    /// \param dataLength_ The length, in bytes, of the data.
    /// \param expiration_ The number of seconds until the message expires.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \returns The sequence number of the message when this client uses a
    ///  publish store, 0 otherwise.
    amps_uint64_t deltaPublish(const char* topic_, size_t topicLength_,
                               const char* data_, size_t dataLength_,
                               unsigned long expiration_)
    {
      return _body.get().deltaPublish(topic_, topicLength_,
                                      data_, dataLength_, expiration_);
    }

    /// Logon to the server, providing the client name, credentials (if available),
    /// and client information (such as client version and connection message type).
    /// Authentication is supplied using the provided authenticator (by default,
    /// the DefaultAuthenticator, which simply provides the password
    /// in the connection string if one is present).
    /// \param timeout_ The time to wait, in milliseconds, for an acknowledgement.  (Default: 0 -- wait forever.)
    /// \param authenticator_ The custom authenticator to use in the logon process.
    /// \param options_ The options to pass in with the logon command such as "conflation_interval=10ms".
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw PublishStoreGapException The server is not up to date on all messages previously published by this Client and a gap of missing messages would be created by publishing new messages here.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw NameInUseException The specified client name is already in use on the server, and will not allow this connection to replace that connection (for example, if the two connections use different credentials)
    std::string logon(int timeout_ = 0,
                      Authenticator& authenticator_ = DefaultAuthenticator::instance(),
                      const char* options_ = NULL)
    {
      return _body.get().logon(timeout_, authenticator_, options_);
    }
    /// Logon to the server, providing the client name, credentials (if available)
    /// client information (such as client version and connection message type), and
    /// the specified options. Authentication is supplied using the DefaultAuthenticator
    /// (which simply provides the password in the connection string if one is present).
    /// \param options_ The options to pass in with the logon command such as "conflation_interval=10ms".
    /// \param timeout_ The time to wait, in milliseconds, for an acknowledgement.  (Default: 0 -- wait forever.)
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw PublishStoreGapException The server is not up to date on all messages previously published by this Client and a gap of missing messages would be created by publishing new messages here.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw NameInUseException The specified client name is already in use on the server, and will not allow this connection to replace that connection (for example, if the two connections use different credentials)
    std::string logon(const char* options_, int timeout_ = 0)
    {
      return _body.get().logon(timeout_, DefaultAuthenticator::instance(),
                               options_);
    }

    /// Logon to the server, providing the client name, credentials (if available)
    /// client information (such as client version and connection message type), and
    /// the specified options. Authentication is supplied using the DefaultAuthenticator
    /// (which simply provides the password in the connection string if one is present).
    /// \param options_ The options to pass in with the logon command such as "conflation_interval=10ms".
    /// \param timeout_ The time to wait, in milliseconds, for an acknowledgement.  (Default: 0 -- wait forever.)
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw PublishStoreGapException The server is not up to date on all messages previously published by this Client and a gap of missing messages would be created by publishing new messages here.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw NameInUseException The specified client name is already in use on the server, and will not allow this connection to replace that connection (for example, if the two connections use different credentials)
    std::string logon(const std::string& options_, int timeout_ = 0)
    {
      return _body.get().logon(timeout_, DefaultAuthenticator::instance(),
                               options_.c_str());
    }

    /// Subscribe to a topic.
    /// Initiates a subscription to a topic on the server, using the provided
    /// messageHandler_ to receive messages for the subscription. The
    /// messageHandler_ is called on the client receive thread: see the
    /// Developer Guide for details on the threading considerations.
    /// \param messageHandler_ The function to be invoked when messages for this topic arrive.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param options_ Optional options to supply with this subscription.
    /// \param subId_ Optional subscription id to use with this subscription.
    /// \returns A std::string with a unique identifier for this subscription.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    std::string subscribe(const MessageHandler& messageHandler_,
                          const std::string& topic_,
                          long timeout_ = 0,
                          const std::string& filter_ = "",
                          const std::string& options_ = "",
                          const std::string& subId_ = "")
    {
      return _body.get().subscribe(messageHandler_, topic_, timeout_,
                                   filter_, "", options_, subId_);
    }

    /// Subscribe to a topic.
    /// Initiates a subscription to a topic on the server, returning a MessageStream to iterate over.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param options_ Optional options to supply with this subscription.
    /// \param subId_ Optional subscription id to use with this subscription.
    /// \returns A MessageStream to iterate over.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    MessageStream subscribe(const std::string& topic_,
                            long timeout_ = 0, const std::string& filter_ = "",
                            const std::string& options_ = "",
                            const std::string& subId_ = "")
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().subscribe(
                               result.operator MessageHandler(),
                               topic_, timeout_, filter_, "",
                               options_, subId_, false));
      return result;
    }

    /// Subscribe to a topic.
    /// Initiates a subscription to a topic on the server, returning a MessageStream to iterate over.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param options_ Optional options to supply with this subscription.
    /// \param subId_ Optional subscription id to use with this subscription.
    /// \returns A MessageStream to iterate over.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    MessageStream subscribe(const char* topic_,
                            long timeout_ = 0, const std::string& filter_ = "",
                            const std::string& options_ = "",
                            const std::string& subId_ = "")
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().subscribe(
                               result.operator MessageHandler(),
                               topic_, timeout_, filter_, "",
                               options_, subId_, false));
      return result;
    }

    /// Delta Subscribe to a topic.
    /// Initiates a delta subscription to a topic on the server, using the provided
    /// messageHandler_ to receive messages for the subscription. The
    /// messageHandler_ is called on the client receive thread: see the
    /// Developer Guide for details on the threading considerations.
    /// \param messageHandler_ The function to be invoked when messages for this topic arrive.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param options_ Optional options to supply with this subscription.
    /// \param subId_ Optional subscription id to use with this subscription.
    /// \returns A std::string with a unique identifier for this subscription.
    std::string deltaSubscribe(const MessageHandler& messageHandler_,
                               const std::string& topic_,
                               long timeout_,
                               const std::string& filter_ = "",
                               const std::string& options_ = "",
                               const std::string& subId_ = "")
    {
      return _body.get().deltaSubscribe(messageHandler_, topic_, timeout_,
                                        filter_, "", options_, subId_);
    }
    /// Delta Subscribe to a topic.
    /// Initiates a delta subscription to a topic on the server, returning a MessageStream.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param options_ Optional options to supply with this subscription.
    /// \param subId_ Optional subscription id to use with this subscription.
    /// \returns A MessageStream to iterate over.
    MessageStream deltaSubscribe(const std::string& topic_,
                                 long timeout_, const std::string& filter_ = "",
                                 const std::string& options_ = "",
                                 const std::string& subId_ = "")
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().deltaSubscribe(
                               result.operator MessageHandler(),
                               topic_, timeout_, filter_, "",
                               options_, subId_, false));
      return result;
    }

    /// \copydoc deltaSubscribe(const std::string&,long,const std::string&,const std::string&,const std::string&)
    MessageStream deltaSubscribe(const char* topic_,
                                 long timeout_, const std::string& filter_ = "",
                                 const std::string& options_ = "",
                                 const std::string& subId_ = "")
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().deltaSubscribe(
                               result.operator MessageHandler(),
                               topic_, timeout_, filter_, "",
                               options_, subId_, false));
      return result;
    }

    /// Subscribe to a topic using a bookmark.
    /// Initiates a bookmark subscription (a transaction log replay)
    /// to a topic on the server. using the provided
    /// messageHandler_ to receive messages for the subscription. The
    /// messageHandler_ is called on the client receive thread: see the
    /// Developer Guide for details on the threading considerations.
    /// \param messageHandler_ The function to be invoked when messages for this topic arrive.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \param bookmark_ Bookmark to supply with this subscription. This can be a bookmark within the journal file, or one of the \ref specialBookmarks.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param options_ Optional options to supply with this subscription.
    /// \param subId_ Optional subscription Id to use for this subscription. When
    ///  used with the 'replace' option, this is the subscription to be replaced.
    ///  With a bookmark store, this is the subscription Id used for recovery.
    ///  so, when using a persistent bookmark store, provide an explicit
    ///  subscription Id that is consistent across application restarts.
    /// \returns A std::string with a unique identifier for this subscription.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    std::string bookmarkSubscribe(const MessageHandler& messageHandler_,
                                  const std::string& topic_,
                                  long timeout_,
                                  const std::string& bookmark_,
                                  const std::string& filter_ = "",
                                  const std::string& options_ = "",
                                  const std::string& subId_ = "")
    {
      return _body.get().subscribe(messageHandler_, topic_, timeout_,
                                   filter_, bookmark_, options_, subId_);
    }
    /// Subscribe to a topic using a bookmark.
    /// Initiates a bookmark subscription (transaction log replay)
    /// to a topic on the server, returning a MessageStream to iterate over.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \param bookmark_ Bookmark to supply with this subscription. This can be a bookmark within the journal file, or one of the \ref specialBookmarks.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param options_ Optional options to supply with this subscription.
    /// \param subId_ Optional subscription Id to use for this subscription.
    /// \returns A MessageStream to iterate over.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    MessageStream bookmarkSubscribe(const std::string& topic_,
                                    long timeout_,
                                    const std::string& bookmark_,
                                    const std::string& filter_ = "",
                                    const std::string& options_ = "",
                                    const std::string& subId_ = "")
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().subscribe(
                               result.operator MessageHandler(),
                               topic_, timeout_, filter_,
                               bookmark_, options_,
                               subId_, false));
      return result;
    }

    /// \copydoc bookmarkSubscribe(const std::string&,long,const std::string&,const std::string&,const std::string&,const std::string&)
    MessageStream bookmarkSubscribe(const char* topic_,
                                    long timeout_,
                                    const std::string& bookmark_,
                                    const std::string& filter_ = "",
                                    const std::string& options_ = "",
                                    const std::string& subId_ = "")
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().subscribe(
                               result.operator MessageHandler(),
                               topic_, timeout_, filter_,
                               bookmark_, options_,
                               subId_, false));
      return result;
    }

    /// Unsubscribe from a topic.
    /// Stops a subscription already placed on this Client.
    /// \param commandId The unique identifier for this subscription.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    void unsubscribe(const std::string& commandId)
    {
      return _body.get().unsubscribe(commandId);
    }

    /// Unsubscribe from all topics.
    /// Stops all active subscriptions on this client.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    void unsubscribe()
    {
      return _body.get().unsubscribe();
    }


    /// \brief Query a State-of-the-World topic.
    /// Queries a SOW topic on the AMPS server. When you call this function,
    /// AMPS returns the command ID assigned to this command
    /// immediately. The query runs on a background thread, and the provided
    /// message handler is called on that thread. See the Developer Guide
    /// for considerations on the threading model.
    ///
    /// \param messageHandler_ The function to be invoked when messages for this topic arrive.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param orderBy_ Optional orderBy to have records ordered by server.
    /// \param bookmark_ Optional bookmark for historical query. Bookmarks can
    ///  be one of the \ref specialBookmarks, the bookmark assigned to a
    ///  specific message, or a timestamp of the format YYYYmmddTHHMMSS.
    /// \param batchSize_ The number of records the server should pack into each message: typically applications use the DEFAULT_BATCH_SIZE unless the records are much smaller than the MTU for the network or are very large
    /// \param topN_ The maximum number of records the server will return (default is all that match).
    /// \param options_ An 'or'ing of options for the command.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \returns A std::string with a unique identifier for this subscription.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    /// \ingroup async
    std::string sow(const MessageHandler& messageHandler_,
                    const std::string& topic_,
                    const std::string& filter_ = "",
                    const std::string& orderBy_ = "",
                    const std::string& bookmark_ = "",
                    int batchSize_ = DEFAULT_BATCH_SIZE,
                    int topN_ = DEFAULT_TOP_N,
                    const std::string& options_ = "",
                    long timeout_ = DEFAULT_COMMAND_TIMEOUT)
    {
      return _body.get().sow(messageHandler_, topic_, filter_, orderBy_,
                             bookmark_, batchSize_, topN_, options_,
                             timeout_);
    }
    /// Query the SOW cache of a topic.
    /// Queries the SOW cache of a topic on the AMPS server, returning a MessageStream to iterate over.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param orderBy_ Optional orderBy to have records ordered by server.
    /// \param bookmark_ Optional bookmark for historical query. Bookmarks can
    ///  be one of the \ref specialBookmarks, the bookmark assigned to a
    ///  specific message, or a timestamp of the format YYYYmmddTHHMMSS, as
    ///  described in the AMPS User's Guide.
    /// \param batchSize_ The number of records the server should pack into each message: typically applications use DEFAULT_BATCH_SIZE unless the messages are much smaller than the MTU for the network or are very large
    /// \param topN_ The maximum number of records the server will return (default is all that match).
    /// \param options_ An 'or'ing of options for the command.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \returns A MessageStream to iterate over.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    /// \ingroup sync
    MessageStream sow(const std::string& topic_,
                      const std::string& filter_ = "",
                      const std::string& orderBy_ = "",
                      const std::string& bookmark_ = "",
                      int batchSize_ = DEFAULT_BATCH_SIZE,
                      int topN_ = DEFAULT_TOP_N,
                      const std::string& options_ = "",
                      long timeout_ = DEFAULT_COMMAND_TIMEOUT)
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSOWOnly(_body.get().sow(result.operator MessageHandler(),
                                        topic_, filter_, orderBy_, bookmark_,
                                        batchSize_, topN_, options_, timeout_));
      return result;
    }

    /// \copydoc sow(const std::string&,const std::string&,const std::string&,const std::string&,int,int,const std::string&,long)
    MessageStream sow(const char* topic_,
                      const std::string& filter_ = "",
                      const std::string& orderBy_ = "",
                      const std::string& bookmark_ = "",
                      int batchSize_ = DEFAULT_BATCH_SIZE,
                      int topN_ = DEFAULT_TOP_N,
                      const std::string& options_ = "",
                      long timeout_ = DEFAULT_COMMAND_TIMEOUT)
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSOWOnly(_body.get().sow(result.operator MessageHandler(),
                                        topic_, filter_, orderBy_, bookmark_,
                                        batchSize_, topN_, options_, timeout_));
      return result;
    }
    /// Query the SOW cache of a topic.
    /// Queries the SOW cache of a topic on the AMPS server. The query runs
    /// on a background thread, and the provided messageHandler_ is invoked on
    /// that thread. See the Developer Guide for details on threading
    /// considerations.
    /// \param messageHandler_ The function to be invoked when messages for this topic arrive.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param batchSize_ The number of records the server should pack into each message.
    /// \param topN_ The maximum number of records the server will return (default is all that match).
    /// \returns A std::string with a unique identifier for this subscription.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    /// \ingroup async
    std::string sow(const MessageHandler& messageHandler_,
                    const std::string& topic_,
                    long timeout_,
                    const std::string& filter_ = "",
                    int batchSize_ = DEFAULT_BATCH_SIZE,
                    int topN_ = DEFAULT_TOP_N)
    {
      return _body.get().sow(messageHandler_, topic_, timeout_, filter_,
                             batchSize_, topN_);
    }
    /// Query the SOW cache of a topic and initiates a new subscription on it.
    /// Queries the SOW cache of a topic on the AMPS server, and simultaneously begins a new subscription.
    /// The messageHandler_ provided is invoked on the client receive thread.
    /// See the Developer Guide for details on the threading considerations.
    /// \param messageHandler_ The function to be invoked when messages for this topic arrive.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param batchSize_ The number of records the server should pack into each message.
    /// \param oofEnabled_ <c>true</c> indicates Out-Of-Focus messages are desired for this topic.
    /// \param topN_ The maximum number of records the server will return (default is all that match).
    /// \returns A std::string with a unique identifier for this subscription.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    /// \ingroup async
    std::string sowAndSubscribe(const MessageHandler& messageHandler_,
                                const std::string& topic_,
                                long timeout_,
                                const std::string& filter_ = "",
                                int batchSize_ = DEFAULT_BATCH_SIZE,
                                bool oofEnabled_ = false,
                                int topN_ = DEFAULT_TOP_N)
    {
      return _body.get().sowAndSubscribe(messageHandler_, topic_, timeout_,
                                         filter_, batchSize_, oofEnabled_,
                                         topN_);
    }

    /// Query the SOW cache of a topic and initiates a new subscription on it.
    /// Queries the SOW cache of a topic on the AMPS server, and simultaneously begins a new subscription.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param batchSize_ The number of records the server should pack into each message.
    /// \param oofEnabled_ <c>true</c> indicates Out-Of-Focus messages are desired for this topic.
    /// \param topN_ The maximum number of records the server will return (default is all that match).
    /// \returns A MessageStream to iterate over.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    /// \ingroup sync
    MessageStream sowAndSubscribe(const std::string& topic_,
                                  long timeout_,
                                  const std::string& filter_ = "",
                                  int batchSize_ = DEFAULT_BATCH_SIZE,
                                  bool oofEnabled_ = false,
                                  int topN_ = DEFAULT_TOP_N)
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().sowAndSubscribe(
                               result.operator MessageHandler(),
                               topic_, timeout_, filter_,
                               batchSize_, oofEnabled_,
                               topN_, false));
      return result;
    }
    /// Query the SOW cache of a topic and initiates a new subscription on it.
    /// Queries the SOW cache of a topic on the AMPS server, and simultaneously begins a new subscription.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param batchSize_ The number of records the server should pack into each message.
    /// \param oofEnabled_ <c>true</c> indicates Out-Of-Focus messages are desired for this topic.
    /// \param topN_ The maximum number of records the server will return (default is all that match).
    /// \returns A MessageStream to iterate over.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    /// \ingroup sync
    MessageStream sowAndSubscribe(const char* topic_,
                                  long timeout_,
                                  const std::string& filter_ = "",
                                  int batchSize_ = DEFAULT_BATCH_SIZE,
                                  bool oofEnabled_ = false,
                                  int topN_ = DEFAULT_TOP_N)
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().sowAndSubscribe(
                               result.operator MessageHandler(),
                               topic_, timeout_, filter_,
                               batchSize_, oofEnabled_,
                               topN_, false));
      return result;
    }


    /// Query the SOW cache of a topic and initiates a new subscription on it.
    /// Queries the SOW cache of a topic on the AMPS server, and simultaneously begins a new subscription.
    /// The messageHandler_ provided is invoked on the client receive thread.
    /// See the Developer Guide for details on the threading considerations.
    /// \param messageHandler_ The function to be invoked when messages for this topic arrive.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param orderBy_ Optional orderBy to have records ordered by server.
    /// \param bookmark_ Optional bookmark for historical query. Bookmarks can
    ///  be one of the \ref specialBookmarks, the bookmark assigned to a
    ///  specific message, or a timestamp of the format YYYYmmddTHHMMSS, as
    ///  described in the AMPS User's Guide.
    /// \param batchSize_ The number of records the server should pack into each message.
    /// \param topN_ The maximum number of records the server will return (default is all that match).
    /// \param options_ An 'or'ing of options for the command.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \returns A std::string with a unique identifier for this subscription.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    /// \ingroup async
    std::string sowAndSubscribe(const MessageHandler& messageHandler_,
                                const std::string& topic_,
                                const std::string& filter_ = "",
                                const std::string& orderBy_ = "",
                                const std::string& bookmark_ = "",
                                int batchSize_ = DEFAULT_BATCH_SIZE,
                                int topN_ = DEFAULT_TOP_N,
                                const std::string& options_ = "",
                                long timeout_ = DEFAULT_COMMAND_TIMEOUT)
    {
      return _body.get().sowAndSubscribe(messageHandler_, topic_, filter_,
                                         orderBy_, bookmark_, batchSize_,
                                         topN_, options_, timeout_);
    }

    /// Query the SOW cache of a topic and initiates a new subscription on it.
    /// Queries the SOW cache of a topic on the AMPS server, and simultaneously begins a new subscription, returning a MessageStream to iterate over.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param orderBy_ Optional orderBy to have records ordered by server.
    /// \param bookmark_ Optional bookmark for historical query. Bookmarks can
    ///  be one of the \ref specialBookmarks, the bookmark assigned to a
    ///  specific message, or a timestamp of the format YYYYmmddTHHMMSS, as
    ///  described in the AMPS User's Guide.
    /// \param batchSize_ The number of records the server should pack into each message.
    /// \param topN_ The maximum number of records the server will return (default is all that match).
    /// \param options_ An 'or'ing of options for the command.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \returns A MessageStream to iterate over.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    /// \ingroup sync
    MessageStream sowAndSubscribe(const std::string& topic_,
                                  const std::string& filter_ = "",
                                  const std::string& orderBy_ = "",
                                  const std::string& bookmark_ = "",
                                  int batchSize_ = DEFAULT_BATCH_SIZE,
                                  int topN_ = DEFAULT_TOP_N,
                                  const std::string& options_ = "",
                                  long timeout_ = DEFAULT_COMMAND_TIMEOUT)
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().sowAndSubscribe(
                               result.operator MessageHandler(),
                               topic_, filter_, orderBy_,
                               bookmark_, batchSize_, topN_,
                               options_, timeout_, false));
      return result;
    }

    /// \copydoc sowAndSubscribe(const std::string&, const std::string&,const std::string&,const std::string&,int,int,const std::string&,long)
    MessageStream sowAndSubscribe(const char* topic_,
                                  const std::string& filter_ = "",
                                  const std::string& orderBy_ = "",
                                  const std::string& bookmark_ = "",
                                  int batchSize_ = DEFAULT_BATCH_SIZE,
                                  int topN_ = DEFAULT_TOP_N,
                                  const std::string& options_ = "",
                                  long timeout_ = DEFAULT_COMMAND_TIMEOUT)
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().sowAndSubscribe(
                               result.operator MessageHandler(),
                               topic_, filter_, orderBy_,
                               bookmark_, batchSize_, topN_,
                               options_, timeout_, false));
      return result;
    }

    /// Query the SOW cache of a topic and initiates a new delta subscription on it.
    /// Queries the SOW cache of a topic on the AMPS server,
    /// and simultaneously begins a new delta subscription on it. The
    /// messageHandler_ is invoked on the client receive thread. See the
    /// Developer Guide for details on the threading considerations.
    /// \param messageHandler_ The function to be invoked when messages for this topic arrive.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param orderBy_ Optional orderBy to have records ordered by server.
    /// \param batchSize_ The number of records the server should pack into each message.
    /// \param topN_ The maximum number of records the server will return (default is all that match).
    /// \param options_ An 'or'ing of options for the command.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \returns A std::string with a unique identifier for this subscription.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    /// \ingroup async
    std::string sowAndDeltaSubscribe(const MessageHandler& messageHandler_,
                                     const std::string& topic_,
                                     const std::string& filter_ = "",
                                     const std::string& orderBy_ = "",
                                     int batchSize_ = DEFAULT_BATCH_SIZE,
                                     int topN_ = DEFAULT_TOP_N,
                                     const std::string& options_ = "",
                                     long timeout_ = DEFAULT_COMMAND_TIMEOUT)
    {
      return _body.get().sowAndDeltaSubscribe(messageHandler_, topic_,
                                              filter_, orderBy_, batchSize_,
                                              topN_, options_, timeout_);
    }
    /// Query the SOW cache of a topic and initiates a new delta subscription on it.
    /// Queries the SOW cache of a topic on the AMPS server, and simultaneously begins a new delta subscription on it. Returns a MessageStream to iterate over.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param orderBy_ Optional orderBy to have records ordered by server.
    /// \param batchSize_ The number of records the server should pack into each message.
    /// \param topN_ The maximum number of records the server will return (default is all that match).
    /// \param options_ An 'or'ing of options for the command.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \returns A MessageStream to iterate over.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    /// \ingroup sync
    MessageStream sowAndDeltaSubscribe(const std::string& topic_,
                                       const std::string& filter_ = "",
                                       const std::string& orderBy_ = "",
                                       int batchSize_ = DEFAULT_BATCH_SIZE,
                                       int topN_ = DEFAULT_TOP_N,
                                       const std::string& options_ = "",
                                       long timeout_ = DEFAULT_COMMAND_TIMEOUT)
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().sowAndDeltaSubscribe(
                               result.operator MessageHandler(),
                               topic_, filter_, orderBy_,
                               batchSize_, topN_, options_,
                               timeout_, false));
      return result;
    }

    /// \copydoc sowAndDeltaSubscribe(const std::string&, const std::string&, const std::string&, int,int, const std::string&, long)
    MessageStream sowAndDeltaSubscribe(const char* topic_,
                                       const std::string& filter_ = "",
                                       const std::string& orderBy_ = "",
                                       int batchSize_ = DEFAULT_BATCH_SIZE,
                                       int topN_ = DEFAULT_TOP_N,
                                       const std::string& options_ = "",
                                       long timeout_ = DEFAULT_COMMAND_TIMEOUT)
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().sowAndDeltaSubscribe(
                               result.operator MessageHandler(),
                               topic_, filter_, orderBy_,
                               batchSize_, topN_, options_,
                               timeout_, false));
      return result;
    }

    /// Query the SOW cache of a topic and initiates a new delta subscription on it.
    /// Queries the SOW cache of a topic on the AMPS server,
    /// and simultaneously begins a new delta subscription on it.
    /// The messageHandler_ is invoked on the client receive thread. See the
    /// Developer Guide for threading considerations.
    /// \param messageHandler_ The function to be invoked when messages for this topic arrive.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param batchSize_ The number of records the server should pack into each message.
    /// \param oofEnabled_ <c>true</c> indicates Out-Of-Focus messages are desired for this topic.
    /// \param sendEmpties_ <c>true</c> indicates empty messages should be sent by the server.
    /// \param topN_ The maximum number of records the server will return (default is all that match).
    /// \returns A std::string with a unique identifier for this subscription.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    /// \ingroup async
    std::string sowAndDeltaSubscribe(const MessageHandler& messageHandler_,
                                     const std::string& topic_,
                                     long timeout_,
                                     const std::string& filter_ = "",
                                     int batchSize_ = DEFAULT_BATCH_SIZE,
                                     bool oofEnabled_ = false,
                                     bool sendEmpties_ = false,
                                     int topN_ = DEFAULT_TOP_N)
    {
      return _body.get().sowAndDeltaSubscribe(messageHandler_, topic_,
                                              timeout_, filter_, batchSize_,
                                              oofEnabled_, sendEmpties_,
                                              topN_);
    }

    /// Query the SOW cache of a topic and initiates a new delta subscription on it.
    /// Queries the SOW cache of a topic on the AMPS server,
    /// and simultaneously begins a new delta subscription on it.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param batchSize_ The number of records the server should pack into each message.
    /// \param oofEnabled_ <c>true</c> indicates Out-Of-Focus messages are desired for this topic.
    /// \param sendEmpties_ <c>true</c> indicates empty messages should be sent by the server.
    /// \param topN_ The maximum number of records the server will return (default is all that match).
    /// \returns A MessageStream to iterate over.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    /// \ingroup sync
    MessageStream sowAndDeltaSubscribe(const std::string& topic_,
                                       long timeout_,
                                       const std::string& filter_ = "",
                                       int batchSize_ = DEFAULT_BATCH_SIZE,
                                       bool oofEnabled_ = false,
                                       bool sendEmpties_ = false,
                                       int topN_ = DEFAULT_TOP_N)
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().sowAndDeltaSubscribe(
                               result.operator MessageHandler(),
                               topic_, timeout_, filter_,
                               batchSize_, oofEnabled_,
                               sendEmpties_, topN_, false));
      return result;
    }
    /// Query the SOW cache of a topic and initiates a new delta subscription on it.
    /// Queries the SOW cache of a topic on the AMPS server,
    /// and simultaneously begins a new delta subscription on it.
    /// \param topic_ The topic, or regular expression topic, to subscribe to.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume an acknowledgment for this command. 0 indicates no timeout.
    /// \param filter_ Optional filter expression for this subscription.
    /// \param batchSize_ The number of records the server should pack into each message.
    /// \param oofEnabled_ <c>true</c> indicates Out-Of-Focus messages are desired for this topic.
    /// \param sendEmpties_ <c>true</c> indicates empty messages should be sent by the server.
    /// \param topN_ The maximum number of records the server will return (default is all that match).
    /// \returns A MessageStream to iterate over.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \throw SubscriptionAlreadyExistsException This ID already exists as a subscription.
    /// \ingroup sync
    MessageStream sowAndDeltaSubscribe(const char* topic_,
                                       long timeout_,
                                       const std::string& filter_ = "",
                                       int batchSize_ = DEFAULT_BATCH_SIZE,
                                       bool oofEnabled_ = false,
                                       bool sendEmpties_ = false,
                                       int topN_ = DEFAULT_TOP_N)
    {
      MessageStream result(*this);
      if (_body.get().getDefaultMaxDepth())
      {
        result.maxDepth(_body.get().getDefaultMaxDepth());
      }
      result.setSubscription(_body.get().sowAndDeltaSubscribe(
                               result.operator MessageHandler(),
                               topic_, timeout_, filter_,
                               batchSize_, oofEnabled_,
                               sendEmpties_, topN_, false));
      return result;
    }
    /// Deletes one or more messages from a topic's SOW cache.
    /// Sends a request to the server to delete one or more messages in the SOW cache.
    /// The messageHandler is called on the client receive thread. See
    /// the Developer Guide for threading considerations.
    /// \param messageHandler The function to be invoked when messages for this topic arrive.
    /// \param topic The topic, or regular expression topic, to delete from.
    /// \param filter Optional filter expression for this subscription. To delete all records,
    ///  set a filter that is always true ('1=1').
    /// \param timeout The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \returns A std::string with a unique identifier for this request.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \ingroup async
    std::string sowDelete(const MessageHandler& messageHandler,
                          const std::string& topic,
                          const std::string& filter,
                          long timeout)
    {
      return _body.get().sowDelete(messageHandler, topic, filter, timeout);
    }
    /// Deletes one or more messages from a topic's SOW cache.
    /// Sends a request to the server to delete one or more messages in the SOW cache, returning once the operation is complete.
    /// \param topic The topic, or regular expression topic, to delete from.
    /// \param filter Optional filter expression for this subscription. To delete all records,
    ///  set a filter that is always true ('1=1').
    /// \param timeout The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \returns A Message containing information about the completed delete operation.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadFilterException The specified filter is not valid or has a syntax error.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \ingroup sync
    Message sowDelete(const std::string& topic, const std::string& filter,
                      long timeout = 0)
    {
      MessageStream stream(*this);
      char buf[Message::IdentifierLength + 1];
      buf[Message::IdentifierLength] = 0;
      AMPS_snprintf(buf, Message::IdentifierLength + 1, "%lx", MessageImpl::newId());
      Field cid(buf);
      try
      {
        stream.setStatsOnly(cid);
        _body.get().sowDelete(stream.operator MessageHandler(), topic, filter, timeout, cid);
        return *(stream.begin());
      }
      catch (const DisconnectedException&)
      {
        removeMessageHandler(cid);
        throw;
      }
    }

    /// \deprecated As of AMPS server 5.3.2.0 start_timer is not valid.
    /// Sends a message to AMPS requesting that AMPS start a server-side
    /// timer. The application can later call stopTimer() to stop the
    /// timer and request a message with the results.
    void startTimer()
    {
      _body.get().startTimer();
    }

    /// \deprecated As of AMPS server 5.3.2.0 stop_timer is not valid.
    /// Sends a message to AMPS requesting that AMPS stop the previously
    /// started timer. The provided MessageHandler will receive an
    /// acknowledgement with information about the timer.
    /// \param messageHandler The function to be invoked when the acknowledgement message arrives
    ///  \sa startTimer()
    std::string stopTimer(const MessageHandler& messageHandler)
    {
      return _body.get().stopTimer(messageHandler);
    }

    /// Deletes messages that match SOW keys from a topic's SOW cache.
    /// Sends a request to the server to delete messages that match SOW keys from a topic's SOW cache.
    /// The SOW keys provided are a comma-delimited list. When the topic is configured
    /// such that AMPS generates the SOW key based on the message contents,
    /// these are the key values that AMPS generates (as returned in the SowKey
    /// field of the message header). When the topic is configured such that
    /// a publisher must provide the SOW key, the key that the publisher provided
    /// can be used to delete a message.
    /// \param messageHandler_ The function to be invoked when messages for this topic arrive.
    /// \param topic_ The topic, or regular expression topic, to delete from.
    /// \param keys_ The sow keys to delete as a comma-separated list
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \returns A std::string with a unique identifier for this request.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \ingroup async
    std::string sowDeleteByKeys(const MessageHandler& messageHandler_,
                                const std::string& topic_,
                                const std::string& keys_,
                                long timeout_ = 0)
    {
      return _body.get().sowDeleteByKeys(messageHandler_, topic_, keys_, timeout_);
    }
    /// Deletes messages that match SOW keys from a topic's SOW cache.
    /// Sends a request to the server to delete messages that match SOW keys from a topic's SOW cache, returning once the operation is complete.
    /// The SOW keys provided are a comma-delimited list. When the topic is configured
    /// such that AMPS generates the SOW key based on the message contents,
    /// these are the key values that AMPS generates (as returned in the SowKey
    /// field of the message header). When the topic is configured such that
    /// a publisher must provide the SOW key, the key that the publisher provided
    /// can be used to delete a message.
    /// \param topic_ The topic, or regular expression topic, to delete from.
    /// \param keys_ The sow keys to delete as a comma-separated list
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \returns A Message containing information about the completed operation.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \ingroup sync
    Message sowDeleteByKeys(const std::string& topic_, const std::string& keys_,
                            long timeout_ = 0)
    {
      MessageStream stream(*this);
      char buf[Message::IdentifierLength + 1];
      buf[Message::IdentifierLength] = 0;
      AMPS_snprintf(buf, Message::IdentifierLength + 1, "%lx", MessageImpl::newId());
      Field cid(buf);
      try
      {
        stream.setStatsOnly(cid);
        _body.get().sowDeleteByKeys(stream.operator MessageHandler(), topic_, keys_, timeout_, cid);
        return *(stream.begin());
      }
      catch (const DisconnectedException&)
      {
        removeMessageHandler(cid);
        throw;
      }
    }

    /// Deletes the message whose keys match the message data provided.
    /// \param messageHandler_ The function to be invoked when statistics about the delete arrive.
    /// \param topic_ The topic, or regular expression topic, to delete from.
    /// \param data_ The message whose keys are matched against the server's SOW cache.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \returns A std::string with a unique identifier for this request.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \ingroup async
    std::string sowDeleteByData(const MessageHandler& messageHandler_,
                                const std::string& topic_, const std::string& data_,
                                long timeout_ = 0)
    {
      return _body.get().sowDeleteByData(messageHandler_, topic_, data_, timeout_);
    }

    /// Deletes the message whose keys match the message data provided.
    /// Sends a request to the server to delete the message whose keys match the data provided. This method returns once the operation is complete.
    /// \param topic_ The topic, or regular expression topic, to delete from.
    /// \param data_ The message whose keys are matched against the server's SOW cache.
    /// \param timeout_ The time to wait, in milliseconds, for the receive thread to consume a processed ack for this command. 0 indicates no timeout.
    /// \returns A Message containing information about the completed operation.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException A timeout occurred while waiting for a response from the server.
    /// \throw AuthenticationException An authentication failure was received from the server.
    /// \throw UnknownException An unknown failure was received from the server.
    /// \throw BadRegexTopicException The specified topic contains an invalid regex.
    /// \throw InvalidTopicException The specified topic may not be used with this command.
    /// \ingroup async
    Message sowDeleteByData(const std::string& topic_, const std::string& data_,
                            long timeout_ = 0)
    {
      MessageStream stream(*this);
      char buf[Message::IdentifierLength + 1];
      buf[Message::IdentifierLength] = 0;
      AMPS_snprintf(buf, Message::IdentifierLength + 1, "%lx", MessageImpl::newId());
      Field cid(buf);
      try
      {
        stream.setStatsOnly(cid);
        _body.get().sowDeleteByData(stream.operator MessageHandler(), topic_, data_, timeout_, cid);
        return *(stream.begin());
      }
      catch (const DisconnectedException&)
      {
        removeMessageHandler(cid);
        throw;
      }
    }

    /// Returns the underlying amps_handle for this client, to be used
    /// with amps_client_* functions from the C api.
    /// \returns The amps_handle associated with self.
    amps_handle getHandle()
    {
      return _body.get().getHandle();
    }

    /// Sets the exception listener for exceptions that are not thrown
    /// back to the user (for example, exceptions that are thrown from
    /// a MessageHandler running on the client receive thread.)
    /// 60East strongly recommends setting this callback if an
    /// application uses asynchronous message processing (that is,
    /// provides message handler callbacks).
    /// \param pListener_ A shared pointer to the ExceptionListener that
    /// should be notified.
    void setExceptionListener(const std::shared_ptr<const ExceptionListener>& pListener_)
    {
      _body.get().setExceptionListener(pListener_);
    }

    /// \deprecated Use setExceptionListener(std::shared_ptr<const ExceptionListener>&)
    /// Sets the exception listener for exceptions that are not thrown
    /// back to the user (for example, exceptions that are thrown from
    /// a MessageHandler running on the client receive thread.)
    /// 60East strongly recommends setting this callback if an
    /// application uses asynchronous message processing (that is,
    /// provides message handler callbacks).
    /// \param listener_ The exception listener to be used
    void setExceptionListener(const ExceptionListener& listener_)
    {
      _body.get().setExceptionListener(listener_);
    }

    /// Returns the exception listener set on this Client.
    /// \returns The ExceptionListener associated with self.
    const ExceptionListener& getExceptionListener(void) const
    {
      return _body.get().getExceptionListener();
    }

    /// Requests heartbeating with the AMPS server.
    ///
    /// The client will provide a heartbeat to the server on the specified
    /// interval, and requests that the server provide a heartbeat to the client
    /// the specified interval.
    ///
    /// If the client does not receive a message (either a heartbeat or another
    //  type of message) from the server for the specified interval (plus a grace period),
    /// the client will consider the connection to have failed and close the connection.
    /// Likewise, if the server does not receive a message from the client
    /// for the specified interval (plus a grace  period), the server will consider the
    /// connection or client to have failed and close the connection.
    /// Notice that operations that block the client receive thread (that is,
    /// blocking in a Handler or Listener) will prevent the client from sending
    /// or responding to heartbeats while the thread is blocked. Any invocation of
    /// a Handler or Listener that takes longer than the heartbeat interval
    /// set may cause the server to consider the client to have failed, and disconnect
    /// the client.
    ///
    /// \param heartbeatTime_ The time (seconds) to request the server heartbeat.
    /// \param readTimeout_ The time (seconds) to block waiting for a read from the socket before timing out the read and checking the heartbeat interval.
    void setHeartbeat(unsigned heartbeatTime_, unsigned readTimeout_)
    {
      _body.get().setHeartbeat(heartbeatTime_, readTimeout_);
    }

    /// Requests heartbeating with the AMPS server.
    ///
    /// The client will provide a heartbeat to the server on the specified
    /// interval, and requests that the server provide a heartbeat to the client
    /// the specified interval.
    ///
    /// If the client does not receive a message (either a heartbeat or another
    //  type of message) from the server for the specified interval (plus a grace period),
    /// the client will consider the connection to have failed and close the connection.
    /// Likewise, if the server does not receive a message from the client
    /// for the specified interval (plus a grace  period), the server will consider the
    /// connection or client to have failed and close the connection.
    /// Notice that operations that block the client receive thread (that is,
    /// blocking in a Handler or Listener) will prevent the client from sending
    /// or responding to heartbeats while the thread is blocked. Any invocation of
    /// a Handler or Listener that takes longer than the heartbeat interval
    /// set may cause the server to consider the client to have failed, and disconnect
    /// the client.
    /// \param heartbeatTime_ The time (seconds) to request the server heartbeat.
    void setHeartbeat(unsigned heartbeatTime_)
    {
      _body.get().setHeartbeat(heartbeatTime_, 2 * heartbeatTime_);
    }

    /// \deprecated. Use setLastChanceMessageHandler
    void setUnhandledMessageHandler(const AMPS::MessageHandler& messageHandler)
    {
      setLastChanceMessageHandler(messageHandler);
    }

    /// Sets the message handler called when no other handler matches.
    /// \param messageHandler The message handler to invoke when no other handler matches.
    void setLastChanceMessageHandler(const AMPS::MessageHandler& messageHandler)
    {
      _body.get().setGlobalCommandTypeMessageHandler(ClientImpl::GlobalCommandTypeHandlers::LastChance,
                                                     messageHandler);
    }

    /// Sets a handler for all messages of a particular type, or for messages
    /// that would be delivered to a particular special handler: currently
    /// supported types are "heartbeat", "ack", "duplicate", and "lastchance".
    ///
    /// When a handler for a specific message type is set, the AMPS client calls
    /// that handler for all messages of that type in addition to any other
    /// handlers called. Use this function in cases where an application needs
    /// to precisely monitor messages that would otherwise be handled by the
    /// AMPS Client infrastructure or the last chance message handler. A global
    /// command type message handler is rarely needed, but
    /// can sometimes be helpful for test harnesses, advanced troubleshooting,
    /// or similar situations where it is important to monitor messages
    /// that are normally handled by the client.
    ///
    /// The handler_ provided is called on the client receive thread. See
    /// the Developer Guide for threading considerations.
    ///
    /// \param command_ A string specifying the type of command to handle.
    /// \param handler_ The message handler to call.
    /// \throws CommandException An unknown or unsupported message type was provided.
    void setGlobalCommandTypeMessageHandler(const std::string& command_, const MessageHandler& handler_)
    {
      _body.get().setGlobalCommandTypeMessageHandler(command_, handler_);
    }

    /// Sets a handler for all messages of a particular type: currently
    /// supported types are heartbeat messages and ack messages. When a
    /// handler for a specific message type is set, the AMPS client calls
    /// that handler for all messages of that type in addition to any other
    /// handlers called.
    ///
    /// Use this function in cases where an application needs
    /// to precisely monitor messages that would otherwise be handled by the
    /// AMPS Client infrastructure or the last chance message handler. A global
    /// command type message handler is rarely needed, but
    /// can sometimes be helpful for test harnesses, advanced troubleshooting,
    /// or similar situations where it is important to monitor messages
    /// that are normally handled by the client.
    ///
    /// The handler_ provided is called on the client receive thread. See
    /// the Developer Guide for threading considerations.
    ///
    /// \param command_ The type of command to handle.
    /// \param handler_ The message handler to call.
    /// \throws CommandException An unknown or unsupported message type was provided.
    void setGlobalCommandTypeMessageHandler(const Message::Command::Type command_, const MessageHandler& handler_)
    {
      _body.get().setGlobalCommandTypeMessageHandler(command_, handler_);
    }

    /** Convenience method for returning the special value to start a subscription at
      *  the end of the transaction log.
      *
      * /returns Always returns #AMPS_BOOKMARK_NOW
      */
    static const char* BOOKMARK_NOW()
    {
      return AMPS_BOOKMARK_NOW;
    }
    /** Convenience method for returning the special value to start a subscription at
      * the end of the transaction log.
      *
      * /returns Always returns #AMPS_BOOKMARK_NOW
      */
    static const char* NOW()
    {
      return AMPS_BOOKMARK_NOW;
    }

    /** Convenience method for returning the special value to start a subscription at
     *  the beginning of the transaction log.
     *
     *  \returns Always returns #AMPS_BOOKMARK_EPOCH
     */
    static const char* BOOKMARK_EPOCH()
    {
      return AMPS_BOOKMARK_EPOCH;
    }

    /** Convenience method for returning the special value to start a subscription at
     *  the beginning of the transaction log.
     *
     *  \returns Always returns #AMPS_BOOKMARK_EPOCH
     */
    static const char* EPOCH()
    {
      return AMPS_BOOKMARK_EPOCH;
    }

    /** Convenience method for returning the special value to start a subscription at
      * a recovery point based on the information that the configured BookmarkStore
      * (if one is set) has for the subscription.
      *
      * \returns Always returns #AMPS_BOOKMARK_RECENT
      */
    static const char* BOOKMARK_MOST_RECENT()
    {
      return AMPS_BOOKMARK_RECENT;
    }

    /** Convenience method for returning the special value to start a subscription at
      * a recovery point based on the information that the configured BookmarkStore
      * (if one is set) has for the subscription.
      *
      * \returns Always returns #AMPS_BOOKMARK_RECENT
      */
    static const char* MOST_RECENT()
    {
      return AMPS_BOOKMARK_RECENT;
    }

    /** Convenience method for returning the special value to start a subscription at
      * a recovery point based on the information that the configured BookmarkStore
      * (if one is set) has for the subscription.
      *
      * \returns Always returns #AMPS_BOOKMARK_RECENT
      */
    static const char* BOOKMARK_RECENT()
    {
      return AMPS_BOOKMARK_RECENT;
    }


    /** Adds a ConnectionStateListener to self's set of listeners. These
    *  listeners are invoked whenever the connection state of this client
    *  changes (either becomes connected or disconnected.)
    *  \param listener The ConnectionStateListener to invoke.
    */
    void addConnectionStateListener(ConnectionStateListener* listener)
    {
      _body.get().addConnectionStateListener(listener);
    }

    /** Attempts to remove listener from self's set of ConnectionStateListeners.
    *   \param listener The ConnectionStateListener to remove.
    */
    void removeConnectionStateListener(ConnectionStateListener* listener)
    {
      _body.get().removeConnectionStateListener(listener);
    }

    /** Clear all listeners from self's set of ConnectionStateListeners.
    */
    void clearConnectionStateListeners()
    {
      _body.get().clearConnectionStateListeners();
    }

    /**
    *  Execute the provided command and, once AMPS acknowledges the command,
    *  process messages in response to the command on the client receive
    *  thread by invoking the provided handler.
    *
    *  This method creates a message based on the provided {@link Command},
    *  sends the message to AMPS, and invokes the provided
    *  MessageHandler to process messages returned in response
    *  to the command. Rather than providing messages on the calling thread,
    *  the Client runs the MessageHandler directly on the client receive
    *  thread.
    *
    *  When the provided MessageHandler is valid (that is, the object contains
    *  a function to be called), this method blocks until AMPS acknowledges
    *  that the command has been processed. The results of the command after
    *  that acknowledgement are provided to the MessageHandler.
    *
    *  This method adds the subscription to any SubscriptionManager present on
    *  the Client, to allow the subscription to be re-entered after failover (the HAClient
    *  disconnect handler does this automatically).
    *
    *  @param command_ The Command object containing the command to send to AMPS
    *  @param handler_ The MessageHandler to invoke to process messages received
    *  @return the command ID for the command
    */
    std::string executeAsync(Command& command_, MessageHandler handler_)
    {
      return _body.get().executeAsync(command_, handler_);
    }

    /**
    *  Execute the provided command and, once AMPS acknowledges the command,
    *  process messages in response to the command on the client receive
    *  thread by invoking the provided handler.
    *
    *  This method creates a message based on the provided {@link Command},
    *  sends the message to AMPS, and invokes the provided
    *  MessageHandler to process messages returned in response
    *  to the command. Rather than providing messages on the calling thread,
    *  the Client runs the MessageHandler directly on the client receive
    *  thread.
    *
    *  When the provided MessageHandler is valid (that is, the object contains
    *  a function to be called), this method blocks until AMPS acknowledges
    *  that the command has been processed. The results of the command after
    *  that acknowledgement are provided to the MessageHandler.
    *
    *  When the provided Command is a form of subscribe, the subscription will
    *  *not* be placed into any SubscriptionMananger which may be in place. If a
    *  disconnect occurs, the subscription will need to be placed again.
    *
    *  While the provided Command doesn't have to be a form of subscribe, the
    *  main purpose of this function is to place a subscription that doesn't
    *  restart after a disconnect.
    *
    *  @param command_ The Command object containing the command to send to AMPS
    *  @param handler_ The MessageHandler to invoke to process messages received
    *  @return the command ID for the command
    */
    std::string executeAsyncNoResubscribe(Command& command_,
                                          MessageHandler handler_)
    {
      std::string id;
      try
      {
        if (command_.isSubscribe())
        {
          Message& message = command_.getMessage();
          Field subId = message.getSubscriptionId();
          bool useExistingHandler = !subId.empty() && !message.getOptions().empty() && message.getOptions().contains("replace", 7);
          if (useExistingHandler)
          {
            MessageHandler existingHandler;
            if (_body.get()._routes.getRoute(subId, existingHandler))
            {
              // we found an existing handler.
              _body.get().executeAsync(command_, existingHandler, false);
              return id; // empty string indicates existing
            }
          }
        }
        id = _body.get().executeAsync(command_, handler_, false);
      }
      catch (const DisconnectedException&)
      {
        removeMessageHandler(command_.getMessage().getCommandId());
        if (command_.isSubscribe())
        {
          removeMessageHandler(command_.getMessage().getSubscriptionId());
        }
        if (command_.isSow())
        {
          removeMessageHandler(command_.getMessage().getQueryID());
        }
        throw;
      }
      return id;
    }

    /**
    *  Execute the provided command and return messages received in response
    *  in a {@link MessageStream}.
    *
    *  This method creates a message based on the provided {@link Command},
    *  sends the message to AMPS, and receives the results. AMPS sends the
    *  message and receives the results on a background thread. That thread
    *  populates the MessageStream returned by this method.
    *
    *  @param command_ The Command object containing the command to send to AMPS
    *  @return A MessageStream that provides messages received in response to the command
    */
    MessageStream execute(Command& command_);

    /**
     * Acknowledge a message queue message by supplying a topic and bookmark: this adds
     * the ack to the current batch (or sends the ack immediately if the batch size is 1
     * or less).
     * @param topic_ The topic to acknowledge
     * @param bookmark_ The bookmark of the message to acknowledge
     * @param options_ The options string for the ack, such as cancel
     */
    void ack(Field& topic_, Field& bookmark_, const char* options_ = NULL)
    {
      _body.get().ack(topic_, bookmark_, options_);
    }

    /**
     * Acknowledge a message queue message by supplying the message directly: this adds the
     * ack to the current batch (or sends the ack immediately if the batch size is 1
     * or less).
     * @param message_ The message to acknowledge
     * @param options_ The options string for the ack, such as cancel
     */
    void ack(Message& message_, const char* options_ = NULL)
    {
      _body.get().ack(message_.getTopic(), message_.getBookmark(), options_);
    }
    /**
     * Acknowledge a message queue message by supplying a topic and bookmark string: this
     * adds the ack to the current batch (or sends the ack immediately if the
     * batch size is 1 or less).
     * @param topic_ The message queue topic to acknowledge
     * @param bookmark_ The bookmark to acknowledge
     * @param options_ The options string for the ack, such as cancel
     */
    void ack(const std::string& topic_, const std::string& bookmark_,
             const char* options_ = NULL)
    {
      _body.get().ack(Field(topic_.data(), topic_.length()), Field(bookmark_.data(), bookmark_.length()), options_);
    }

    /**
     * \cond NOT_DOCUMENTED
     *
     * This function is intended for 60East use only.
     */
    void ackDeferredAutoAck(Field& topic_, Field& bookmark_, const char* options_ = NULL)
    {
      _body.get()._ack(topic_, bookmark_, options_);
    }
    /**
     * \endcond
     */

    /**
     * Sends any queued message queue ack messages to the server immediately. If your
     * application uses a message queue and has set an ack batch size, it can be helpful
     * to call this method before the application exits.
     */
    void flushAcks(void)
    {
      _body.get().flushAcks();
    }

    /**
     * Returns the value of the queue auto-ack setting.
     * @return true if auto-ack is enabled, false otherwise.
     */
    bool getAutoAck(void) const
    {
      return _body.get().getAutoAck();
    }
    /**
     * Sets the queue auto-ack setting on this client. If true, message queue messages
     * are automatically acknowledged upon successful return from your message handler. If your
     * message handler throws an exception, the message is returned to the queue.
     * @param isAutoAckEnabled_ The new value of the auto-ack setting.
     */
    void setAutoAck(bool isAutoAckEnabled_)
    {
      _body.get().setAutoAck(isAutoAckEnabled_);
    }
    /**
     * Returns the value of the queue ack batch size setting.
     * @return The current ack batch size (default: 0)
     */
    unsigned getAckBatchSize(void) const
    {
      return _body.get().getAckBatchSize();
    }
    /**
     * Sets the queue ack batch size setting. When set to a value greater than 1, message queue
     * success ack messages are not sent immediately, but are batched together for increased
     * performance.
     * @param ackBatchSize_ The batch size to use.
     */
    void setAckBatchSize(const unsigned ackBatchSize_)
    {
      _body.get().setAckBatchSize(ackBatchSize_);
    }

    /**
     * Returns the current value of the message queue ack timeout setting -- that is,
     * the amount of time after which queue messages will be acknowledged even if the
     * ack batch is not full.
     * @return the current ack timeout, in milliseconds.
     */
    int getAckTimeout(void) const
    {
      return _body.get().getAckTimeout();
    }
    /**
     * Sets the message queue ack timeout value. When set, queued message queue
     * ack messages are sent when the oldest pending ack for a message queue has exceeded this
     * timeout.
     * \param ackTimeout_ The ack timeout value, in milliseconds. Throws a UsageException
     * if setting to 0 and the ack batch size > 1.
     * \throw UsageException If ackTimeout_ is 0 but the ack batch size > 1.
     */
    void setAckTimeout(const int ackTimeout_)
    {
      if (!ackTimeout_ && _body.get().getAckBatchSize() > 1)
      {
        throw UsageException("Ack timeout must be > 0 when ack batch size > 1");
      }
      _body.get().setAckTimeout(ackTimeout_);
    }


    /**
     * Enables or disables automatic retry of a command to AMPS after a
     * reconnect. This behavior is enabled by default.
     * NOTE: Clients using a publish store will have all publish messages sent,
     * regardless of this setting. Also, Clients with a subscription manager,
     * including all HAClients, will have all subscribe calls placed.
     * @param isRetryOnDisconnect_ false will disable this behavior.
     */
    void setRetryOnDisconnect(bool isRetryOnDisconnect_)
    {
      _body.get().setRetryOnDisconnect(isRetryOnDisconnect_);
    }

    /**
     * Returns true if automatic retry of a command to AMPS after a reconnect
     * is enabled.
     */
    bool getRetryOnDisconnect(void) const
    {
      return _body.get().getRetryOnDisconnect();
    }

    /**
     * Sets a default max depth on all subsequently created `MessageStream` objects.
     * \param maxDepth_ The new max depth to use.
     */
    void setDefaultMaxDepth(unsigned maxDepth_)
    {
      _body.get().setDefaultMaxDepth(maxDepth_);
    }

    /**
     * Returns the default max depth for returned `MessageStream` objects.
     * \return The max depth in use, where 0 is unlimited.
     */
    unsigned getDefaultMaxDepth(void) const
    {
      return _body.get().getDefaultMaxDepth();
    }

    /**
     * Sets a filter function on the transport that is called with all raw data
     * sent or received.
     * \param filter_ The amps_transport_filter_function to call.
     * \param userData_ Any data that should be passed to the function.
     */
    void setTransportFilterFunction(amps_transport_filter_function filter_,
                                    void* userData_)
    {
      return _body.get().setTransportFilterFunction(filter_, userData_);
    }

    /**
     * Sets a callback function on the transport that is called when a new
     * thread is created to receive data from the server. The callback should
     * only be used if setting specific thread attributes, such as the
     * thread name.
     * \param callback_ The function to call first in the new thread.
     * \param userData_ Any data that should be passed to the function.
     */
    void setThreadCreatedCallback(amps_thread_created_callback callback_,
                                  void* userData_)
    {
      return _body.get().setThreadCreatedCallback(callback_, userData_);
    }

    /**
     * \cond NOT_DOCUMENTED
     *
     * This function is intended for 60East use only.
     */
    void deferredExecution(DeferredExecutionFunc func_, void* userData_)
    {
      _body.get().deferredExecution(func_, userData_);
    }
    /**
     * \endcond
     */
  };

  inline void
  ClientImpl::lastChance(AMPS::Message& message)
  {
    AMPS_CALL_EXCEPTION_WRAPPER(_globalCommandTypeHandlers[GlobalCommandTypeHandlers::LastChance].invoke(message));
  }

  inline unsigned
  ClientImpl::persistedAck(AMPS::Message& message)
  {
    unsigned deliveries = 0;
    try
    {
      /*
      * Best Practice:  If you don't care about the dupe acks that
      * occur during failover or rapid disconnect/reconnect, then just
      * ignore them. We could discard each duplicate from the
      * persisted store, but the storage costs of doing 1 record
      * discards is heavy.  In most scenarios we'll just quickly blow
      * through the duplicates and get back to processing the
      * non-dupes.
      */
      const char* data = NULL;
      size_t len = 0;
      const char* status = NULL;
      size_t statusLen = 0;
      amps_handle messageHandle = message.getMessage();
      const size_t NotEntitled = 12, Duplicate = 9, Failure = 7;
      amps_message_get_field_value(messageHandle, AMPS_Reason, &data, &len);
      amps_message_get_field_value(messageHandle, AMPS_Status, &status, &statusLen);
      if (len == NotEntitled || len == Duplicate ||
          (statusLen == Failure && status[0] == 'f'))
      {
        if (_failedWriteHandler)
        {
          if (_publishStore.isValid())
          {
            amps_uint64_t sequence =
              amps_message_get_field_uint64(messageHandle, AMPS_Sequence);
            FailedWriteStoreReplayer replayer(this, data, len);
            AMPS_CALL_EXCEPTION_WRAPPER(_publishStore.replaySingle(
                                          replayer, sequence));
          }
          else // Call the handler with what little we have
          {
            static Message emptyMessage;
            emptyMessage.setSequence(message.getSequence());
            AMPS_CALL_EXCEPTION_WRAPPER(
              _failedWriteHandler->failedWrite(emptyMessage,
                                               data, len));
          }
          ++deliveries;
        }
      }
      if (_publishStore.isValid())
      {
        // Ack for publisher will have sequence while
        // ack for bookmark subscribe won't
        amps_uint64_t seq = amps_message_get_field_uint64(messageHandle,
                                                          AMPS_Sequence);
        if (seq > 0)
        {
          ++deliveries;
          AMPS_CALL_EXCEPTION_WRAPPER(_publishStore.discardUpTo(seq));
        }
      }

      if (!deliveries && _bookmarkStore.isValid())
      {
        amps_message_get_field_value(messageHandle, AMPS_SubscriptionId,
                                     &data, &len);
        if (len > 0)
        {
          Message::Field subId(data, len);
          const char* bookmarkData = NULL;
          size_t bookmarkLen = 0;
          amps_message_get_field_value(messageHandle,
                                       AMPS_Bookmark,
                                       &bookmarkData,
                                       &bookmarkLen);
          // Everything is there and not unsubscribed AC-912
          if (bookmarkLen > 0 && _routes.hasRoute(subId))
          {
            ++deliveries;
            _bookmarkStore.persisted(subId, Message::Field(bookmarkData, bookmarkLen));
          }
        }
      }
    }
    catch (std::exception& ex)
    {
      AMPS_UNHANDLED_EXCEPTION(ex);
    }
    return deliveries;
  }

  inline unsigned
  ClientImpl::processedAck(Message& message)
  {
    unsigned deliveries = 0;
    AckResponse ack;
    const char* data = NULL;
    size_t len = 0;
    amps_handle messageHandle = message.getMessage();
    amps_message_get_field_value(messageHandle, AMPS_CommandId, &data, &len);
    Lock<Mutex> l(_lock);
    if (data && len)
    {
      Lock<Mutex> guard(_ackMapLock);
      AckMap::iterator i = _ackMap.find(std::string(data, len));
      if (i != _ackMap.end())
      {
        ++deliveries;
        ack = i->second;
        _ackMap.erase(i);
      }
    }
    if (deliveries)
    {
      amps_message_get_field_value(messageHandle, AMPS_Status, &data, &len);
      ack.setStatus(data, len);
      amps_message_get_field_value(messageHandle, AMPS_Reason, &data, &len);
      ack.setReason(data, len);
      amps_message_get_field_value(messageHandle, AMPS_UserId, &data, &len);
      ack.setUsername(data, len);
      amps_message_get_field_value(messageHandle, AMPS_Password, &data, &len);
      ack.setPassword(data, len);
      amps_message_get_field_value(messageHandle, AMPS_Version, &data, &len);
      ack.setServerVersion(data, len);
      amps_message_get_field_value(messageHandle, AMPS_Options, &data, &len);
      ack.setOptions(data, len);
      // This sets bookmark, nameHashValue, and sequenceNo
      ack.setBookmark(message.getBookmark());
      ack.setResponded();
      _lock.signalAll();
    }
    return deliveries;
  }

  inline void
  ClientImpl::checkAndSendHeartbeat(bool force)
  {
    if (force || _heartbeatTimer.check())
    {
      _heartbeatTimer.start();
      try
      {
        sendWithoutRetry(_beatMessage);
      }
      catch (const AMPSException&)
      {
        ;
      }
    }
  }

  inline ConnectionInfo ClientImpl::getConnectionInfo() const
  {
    ConnectionInfo info;
    std::ostringstream writer;

    info["client.uri"] = _lastUri;
    info["client.name"] = _name;
    info["client.username"] = _username;
    if (_publishStore.isValid())
    {
      writer << _publishStore.unpersistedCount();
      info["publishStore.unpersistedCount"] = writer.str();
      writer.clear();
      writer.str("");
    }

    return info;
  }

  inline amps_result
  ClientImpl::ClientImplMessageHandler(amps_handle messageHandle_, void* userData_)
  {
    const unsigned SOWMask = Message::Command::SOW | Message::Command::GroupBegin | Message::Command::GroupEnd;
    const unsigned PublishMask = Message::Command::OOF | Message::Command::Publish | Message::Command::DeltaPublish;
    ClientImpl* me = (ClientImpl*) userData_;
    AMPS_CALL_EXCEPTION_WRAPPER_2(me, me->processDeferredExecutions());
    if (!messageHandle_)
    {
      if (me->_queueAckTimeout)
      {
        me->checkQueueAcks();
      }
      return AMPS_E_OK;
    }

    me->_readMessage.replace(messageHandle_);
    Message& message = me->_readMessage;
    Message::Command::Type commandType = message.getCommandEnum();
    if (commandType & SOWMask)
    {
#if 0 // Not currently implemented, to avoid an extra branch in delivery
      // A small cheat here to get the right handler, using knowledge of the
      // Command values of SOW (8), GroupBegin (8192), and GroupEnd (16384)
      // and their GlobalCommandTypeHandlers values 1, 2, 3.
      AMPS_CALL_EXCEPTION_WRAPPER_2(me,
                                    me->_globalCommandTypeHandlers[1 + (commandType / 8192)].invoke(message));
#endif
      AMPS_CALL_EXCEPTION_WRAPPER_2(me, me->_routes.deliverData(message,
                                                                message.getQueryID()));
    }
    else if (commandType & PublishMask)
    {
#if 0 // Not currently implemented, to avoid an extra branch in delivery
      AMPS_CALL_EXCEPTION_WRAPPER_2(me,
                                    me->_globalCommandTypeHandlers[(commandType == Message::Command::Publish ?
                                                                    GlobalCommandTypeHandlers::Publish :
                                                                    GlobalCommandTypeHandlers::OOF)].invoke(message));
#endif
      const char* subIds = NULL;
      size_t subIdsLen = 0;
      // Publish command, send to subscriptions
      amps_message_get_field_value(messageHandle_, AMPS_SubscriptionIds,
                                   &subIds, &subIdsLen);
      size_t subIdCount = me->_routes.parseRoutes(AMPS::Field(subIds, subIdsLen), me->_routeCache);
      for (size_t i = 0; i < subIdCount; ++i)
      {
        MessageRouter::RouteCache::value_type& lookupResult = me->_routeCache[i];
        MessageHandler& handler = lookupResult.handler;
        if (handler.isValid())
        {
          amps_message_set_field_value(messageHandle_,
                                       AMPS_SubscriptionId,
                                       subIds + lookupResult.idOffset,
                                       lookupResult.idLength);
          Message::Field bookmark = message.getBookmark();
          bool isMessageQueue = message.getLeasePeriod().len() != 0;
          bool isAutoAck = me->_isAutoAckEnabled;

          if (!isMessageQueue && !bookmark.empty() &&
              me->_bookmarkStore.isValid())
          {
            if (me->_bookmarkStore.isDiscarded(me->_readMessage))
            {
              //Call duplicate message handler in handlers map
              if (me->_globalCommandTypeHandlers[GlobalCommandTypeHandlers::DuplicateMessage].isValid())
              {
                AMPS_CALL_EXCEPTION_WRAPPER_2(me, me->_globalCommandTypeHandlers[GlobalCommandTypeHandlers::DuplicateMessage].invoke(message));
              }
            }
            else
            {
              me->_bookmarkStore.log(me->_readMessage);
              AMPS_CALL_EXCEPTION_WRAPPER_2(me,
                                            handler.invoke(message));
            }
          }
          else
          {
            if (isMessageQueue && isAutoAck)
            {
              try
              {
                AMPS_CALL_EXCEPTION_WRAPPER_STREAM_FULL_2(me, handler.invoke(message));
                if (!message.getIgnoreAutoAck())
                {
                  AMPS_CALL_EXCEPTION_WRAPPER_2(me,
                                                me->_ack(message.getTopic(), message.getBookmark()));
                }
              }
              catch (std::exception& ex)
              {
                if (!message.getIgnoreAutoAck())
                {
                  AMPS_CALL_EXCEPTION_WRAPPER_2(me,
                                                me->_ack(message.getTopic(), message.getBookmark(), "cancel"));
                }
                AMPS_UNHANDLED_EXCEPTION_2(me, ex);
              }
            }
            else
            {
              AMPS_CALL_EXCEPTION_WRAPPER_2(me,
                                            handler.invoke(message));
            }
          }
        }
        else
        {
          me->lastChance(message);
        }
      } // for (subidsEnd)
    }
    else if (commandType == Message::Command::Ack)
    {
      AMPS_CALL_EXCEPTION_WRAPPER_2(me,
                                    me->_globalCommandTypeHandlers[GlobalCommandTypeHandlers::Ack].invoke(message));
      unsigned ackType = message.getAckTypeEnum();
      unsigned deliveries = 0U;
      switch (ackType)
      {
      case Message::AckType::Persisted:
        deliveries += me->persistedAck(message);
        break;
      case Message::AckType::Processed: // processed
        deliveries += me->processedAck(message);
        break;
      }
      AMPS_CALL_EXCEPTION_WRAPPER_2(me, deliveries += me->_routes.deliverAck(message, ackType));
      if (deliveries == 0)
      {
        me->lastChance(message);
      }
    }
    else if (commandType == Message::Command::Heartbeat)
    {
      AMPS_CALL_EXCEPTION_WRAPPER_2(me,
                                    me->_globalCommandTypeHandlers[GlobalCommandTypeHandlers::Heartbeat].invoke(message));
      if (me->_heartbeatTimer.getTimeout() != 0.0) // -V550
      {
        me->checkAndSendHeartbeat(true);
      }
      else
      {
        me->lastChance(message);
      }
      return AMPS_E_OK;
    }
    else if (!message.getCommandId().empty())
    {
      unsigned deliveries = 0U;
      try
      {
        while (me->_connected) // Keep sending heartbeats when stream is full
        {
          try
          {
            deliveries = me->_routes.deliverData(message, message.getCommandId());
            break;
          }
#ifdef _WIN32
          catch (MessageStreamFullException&)
#else
          catch (MessageStreamFullException& ex_)
#endif
          {
            me->checkAndSendHeartbeat(false);
          }
        }
      }
      catch (std::exception& ex_)
      {
        try
        {
          me->_exceptionListener->exceptionThrown(ex_);
        }
        catch (...)
        {
          ;
        }
      }
      if (deliveries == 0)
      {
        me->lastChance(message);
      }
    }
    me->checkAndSendHeartbeat();
    return AMPS_E_OK;
  }

  inline void
  ClientImpl::ClientImplPreDisconnectHandler(amps_handle /*client*/, unsigned failedConnectionVersion, void* userData)
  {
    ClientImpl* me = (ClientImpl*) userData;
    //Client wrapper(me);
    // Go ahead and signal any waiters if they are around...
    me->clearAcks(failedConnectionVersion);
  }

  inline amps_result
  ClientImpl::ClientImplDisconnectHandler(amps_handle /*client*/, void* userData)
  {
    ClientImpl* me = (ClientImpl*) userData;
    Lock<Mutex> l(me->_lock);
    Client wrapper(me, false);
    if (me->_connected)
    {
      me->broadcastConnectionStateChanged(ConnectionStateListener::Disconnected);
    }
    while (true)
    {
      AtomicFlagFlip subFlip(&me->_badTimeToHASubscribe);
      try
      {
        me->_connected = false;
        {
          // Have to release the lock here or receive thread can't
          // invoke the message handler.
          Unlock<Mutex> unlock(me->_lock);
          me->_disconnectHandler.invoke(wrapper);
        }
      }
      catch (const std::exception& ex)
      {
        AMPS_UNHANDLED_EXCEPTION_2(me, ex);
      }
      me->_lock.signalAll();

      if (!me->_connected)
      {
        me->broadcastConnectionStateChanged(ConnectionStateListener::Shutdown);
        AMPS_UNHANDLED_EXCEPTION_2(me, DisconnectedException("Reconnect failed."));
        return AMPS_E_DISCONNECTED;
      }
      try
      {
        // Resubscribe
        if (me->_subscriptionManager)
        {
          {
            // Have to release the lock here or receive thread can't
            // invoke the message handler.
            Unlock<Mutex> unlock(me->_lock);
            me->_subscriptionManager->resubscribe(wrapper);
          }
          me->broadcastConnectionStateChanged(ConnectionStateListener::Resubscribed);
        }
        return AMPS_E_OK;
      }
      catch (const AMPSException& subEx)
      {
        AMPS_UNHANDLED_EXCEPTION_2(me, subEx);
      }
      catch (const std::exception& subEx)
      {
        AMPS_UNHANDLED_EXCEPTION_2(me, subEx);
        return AMPS_E_RETRY;
      }
      catch (...)
      {
        return AMPS_E_RETRY;
      }
    }
    return AMPS_E_RETRY;
  }

  class FIX
  {
    const char* _data;
    size_t _len;
    char _fieldSep;
  public:
    class iterator
    {
      const char* _data;
      size_t _len;
      size_t _pos;
      char _fieldSep;
      iterator(const char* data_, size_t len_, size_t pos_, char fieldSep_)
        : _data(data_), _len(len_), _pos(pos_), _fieldSep(fieldSep_)
      {
        while (_pos != _len && _data[_pos] == _fieldSep)
        {
          ++_pos;
        }
      }
    public:
      typedef void* difference_type;
      typedef std::forward_iterator_tag iterator_category;
      typedef std::pair<Message::Field, Message::Field> value_type;
      typedef value_type* pointer;
      typedef value_type& reference;
      bool operator==(const iterator& rhs) const
      {
        return _pos == rhs._pos;
      }
      bool operator!=(const iterator& rhs) const
      {
        return _pos != rhs._pos;
      }
      iterator& operator++()
      {
        // Skip through the data
        while (_pos != _len && _data[_pos] != _fieldSep)
        {
          ++_pos;
        }
        // Skip through any field separators
        while (_pos != _len && _data[_pos] == _fieldSep)
        {
          ++_pos;
        }
        return *this;
      }

      value_type operator*() const
      {
        value_type result;
        size_t i = _pos, keyLength = 0, valueStart = 0, valueLength = 0;
        for (; i < _len && _data[i] != '='; ++i)
        {
          ++keyLength;
        }

        result.first.assign(_data + _pos, keyLength);

        if (i < _len && _data[i] == '=')
        {
          ++i;
          valueStart = i;
          for (; i < _len && _data[i] != _fieldSep; ++i)
          {
            valueLength++;
          }
        }
        result.second.assign(_data + valueStart, valueLength);
        return result;
      }

      friend class FIX;
    };
    class reverse_iterator
    {
      const char* _data;
      size_t _len;
      const char* _pos;
      char _fieldSep;
    public:
      typedef std::pair<Message::Field, Message::Field> value_type;
      reverse_iterator(const char* data, size_t len, const char* pos, char fieldsep)
        : _data(data), _len(len), _pos(pos), _fieldSep(fieldsep)
      {
        if (_pos)
        {
          // skip past meaningless trailing fieldseps
          while (_pos >= _data && *_pos == _fieldSep)
          {
            --_pos;
          }
          while (_pos > _data && *_pos != _fieldSep)
          {
            --_pos;
          }
          // if we stopped before the 0th character, it's because
          // it's a field sep.  advance one to point to the first character
          // of a key.
          if (_pos > _data || (_pos == _data && *_pos == _fieldSep))
          {
            ++_pos;
          }
          if (_pos < _data)
          {
            _pos = 0;
          }
        }
      }
      bool operator==(const reverse_iterator& rhs) const
      {
        return _pos == rhs._pos;
      }
      bool operator!=(const reverse_iterator& rhs) const
      {
        return _pos != rhs._pos;
      }
      reverse_iterator& operator++()
      {
        if (_pos == _data)
        {
          _pos = 0;
        }
        else
        {
          // back up 1 to a field separator
          --_pos;
          // keep backing up through field separators
          while (_pos >= _data && *_pos == _fieldSep)
          {
            --_pos;
          }
          // now back up to the beginning of this field
          while (_pos > _data && *_pos != _fieldSep)
          {
            --_pos;
          }
          if (_pos > _data || (_pos == _data && *_pos == _fieldSep))
          {
            ++_pos;
          }
          if (_pos < _data)
          {
            _pos = 0;
          }
        }
        return *this;
      }
      value_type operator*() const
      {
        value_type result;
        size_t keyLength = 0, valueStart = 0, valueLength = 0;
        size_t i = (size_t)(_pos - _data);
        for (; i < _len && _data[i] != '='; ++i)
        {
          ++keyLength;
        }
        result.first.assign(_pos, keyLength);
        if (i < _len && _data[i] == '=')
        {
          ++i;
          valueStart = i;
          for (; i < _len && _data[i] != _fieldSep; ++i)
          {
            valueLength++;
          }
        }
        result.second.assign(_data + valueStart, valueLength);
        return result;
      }
    };
    FIX(const Message::Field& data, char fieldSeparator = 1)
      : _data(data.data()), _len(data.len()),
        _fieldSep(fieldSeparator)
    {
    }

    FIX(const char* data, size_t len, char fieldSeparator = 1)
      : _data(data), _len(len), _fieldSep(fieldSeparator)
    {
    }

    iterator begin() const
    {
      return iterator(_data, _len, 0, _fieldSep);
    }
    iterator end() const
    {
      return iterator(_data, _len, _len, _fieldSep);
    }


    reverse_iterator rbegin() const
    {
      return reverse_iterator(_data, _len, _data + (_len - 1), _fieldSep);
    }

    reverse_iterator rend() const
    {
      return reverse_iterator(_data, _len, 0, _fieldSep);
    }
  };


/// Provides a convenient way of building messages in FIX format,
/// typically referenced using the typedefs AMPS::FIXBuilder or
/// AMPS::NVFIXBuilder.
/// This builder simply writes to an underlying \c std::stringstream as fields
/// are added. It does not maintain a dictionary of key/value pairs or
/// otherwise attempt to normalize or optimize the message. This also means
/// that the builder is not validating, and does not prevent an application
/// from producing FIX that is semantically invalid, is missing required
/// fields, or that contains unexpected characters.
///
/// @tparam T The type to accept for keys. This type must provide the \c <<
///         operator to write to a \c std::ostream.

  template <class T>
  class _FIXBuilder
  {
    std::stringstream _data;
    char _fs;
  public:
    ///
    /// Construct an instance of _FIXBuilder, using the specified
    /// separator between fields.
    ///
    /// @param fieldSep_ separator to use between fields (defaults to 0x01)
    _FIXBuilder(char fieldSep_ = (char)1) : _fs(fieldSep_) {;}

    /// Write a field with the provided tag and value to the message
    /// being constructed.
    ///
    /// @param tag The tag to use for the field.
    /// @param value A pointer to the memory that contains the value to use for the field.
    /// @param offset The offset from \c value at which the value begins.
    /// @param length The length of the value.
    void append(const T& tag, const char* value, size_t offset, size_t length)
    {
      _data << tag << '=';
      _data.write(value + offset, (std::streamsize)length);
      _data << _fs;
    }
    /// Write a field with the provided tag and value to the message
    /// being constructed.
    ///
    /// @param tag The tag to use for the field.
    /// @param value The value to use for the field.
    void append(const T& tag, const std::string& value)
    {
      _data << tag << '=' << value << _fs;
    }

    /// Returns the current contents of this builder as a string.
    /// @returns the current message
    std::string getString() const
    {
      return _data.str();
    }
    operator std::string() const
    {
      return _data.str();
    }

    /// Clear all data from the builder.
    void reset()
    {
      _data.str(std::string());
    }
  };

/// Provides convenience methods for creating messages in the format
/// defined by FIX, accepting \c int values as keys (this typedef
/// is a specialization of _FIXBuilder.

  typedef _FIXBuilder<int> FIXBuilder;

/// Provides convenience methods for creating messages in the format
/// defined by FIX, accepting \c std::string values as keys (this typedef
/// is a specialization of _FIXBuilder).

  typedef _FIXBuilder<std::string> NVFIXBuilder;


/// Class for parsing a FIX format message into a \c std::map of
/// keys and values, where the keys and values are represented by
/// AMPS::Field objects. This class does not validate the contents of
/// a message, nor does it require any particular format for the keys
/// that it parses. This class is designed for
/// efficiency, so the AMPS::Field objects returned contain pointers
/// to the original parsed message rather than copies.

  class FIXShredder
  {
    char _fs;
  public:
    /// Construct an instance of FIXShredder using the specified value
    /// as the delimiter between fields.
    ///
    /// @param fieldSep_ the field delimiter (defaults to 0x01)
    FIXShredder(char fieldSep_ = (char)1) : _fs(fieldSep_) {;}

    /// Convenience defintion for the \c std::map specialization
    /// used for this class.
    typedef std::map<Message::Field, Message::Field> map_type;

    /// Returns the key/value pairs within the message, represented
    /// as AMPS::Field objects that contain pointers into the original
    /// message.
    ///
    /// @param data the data to shred into a map
    map_type toMap(const Message::Field& data)
    {
      FIX fix(data, _fs);
      map_type retval;
      for (FIX::iterator a = fix.begin(); a != fix.end(); ++a)
      {
        retval.insert(*a);
      }

      return retval;
    }
  };

#define AMPS_MESSAGE_STREAM_CACHE_MAX 128
  class MessageStreamImpl : public AMPS::RefBody, AMPS::ConnectionStateListener
  {
    Mutex                _lock;
    std::deque<Message>  _q;
    std::deque<Message>  _cache;
    std::string          _commandId;
    std::string          _subId;
    std::string          _queryId;
    Client               _client;
    unsigned             _timeout;
    unsigned             _maxDepth;
    unsigned             _requestedAcks;
    size_t               _cacheMax;
    Message::Field       _previousTopic;
    Message::Field       _previousBookmark;
    typedef enum : unsigned int { Unset = 0x0, Running = 0x10, Subscribe = 0x11, SOWOnly = 0x12, AcksOnly = 0x13, Conflate = 0x14, Closed = 0x1, Disconnected = 0x2 } State;
#if __cplusplus >= 201100L || _MSC_VER >= 1900
    std::atomic<State> _state;
#else
    volatile State _state;
#endif
    typedef std::map<std::string, Message*> SOWKeyMap;
    SOWKeyMap _sowKeyMap;
  public:
    MessageStreamImpl(const Client& client_)
      : _client(client_),
        _timeout(0),
        _maxDepth((unsigned)~0),
        _requestedAcks(0),
        _cacheMax(AMPS_MESSAGE_STREAM_CACHE_MAX),
        _state(Unset)
    {
      if (_client.isValid())
      {
        _client.addConnectionStateListener(this);
      }
    }

    MessageStreamImpl(ClientImpl* client_)
      : _client(client_),
        _timeout(0),
        _maxDepth((unsigned)~0),
        _requestedAcks(0),
        _state(Unset)
    {
      if (_client.isValid())
      {
        _client.addConnectionStateListener(this);
      }
    }

    ~MessageStreamImpl()
    {
    }

    virtual void destroy()
    {
      try
      {
        close();
      }
      catch (std::exception& e)
      {
        try
        {
          if (_client.isValid())
          {
            _client.getExceptionListener().exceptionThrown(e);
          }
        }
        catch (...) {/*Ignore exception listener exceptions*/}   // -V565
      }
      if (_client.isValid())
      {
        _client.removeConnectionStateListener(this);
        Client c = _client;
        _client = Client((ClientImpl*)NULL);
        c.deferredExecution(MessageStreamImpl::destroyer, this);
      }
      else
      {
        delete this;
      }
    }

    static void destroyer(void* vpMessageStreamImpl_)
    {
      delete ((MessageStreamImpl*)vpMessageStreamImpl_);
    }

    void setSubscription(const std::string& subId_,
                         const std::string& commandId_ = "",
                         const std::string& queryId_ = "")
    {
      Lock<Mutex> lock(_lock);
      _subId = subId_;
      if (!commandId_.empty() && commandId_ != subId_)
      {
        _commandId = commandId_;
      }
      if (!queryId_.empty() && queryId_ != subId_ && queryId_ != commandId_)
      {
        _queryId = queryId_;
      }
      // It's possible to disconnect between creation/registration and here.
      if (Disconnected == _state)
      {
        return;
      }
      assert(Unset == _state);
      _state = Subscribe;
    }

    void setSOWOnly(const std::string& commandId_,
                    const std::string& queryId_ = "")
    {
      Lock<Mutex> lock(_lock);
      _commandId = commandId_;
      if (!queryId_.empty() && queryId_ != commandId_)
      {
        _queryId = queryId_;
      }
      // It's possible to disconnect between creation/registration and here.
      if (Disconnected == _state)
      {
        return;
      }
      assert(Unset == _state);
      _state = SOWOnly;
    }

    void setStatsOnly(const std::string& commandId_,
                      const std::string& queryId_ = "")
    {
      Lock<Mutex> lock(_lock);
      _commandId = commandId_;
      if (!queryId_.empty() && queryId_ != commandId_)
      {
        _queryId = queryId_;
      }
      // It's possible to disconnect between creation/registration and here.
      if (Disconnected == _state)
      {
        return;
      }
      assert(Unset == _state);
      _state = AcksOnly;
      _requestedAcks = Message::AckType::Stats;
    }

    void setAcksOnly(const std::string& commandId_, unsigned acks_)
    {
      Lock<Mutex> lock(_lock);
      _commandId = commandId_;
      // It's possible to disconnect between creation/registration and here.
      if (Disconnected == _state)
      {
        return;
      }
      assert(Unset == _state);
      _state = AcksOnly;
      _requestedAcks = acks_;
    }

    void connectionStateChanged(ConnectionStateListener::State state_)
    {
      Lock<Mutex> lock(_lock);
      if (state_ == AMPS::ConnectionStateListener::Disconnected)
      {
        _state = Disconnected;
        close();
      }
      _lock.signalAll();
    }

    void timeout(unsigned timeout_)
    {
      _timeout = timeout_;
    }
    void conflate(void)
    {
      if (_state == Subscribe)
      {
        _state = Conflate;
      }
    }
    void maxDepth(unsigned maxDepth_)
    {
      if (maxDepth_)
      {
        _maxDepth = maxDepth_;
      }
      else
      {
        _maxDepth = (unsigned)~0;
      }
    }
    unsigned getMaxDepth(void) const
    {
      return _maxDepth;
    }
    unsigned getDepth(void) const
    {
      return (unsigned)(_q.size());
    }

    bool next(Message& current_)
    {
      Lock<Mutex> lock(_lock);
      if (!_previousTopic.empty() && !_previousBookmark.empty())
      {
        try
        {
          if (_client.isValid())
          {
            _client.ackDeferredAutoAck(_previousTopic, _previousBookmark);
          }
        }
#ifdef _WIN32
        catch (AMPSException&)
#else
        catch (AMPSException& e)
#endif
        {
          current_.invalidate();
          _previousTopic.clear();
          _previousBookmark.clear();
          return false;
        }
        _previousTopic.clear();
        _previousBookmark.clear();
      }
      double minWaitTime = (double)((_timeout && _timeout > 1000)
                                    ? _timeout : 1000);
      Timer timer(minWaitTime);
      timer.start();
      while (_q.empty() && _state & Running)
      {
        // Using timeout so python can interrupt
        _lock.wait((long)minWaitTime);
        {
          Unlock<Mutex> unlck(_lock);
          amps_invoke_waiting_function();
        }
        if (_timeout)
        {
          // In case we woke up early, see how much longer to wait
          if (timer.checkAndGetRemaining(&minWaitTime))
          {
            break;
          }
        }
      }
      if (current_.isValid() && _cache.size() < _cacheMax)
      {
        current_.reset();
        _cache.push_back(current_);
      }
      if (!_q.empty())
      {
        current_ = _q.front();
        if (_q.size() == _maxDepth)
        {
          _lock.signalAll();
        }
        _q.pop_front();
        if (_state == Conflate)
        {
          std::string sowKey = current_.getSowKey();
          if (sowKey.length())
          {
            _sowKeyMap.erase(sowKey);
          }
        }
        else if (_state == AcksOnly)
        {
          _requestedAcks &= ~(current_.getAckTypeEnum());
        }
        if ((_state == AcksOnly && _requestedAcks == 0) ||
            (_state == SOWOnly && current_.getCommand() == "group_end"))
        {
          _state = Closed;
        }
        else if (current_.getCommandEnum() == Message::Command::Publish &&
                 _client.isValid() && _client.getAutoAck() &&
                 !current_.getLeasePeriod().empty() &&
                 !current_.getBookmark().empty())
        {
          _previousTopic = current_.getTopic().deepCopy();
          _previousBookmark = current_.getBookmark().deepCopy();
        }
        return true;
      }
      if (_state == Disconnected)
      {
        throw DisconnectedException("Connection closed.");
      }
      current_.invalidate();
      if (_state == Closed)
      {
        return false;
      }
      return _timeout != 0;
    }
    void close(void)
    {
      if (_client.isValid())
      {
        if (_state == SOWOnly || _state == Subscribe) //not delete
        {
          if (!_commandId.empty())
          {
            _client.unsubscribe(_commandId);
          }
          if (!_subId.empty())
          {
            _client.unsubscribe(_subId);
          }
          if (!_queryId.empty())
          {
            _client.unsubscribe(_queryId);
          }
        }
        else
        {
          if (!_commandId.empty())
          {
            _client.removeMessageHandler(_commandId);
          }
          if (!_subId.empty())
          {
            _client.removeMessageHandler(_subId);
          }
          if (!_queryId.empty())
          {
            _client.removeMessageHandler(_queryId);
          }
        }
      }
      if (_state == SOWOnly || _state == Subscribe || _state == Unset)
      {
        _state = Closed;
      }
    }
    static void _messageHandler(const Message& message_, MessageStreamImpl* this_)
    {
      Lock<Mutex> lock(this_->_lock);
      if (this_->_state != Conflate)
      {
        AMPS_TESTING_SLOW_MESSAGE_STREAM
        if (this_->_q.size() >= this_->_maxDepth)
        {
          // We throw here so that heartbeats can be sent. The exception
          // will be handled internally only, and the same Message will
          // come back to try again. Make sure to signal.
          this_->_lock.signalAll();
          throw MessageStreamFullException("Stream is currently full.");
        }
        if (!this_->_cache.empty())
        {
          this_->_cache.front().deepCopy(message_);
          this_->_q.push_back(this_->_cache.front());
          this_->_cache.pop_front();
        }
        else
        {
#ifdef AMPS_USE_EMPLACE
          this_->_q.emplace_back(message_.deepCopy());
#else
          this_->_q.push_back(message_.deepCopy());
#endif
        }
        if (message_.getCommandEnum() == Message::Command::Publish &&
            this_->_client.isValid() && this_->_client.getAutoAck() &&
            !message_.getLeasePeriod().empty() &&
            !message_.getBookmark().empty())
        {
          message_.setIgnoreAutoAck();
        }
      }
      else
      {
        std::string sowKey = message_.getSowKey();
        if (sowKey.length())
        {
          SOWKeyMap::iterator it = this_->_sowKeyMap.find(sowKey);
          if (it != this_->_sowKeyMap.end())
          {
            it->second->deepCopy(message_);
          }
          else
          {
            if (this_->_q.size() >= this_->_maxDepth)
            {
              // We throw here so that heartbeats can be sent. The
              // exception will be handled internally only, and the
              // same Message will come back to try again. Make sure
              // to signal.
              this_->_lock.signalAll();
              throw MessageStreamFullException("Stream is currently full.");
            }
            if (!this_->_cache.empty())
            {
              this_->_cache.front().deepCopy(message_);
              this_->_q.push_back(this_->_cache.front());
              this_->_cache.pop_front();
            }
            else
            {
#ifdef AMPS_USE_EMPLACE
              this_->_q.emplace_back(message_.deepCopy());
#else
              this_->_q.push_back(message_.deepCopy());
#endif
            }
            this_->_sowKeyMap[sowKey] = &(this_->_q.back());
          }
        }
        else
        {
          if (this_->_q.size() >= this_->_maxDepth)
          {
            // We throw here so that heartbeats can be sent. The exception
            // will be handled internally only, and the same Message will
            // come back to try again. Make sure to signal.
            this_->_lock.signalAll();
            throw MessageStreamFullException("Stream is currently full.");
          }
          if (!this_->_cache.empty())
          {
            this_->_cache.front().deepCopy(message_);
            this_->_q.push_back(this_->_cache.front());
            this_->_cache.pop_front();
          }
          else
          {
#ifdef   AMPS_USE_EMPLACE
            this_->_q.emplace_back(message_.deepCopy());
#else
            this_->_q.push_back(message_.deepCopy());
#endif
          }
          if (message_.getCommandEnum() == Message::Command::Publish &&
              this_->_client.isValid() && this_->_client.getAutoAck() &&
              !message_.getLeasePeriod().empty() &&
              !message_.getBookmark().empty())
          {
            message_.setIgnoreAutoAck();
          }
        }
      }
      this_->_lock.signalAll();
    }
  };
  inline MessageStream::MessageStream(void)
  {
  }
  inline MessageStream::MessageStream(const Client& client_)
    : _body(new MessageStreamImpl(client_))
  {
  }
  inline void MessageStream::iterator::advance(void)
  {
    _pStream = _pStream->_body->next(_current) ? _pStream : NULL;
  }
  inline MessageStream::operator MessageHandler(void)
  {
    return MessageHandler((void(*)(const Message&, void*))MessageStreamImpl::_messageHandler, &_body.get());
  }
  inline MessageStream MessageStream::fromExistingHandler(const MessageHandler& handler_)
  {
    MessageStream result;
    if (handler_._func == (MessageHandler::FunctionType)MessageStreamImpl::_messageHandler)
    {
      result._body = (MessageStreamImpl*)(handler_._userData);
    }
    return result;
  }

  inline void MessageStream::setSOWOnly(const std::string& commandId_,
                                        const std::string& queryId_)
  {
    _body->setSOWOnly(commandId_, queryId_);
  }
  inline void MessageStream::setSubscription(const std::string& subId_,
                                             const std::string& commandId_,
                                             const std::string& queryId_)
  {
    _body->setSubscription(subId_, commandId_, queryId_);
  }
  inline void MessageStream::setStatsOnly(const std::string& commandId_,
                                          const std::string& queryId_)
  {
    _body->setStatsOnly(commandId_, queryId_);
  }
  inline void MessageStream::setAcksOnly(const std::string& commandId_,
                                         unsigned acks_)
  {
    _body->setAcksOnly(commandId_, acks_);
  }
  inline MessageStream MessageStream::timeout(unsigned timeout_)
  {
    _body->timeout(timeout_);
    return *this;
  }
  inline MessageStream MessageStream::conflate(void)
  {
    _body->conflate();
    return *this;
  }
  inline MessageStream MessageStream::maxDepth(unsigned maxDepth_)
  {
    _body->maxDepth(maxDepth_);
    return *this;
  }
  inline unsigned MessageStream::getMaxDepth(void) const
  {
    return _body->getMaxDepth();
  }
  inline unsigned MessageStream::getDepth(void) const
  {
    return _body->getDepth();
  }

  inline MessageStream ClientImpl::getEmptyMessageStream(void)
  {
    return *(_pEmptyMessageStream.get());
  }

  inline MessageStream Client::execute(Command& command_)
  {
    // If the command is sow and has a sub_id, OR
    // if the command has a replace option, return the existing
    // messagestream, don't create a new one.
    ClientImpl& body = _body.get();
    Message& message = command_.getMessage();
    Field subId = message.getSubscriptionId();
    unsigned ackTypes = message.getAckTypeEnum();
    bool useExistingHandler = !subId.empty() && ((!message.getOptions().empty() && message.getOptions().contains("replace", 7)) || message.getCommandEnum() == Message::Command::SOW);
    if (useExistingHandler)
    {
      // Try to find the existing message handler.
      if (!subId.empty())
      {
        MessageHandler existingHandler;
        if (body._routes.getRoute(subId, existingHandler))
        {
          // we found an existing handler. It might not be a message stream, but that's okay.
          body.executeAsync(command_, existingHandler, false);
          return MessageStream::fromExistingHandler(existingHandler);
        }
      }
      // fall through; we'll a new handler altogether.
    }
    // Make sure something will be returned to the stream or use the empty one
    // Check that: it's a command that doesn't normally return data, and there
    // are no acks requested for the cmd id
    Message::Command::Type command = message.getCommandEnum();
    if ((command & Message::Command::NoDataCommands)
        && (ackTypes == Message::AckType::Persisted
            || ackTypes == Message::AckType::None))
    {
      executeAsync(command_, MessageHandler());
      if (!body._pEmptyMessageStream)
      {
        body._pEmptyMessageStream.reset(new MessageStream((ClientImpl*)0));
        body._pEmptyMessageStream.get()->_body->close();
      }
      return body.getEmptyMessageStream();
    }
    MessageStream stream(*this);
    if (body.getDefaultMaxDepth())
    {
      stream.maxDepth(body.getDefaultMaxDepth());
    }
    MessageHandler handler = stream.operator MessageHandler();
    std::string commandID = body.executeAsync(command_, handler, false);
    if (command_.hasStatsAck())
    {
      stream.setStatsOnly(commandID, command_.getMessage().getQueryId());
    }
    else if (command_.isSow())
    {
      stream.setSOWOnly(commandID, command_.getMessage().getQueryId());
    }
    else if (command_.isSubscribe())
    {
      stream.setSubscription(commandID,
                             command_.getMessage().getCommandId(),
                             command_.getMessage().getQueryId());
    }
    else
    {
      // Persisted acks for writes don't come back with command id
      if (command == Message::Command::Publish ||
          command == Message::Command::DeltaPublish ||
          command == Message::Command::SOWDelete)
      {
        stream.setAcksOnly(commandID,
                           ackTypes & (unsigned)~Message::AckType::Persisted);
      }
      else
      {
        stream.setAcksOnly(commandID, ackTypes);
      }
    }
    return stream;
  }

// This is here because it uses api from Client.
  inline void Message::ack(const char* options_) const
  {
    ClientImpl* pClient = _body.get().clientImpl();
    Message::Field bookmark = getBookmark();
    if (pClient && bookmark.len() &&
        !pClient->getAutoAck())
      //(!pClient->getAutoAck() || getIgnoreAutoAck()))
    {
      pClient->ack(getTopic(), bookmark, options_);
    }
  }
}// end namespace AMPS
#endif
