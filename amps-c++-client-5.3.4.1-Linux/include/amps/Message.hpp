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
#ifndef __AMPS_MESSAGE_HPP__
#define __AMPS_MESSAGE_HPP__
#include "amps/util.hpp"
#include "amps/constants.hpp"
#include "amps/amps_generated.h"
#include "amps/Field.hpp"
#include <stdio.h>
#include <algorithm>
#include <ostream>
#include <string>
#define AMPS_UNSET_SEQUENCE (amps_uint64_t)-1

// Macros for determing what parts of TR1 we can depend on
#if defined(__GXX_EXPERIMENTAL_CXX0X__) || (_MSC_VER >= 1600)
  #define AMPS_USE_FUNCTIONAL 1
#endif

#if (_MSC_VER >= 1600) || (__GNUC__ > 4) || ( (__GNUC__ == 4) && (__GNUC_MINOR__) >=5 )
  #define AMPS_USE_LAMBDAS 1
#endif

#if (_MSC_VER >= 1600) || (__GNUC__ > 4) || ( (__GNUC__ == 4) && (__GNUC_MINOR__) >=8 )
  #define AMPS_USE_EMPLACE 1
#endif

#ifdef AMPS_USE_FUNCTIONAL
  #include <functional>
#endif

#include <algorithm>

///
/// \file  Message.hpp
/// \brief Defines the AMPS::Message class and related classes.

#define AMPS_OPTIONS_NONE ""
#define AMPS_OPTIONS_LIVE "live,"
#define AMPS_OPTIONS_OOF "oof,"
#define AMPS_OPTIONS_REPLACE "replace,"
#define AMPS_OPTIONS_NOEMPTIES "no_empties,"
#define AMPS_OPTIONS_SENDKEYS "send_keys,"
#define AMPS_OPTIONS_TIMESTAMP "timestamp,"
#define AMPS_OPTIONS_NOSOWKEY "no_sowkey,"
#define AMPS_OPTIONS_CANCEL "cancel,"
#define AMPS_OPTIONS_RESUME "resume,"
#define AMPS_OPTIONS_PAUSE "pause,"
#define AMPS_OPTIONS_FULLY_DURABLE "fully_durable,"
#define AMPS_OPTIONS_EXPIRE "expire,"
#define AMPS_OPTIONS_TOPN(x) "top_n=##x,"
#define AMPS_OPTIONS_MAXBACKLOG(x) "max_backlog=##x,"
#define AMPS_OPTIONS_RATE(x) "rate=##x,"

namespace AMPS
{
  typedef void* amps_subscription_handle;

  class ClientImpl;

///
/// Implementation class for a Message.  Holds an amps_handle
/// with the real body and storage for the message.
///
  class MessageImpl : public RefBody
  {
  private:
    amps_handle _message;
    //Mutex _lock;
    bool _owner;
    mutable bool _isIgnoreAutoAck;
    size_t _bookmarkSeqNo;
    amps_subscription_handle _subscription;
    ClientImpl* _clientImpl;
  public:
    ///
    /// Constructs a messageImpl from an existing AMPS message.
    /// The owner flag tells us if we own the message and should destroy it
    /// when done or not.
    /// \param message_ The AMPS message to use.
    /// \param owner_ If we own the memory of the message_.
    /// \param ignoreAutoAck_ If we own the memory of the message_.
    /// \param bookmarkSeqNo_ The sequence number in the bookmark store.
    /// \param subscription_ The handle of the subscription where message was logged.
    /// \param clientImpl_ The client creating the message, used for automatic queue acking.
    ///
    MessageImpl(amps_handle message_, bool owner_ = false,
                bool ignoreAutoAck_ = false, size_t bookmarkSeqNo_ = 0,
                amps_subscription_handle subscription_ = NULL,
                ClientImpl* clientImpl_ = NULL)
      : _message(message_), _owner(owner_), _isIgnoreAutoAck(ignoreAutoAck_)
      , _bookmarkSeqNo(bookmarkSeqNo_)
      , _subscription(subscription_), _clientImpl(clientImpl_)
    {
    }

    ///
    /// Constructs a MessageImpl with a new, empty AMPS message.
    ///
    MessageImpl()
      : _message(NULL), _owner(true), _isIgnoreAutoAck(false), _bookmarkSeqNo(0), _subscription(NULL), _clientImpl(NULL)
    {
      // try to create one
      _message = amps_message_create(NULL);
    }

    virtual ~MessageImpl()
    {
      if (_owner && _message)
      {
        amps_message_destroy(_message);
      }
    }

    MessageImpl* copy() const
    {
      amps_handle copy = amps_message_copy(_message);
      return new MessageImpl(copy, true, _isIgnoreAutoAck, _bookmarkSeqNo,
                             _subscription, _clientImpl);
    }

    void copy(const MessageImpl& rhs_)
    {
      if (_owner && _message)
      {
        amps_message_destroy(_message);
      }
      _message = amps_message_copy(rhs_._message);
      _owner = true;
      _bookmarkSeqNo = rhs_._bookmarkSeqNo;
      _subscription = rhs_._subscription;
      _isIgnoreAutoAck = rhs_._isIgnoreAutoAck;
      _clientImpl = rhs_._clientImpl;
    }

    void setClientImpl(ClientImpl* clientImpl_)
    {
      _clientImpl = clientImpl_;
    }

    ClientImpl* clientImpl(void) const
    {
      return _clientImpl;
    }

    ///
    /// Returns the underling AMPS message object from the C layer.
    amps_handle getMessage() const
    {
      return _message;
    }

    void reset()
    {
      //Lock<Mutex> l(_lock);
      amps_message_reset(_message);
      _bookmarkSeqNo = 0;
      _subscription = NULL;
      _isIgnoreAutoAck = false;
      _clientImpl = NULL;
    }

    ///
    /// Causes self to refer to a new AMPS message, freeing any
    /// current message owned by self along the way.
    /// \param message_ The new AMPS message to use.
    /// \param owner_ If this MessageImpl owns the memory of message_.
    void replace(amps_handle message_, bool owner_ = false)
    {
      //Lock<Mutex> l(_lock);
      if (_message == message_)
      {
        return;
      }
      if (_owner && _message)
      {
        amps_message_destroy(_message);
      }
      _owner = owner_;
      _message = message_;
      _bookmarkSeqNo = 0;
      _subscription = NULL;
      _isIgnoreAutoAck = false;
    }

    void disown()
    {
      //Lock<Mutex> l(_lock);
      _owner = false;
    }

    static unsigned long newId()
    {
#if __cplusplus >= 201100L || _MSC_VER >= 1900
      static std::atomic<uint_fast64_t> _id(0);
      return (unsigned long)++_id;
#else
      static AMPS_ATOMIC_TYPE _id = 0;
      return (unsigned long)(AMPS_FETCH_ADD(&_id, 1));
#endif
    }

    void setBookmarkSeqNo(size_t val_)
    {
      _bookmarkSeqNo = val_;
    }

    size_t getBookmarkSeqNo(void) const
    {
      return _bookmarkSeqNo;
    }

    void setSubscriptionHandle(amps_subscription_handle subscription_)
    {
      _subscription = subscription_;
    }

    amps_subscription_handle getSubscriptionHandle(void) const
    {
      return _subscription;
    }

    void setIgnoreAutoAck() const
    {
      _isIgnoreAutoAck = true;
    }

    bool getIgnoreAutoAck() const
    {
      return _isIgnoreAutoAck;
    }
  };


// This block of macros works with the Doxygen preprocessor to
// create documentation comments for fields defined with the AMPS_FIELD macro.
// A C++ compiler removes comments before expanding macros, so these macros
// must ONLY be defined for Doxygen and not for actual compilation.

#ifdef DOXYGEN_PREPROCESSOR

#define DOX_COMMENTHEAD(s) / ##  ** ## s ## * ## /
#define DOX_GROUPNAME(s) DOX_COMMENTHEAD(@name s Functions)
#define DOX_OPENGROUP(s) DOX_COMMENTHEAD(@{) \
                                         DOX_GROUPNAME(s)
#define DOX_CLOSEGROUP() DOX_COMMENTHEAD(@})
#define DOX_MAKEGETCOMMENT(x) DOX_COMMENTHEAD( Retrieves the value of the x header of the Message as a new Field. Notice that not all headers are present on all messages returned by AMPS. See the AMPS %Command Reference for details on which fields will be present in response to a specific command. )
#define DOX_MAKEGETRAWCOMMENT(x) DOX_COMMENTHEAD( Retrieves the value of the x header of self as a Field that references the underlying buffer managed by this Message. Notice that not all headers are present on all messages returned by AMPS. See the AMPS %Command Reference for details on which fields will be present in response to a specific command. )
#define DOX_MAKESETCOMMENT(x) DOX_COMMENTHEAD( Sets the value of the x header for this Message. Not all headers are processed by AMPS for all commands. See the AMPS %Command Reference for which headers are used by AMPS for a specific command. )
#define DOX_MAKEASSIGNCOMMENT(x) DOX_COMMENTHEAD( Sets the value of the x header for this Message. Not all headers are processed by AMPS for all commands. See the AMPS %Command Reference for which headers are used by AMPS for a specific command. )
#define DOX_MAKENEWCOMMENT(x) DOX_COMMENTHEAD(Creates and sets a new sequential value for the x header for this Message. This function is most useful for headers such as %CommandId and %SubId.)

#else

#define DOX_COMMENTHEAD(s)
#define DOX_GROUPNAME(s)
#define DOX_OPENGROUP(x)
#define DOX_CLOSEGROUP()
#define DOX_MAKEGETCOMMENT(x)
#define DOX_MAKEGETRAWCOMMENT(x)
#define DOX_MAKESETCOMMENT(x)
#define DOX_MAKEASSIGNCOMMENT(x)
#define DOX_MAKENEWCOMMENT(x)

#endif

// Macro for defining all of the necessary methods for a field in an AMPS
// message.


#define AMPS_FIELD(x) \
  DOX_OPENGROUP(x) \
  DOX_MAKEGETCOMMENT(x) \
  Field get##x() const {\
    Field returnValue;\
    const char* ptr;\
    size_t sz;\
    amps_message_get_field_value(_body.get().getMessage(),\
                                 AMPS_##x, &ptr, &sz);\
    returnValue.assign(ptr, sz);\
    return returnValue;\
  }\
  DOX_MAKEGETRAWCOMMENT(x) \
  void getRaw##x(const char** dataptr, size_t* sizeptr) const {\
    amps_message_get_field_value(_body.get().getMessage(),\
                                 AMPS_##x, dataptr, sizeptr);\
    return;\
  }\
  DOX_MAKESETCOMMENT(x) \
  Message& set##x(const std::string& v) {\
    amps_message_set_field_value(_body.get().getMessage(),\
                                 AMPS_##x, v.c_str(), v.length());\
    return *this;\
  }\
  DOX_MAKESETCOMMENT(x) \
  Message& set##x(amps_uint64_t v) {\
    char buf[22];\
    AMPS_snprintf_amps_uint64_t(buf,22,v);\
    amps_message_set_field_value_nts(_body.get().getMessage(),\
                                     AMPS_##x, buf);\
    return *this;\
  }\
  DOX_MAKEASSIGNCOMMENT(x) \
  Message& assign##x(const std::string& v) {\
    amps_message_assign_field_value(_body.get().getMessage(),\
                                    AMPS_##x, v.c_str(), v.length());\
    return *this;\
  }\
  DOX_MAKEASSIGNCOMMENT(x) \
  Message& assign##x(const char* data, size_t len) {\
    amps_message_assign_field_value(_body.get().getMessage(),\
                                    AMPS_##x, data, len);\
    return *this;\
  }\
  DOX_MAKESETCOMMENT(x) \
  Message& set##x(const char* str) {\
    amps_message_set_field_value_nts(_body.get().getMessage(),\
                                     AMPS_##x, str);\
    return *this;\
  }\
  DOX_MAKESETCOMMENT(x) \
  Message& set##x(const char* str,size_t len) {\
    amps_message_set_field_value(_body.get().getMessage(),\
                                 AMPS_##x, str,len);\
    return *this;\
  }\
  DOX_MAKENEWCOMMENT(x) \
  Message& new##x() {\
    char buf[Message::IdentifierLength+1];\
    buf[Message::IdentifierLength] = 0;\
    AMPS_snprintf(buf, Message::IdentifierLength+1, "auto%lu" , (unsigned long)(_body.get().newId()));\
    amps_message_set_field_value_nts(_body.get().getMessage(),\
                                     AMPS_##x, buf);\
    return *this;\
  } \
  DOX_CLOSEGROUP()

#define AMPS_FIELD_ALIAS(x,y) \
  DOX_OPENGROUP(y) \
  DOX_MAKEGETCOMMENT(y) \
  Field get##y() const {\
    Field returnValue;\
    const char* ptr;\
    size_t sz;\
    amps_message_get_field_value(_body.get().getMessage(),\
                                 AMPS_##y, &ptr, &sz);\
    returnValue.assign(ptr, sz);\
    return returnValue;\
  }\
  DOX_MAKEGETRAWCOMMENT(y) \
  void getRaw##y(const char** dataptr, size_t* sizeptr) const {\
    amps_message_get_field_value(_body.get().getMessage(),\
                                 AMPS_##y, dataptr, sizeptr);\
    return;\
  }\
  DOX_MAKESETCOMMENT(y) \
  Message& set##y(const std::string& v) {\
    amps_message_set_field_value(_body.get().getMessage(),\
                                 AMPS_##y, v.c_str(), v.length());\
    return *this;\
  }\
  DOX_MAKESETCOMMENT(y) \
  Message& set##y(amps_uint64_t v) {\
    char buf[22];\
    AMPS_snprintf_amps_uint64_t(buf,22,v);\
    amps_message_set_field_value_nts(_body.get().getMessage(),\
                                     AMPS_##y, buf);\
    return *this;\
  }\
  DOX_MAKEASSIGNCOMMENT(y) \
  Message& assign##y(const std::string& v) {\
    amps_message_assign_field_value(_body.get().getMessage(),\
                                    AMPS_##y, v.c_str(), v.length());\
    return *this;\
  }\
  DOX_MAKEASSIGNCOMMENT(y) \
  Message& assign##y(const char* data, size_t len) {\
    amps_message_assign_field_value(_body.get().getMessage(),\
                                    AMPS_##y, data, len);\
    return *this;\
  }\
  DOX_MAKESETCOMMENT(y) \
  Message& set##y(const char* str) {\
    amps_message_set_field_value_nts(_body.get().getMessage(),\
                                     AMPS_##y, str);\
    return *this;\
  }\
  DOX_MAKESETCOMMENT(y) \
  Message& set##y(const char* str,size_t len) {\
    amps_message_set_field_value(_body.get().getMessage(),\
                                 AMPS_##y, str,len);\
    return *this;\
  }\
  DOX_MAKENEWCOMMENT(y) \
  Message& new##y() {\
    char buf[Message::IdentifierLength+1];\
    buf[Message::IdentifierLength] = 0;\
    AMPS_snprintf(buf, Message::IdentifierLength+1, "auto%lux" , (unsigned long)(_body.get().newId()));\
    amps_message_set_field_value_nts(_body.get().getMessage(),\
                                     AMPS_##y, buf);\
    return *this;\
  }\
  DOX_MAKEGETCOMMENT(y) \
  Field get##x() const {\
    Field returnValue;\
    const char* ptr;\
    size_t sz;\
    amps_message_get_field_value(_body.get().getMessage(),\
                                 AMPS_##y, &ptr, &sz);\
    returnValue.assign(ptr, sz);\
    return returnValue;\
  }\
  DOX_MAKEGETRAWCOMMENT(y) \
  void getRaw##x(const char** dataptr, size_t* sizeptr) const {\
    amps_message_get_field_value(_body.get().getMessage(),\
                                 AMPS_##y, dataptr, sizeptr);\
    return;\
  }\
  DOX_MAKESETCOMMENT(y) \
  Message& set##x(const std::string& v) {\
    amps_message_set_field_value(_body.get().getMessage(),\
                                 AMPS_##y, v.c_str(), v.length());\
    return *this;\
  }\
  DOX_MAKESETCOMMENT(y) \
  Message& set##x(amps_uint64_t v) {\
    char buf[22];\
    AMPS_snprintf_amps_uint64_t(buf,22,v);\
    amps_message_set_field_value_nts(_body.get().getMessage(),\
                                     AMPS_##y, buf);\
    return *this;\
  }\
  DOX_MAKEASSIGNCOMMENT(y) \
  Message& assign##x(const std::string& v) {\
    amps_message_assign_field_value(_body.get().getMessage(),\
                                    AMPS_##y, v.c_str(), v.length());\
    return *this;\
  }\
  DOX_MAKEASSIGNCOMMENT(y) \
  Message& assign##x(const char* data, size_t len) {\
    amps_message_assign_field_value(_body.get().getMessage(),\
                                    AMPS_##y, data, len);\
    return *this;\
  }\
  DOX_MAKESETCOMMENT(y) \
  Message& set##x(const char* str) {\
    amps_message_set_field_value_nts(_body.get().getMessage(),\
                                     AMPS_##y, str);\
    return *this;\
  }\
  DOX_MAKESETCOMMENT(y) \
  Message& set##x(const char* str,size_t len) {\
    amps_message_set_field_value(_body.get().getMessage(),\
                                 AMPS_##y, str,len);\
    return *this;\
  }\
  DOX_MAKENEWCOMMENT(y) \
  Message& new##x() {\
    char buf[Message::IdentifierLength+1];\
    buf[Message::IdentifierLength] = 0;\
    AMPS_snprintf(buf, Message::IdentifierLength+1, "auto%lux" , (unsigned long)(_body.get().newId()));\
    amps_message_set_field_value_nts(_body.get().getMessage(),\
                                     AMPS_##y, buf);\
    return *this;\
  } \
  DOX_CLOSEGROUP()


///
/// Message encapsulates a single message sent to or received from an AMPS
/// server, and provides methods for every header that can be present,
/// whether or not that header will be populated or used in a particular
/// context.
///
/// Applications typically use a Command to create outgoing requests to AMPS,
/// and receive instances of Message in response.
///
/// The AMPS %Command Reference provides details on which headers are used
/// by AMPS and which will be populated on messages received from AMPS,
/// depending on the command the Message responds to, the options and
/// headers set on that command, and the type of the response Message.
///
/// Message is based on a handle-body metaphor, so you can copy
/// Message objects around with fairly high performance, and without worrying
/// about resource leaks or double-frees.
///
/// The Message class has been designed to minimize unnecessary memory
/// allocation. Copying a Message copies the handle, but does not copy
/// the underlying body. When the AMPS client provides a Message
/// to a MessageHandler function, the data in that Message refers to the
/// buffer that the AMPS client uses to read from the socket. If your
/// application will use the Message after the MessageHandler returns
/// (for example, by dispatching the message to a worker thread), you
/// should use the deepCopy() function to copy the underlying data, since
/// in this case the AMPS client will reuse the underlying buffer once
/// the MessageHandler returns.
///
/// If your application has the need to bypass most of the Client
/// infrastructure for some reason when sending commands to AMPS, the
/// Message / Client.send() interface provides the flexibility to do so.
/// In return, your application must provide functionality that is normally
/// provided automatically by the Client (for example, tracking subscriptions
/// for failover, recording the message in the publish store and managing
/// success or failure of the publish, and so on). Although this functionality
/// is available for flexibility, it is rarely needed in practice. 60East
/// recommends using Command objects with Client.execute() and
/// Client.executeAsync() for sending commands to AMPS.
///
  class Message
  {
    RefHandle<MessageImpl> _body;

    Message(MessageImpl* body_) : _body(body_) { ; }

  public:
    typedef AMPS::Field Field;

    /// The length of identifiers used for unique identification
    /// of commands and subscriptions.
    static const unsigned int IdentifierLength = 32;

    ///
    /// An indicator of no bookmark value.
    static const size_t BOOKMARK_NONE = AMPS_UNSET_INDEX;

    ///
    /// A flag to indicate not to create a body. Useful for creating a
    /// Message that is only going to copy another Message.
    enum CtorFlag { EMPTY };

    ///
    /// Constructs a new empty, invalid Message.
    Message(CtorFlag) : _body()
    {
    }

    ///
    /// Constructs a new Message to wrap message.
    /// Only necessary if you're using both the C client and C++.
    /// \param message_ the C-client message handle to wrap.
    /// \param owner_ Flag to indicate if this Message object is the owner of message handle and responsible for destroying it.
    ///
    Message(amps_handle message_, bool owner_ = false)
      : _body(new MessageImpl(message_, owner_))
    {
    }

    ///
    /// Construct a new, empty Message.
    ///
    Message() : _body(new MessageImpl())
    {
    }

    ///
    /// Returns a deep copy of self
    Message deepCopy(void) const
    {
      return Message(_body.get().copy());
    }

    ///
    /// Makes self a deep copy of rhs_
    void deepCopy(const Message& rhs_)
    {
      _body.get().copy(rhs_._body.get());
    }

    ///
    /// Class for constructing the options string to pass to AMPS in a Message.
    ///
    /// This class can provide a convenient way to format an options string
    /// in a command provided to AMPS. Notice that this class is only
    /// intended to help with correctly formatting the options string
    /// and to provide constant values for option names. The class does not
    /// validate the values provided to options, or that the combination of
    /// options provided is useful or valid for any particular command.
    ///
    class Options
    {
    public:
      static const char* None(void)
      {
        return AMPS_OPTIONS_NONE;
      }
      static const char* Live(void)
      {
        return AMPS_OPTIONS_LIVE;
      }
      static const char* OOF(void)
      {
        return AMPS_OPTIONS_OOF;
      }
      static const char* Replace(void)
      {
        return AMPS_OPTIONS_REPLACE;
      }
      static const char* NoEmpties(void)
      {
        return AMPS_OPTIONS_NOEMPTIES;
      }
      static const char* SendKeys(void)
      {
        return AMPS_OPTIONS_SENDKEYS;
      }
      static const char* Timestamp(void)
      {
        return AMPS_OPTIONS_TIMESTAMP;
      }
      static const char* NoSowKey(void)
      {
        return AMPS_OPTIONS_NOSOWKEY;
      }
      static const char* Cancel(void)
      {
        return AMPS_OPTIONS_CANCEL;
      }
      static const char* Resume(void)
      {
        return AMPS_OPTIONS_RESUME;
      }
      static const char* Pause(void)
      {
        return AMPS_OPTIONS_PAUSE;
      }
      static const char* FullyDurable(void)
      {
        return AMPS_OPTIONS_FULLY_DURABLE;
      }
      static const char* Expire(void)
      {
        return AMPS_OPTIONS_EXPIRE;
      }
      static std::string Conflation(const char* conflation_)
      {
        char buf[64];
        AMPS_snprintf(buf, sizeof(buf), "conflation=%s,", conflation_);
        return buf;
      }
      static std::string ConflationKey(const char* conflationKey_)
      {
        std::string option("conflation_key=");
        option.append(conflationKey_).append(",");
        return option;
      }
      static std::string TopN(int topN_)
      {
        char buf[24];
        AMPS_snprintf(buf, sizeof(buf), "top_n=%d,", topN_);
        return buf;
      }
      static std::string MaxBacklog(int maxBacklog_)
      {
        char buf[24];
        AMPS_snprintf(buf, sizeof(buf), "max_backlog=%d,", maxBacklog_);
        return buf;
      }
      static std::string Rate(const char* rate_)
      {
        char buf[64];
        AMPS_snprintf(buf, sizeof(buf), "rate=%s,", rate_);
        return buf;
      }
      static std::string RateMaxGap(const char* rateMaxGap_)
      {
        char buf[64];
        AMPS_snprintf(buf, sizeof(buf), "rate_max_gap=%s,", rateMaxGap_);
        return buf;
      }
      static std::string SkipN(int skipN_)
      {
        char buf[24];
        AMPS_snprintf(buf, sizeof(buf), "skip_n=%d,", skipN_);
        return buf;
      }

      static std::string Projection(const std::string& projection_)
      {
        return "projection=[" + projection_ + "],";
      }

      template<class Iterator>
      static std::string Projection(Iterator begin_, Iterator end_)
      {
        std::string projection = "projection=[";
        for (Iterator i = begin_; i != end_; ++i)
        {
          projection += *i;
          projection += ',';
        }
        projection.insert(projection.length() - 1, "]");
        return projection;
      }

      static std::string Grouping(const std::string& grouping_)
      {
        return "grouping=[" + grouping_ + "],";
      }

      template<class Iterator>
      static std::string Grouping(Iterator begin_, Iterator end_)
      {
        std::string grouping = "grouping=[";
        for (Iterator i = begin_; i != end_; ++i)
        {
          grouping += *i;
          grouping += ',';
        }
        grouping.insert(grouping.length() - 1, "]");
        return grouping;
      }

      static std::string Select(const std::string& select_)
      {
        return "select=[" + select_ + "],";
      }

      template<class Iterator>
      static std::string Select(Iterator begin_, Iterator end_)
      {
        std::string select = "select=[";
        for (Iterator i = begin_; i != end_; ++i)
        {
          select += *i;
          select += ',';
        }
        select.insert(select.length() - 1, "]");
        return select;
      }

      static std::string AckConflationInterval(const std::string& interval_)
      {
        return "ack_conflation=" + interval_ + ",";
      }

      static std::string AckConflationInterval(const char* interval_)
      {
        static const std::string start("ack_conflation=");
        return start + interval_ + ",";
      }

      ///
      /// ctor - default to None
      Options(std::string options_ = "")
        : _optionStr(options_)
        , _maxBacklog(0)
        , _topN(0)
        , _skipN(0)
      {;}

      int getMaxBacklog(void) const
      {
        return _maxBacklog;
      }
      std::string getConflation(void) const
      {
        return _conflation;
      }
      std::string getConflationKey(void) const
      {
        return _conflationKey;
      }
      int getTopN(void) const
      {
        return _topN;
      }
      std::string getRate(void) const
      {
        return _rate;
      }
      std::string getRateMaxGap(void) const
      {
        return _rateMaxGap;
      }

      ///
      /// Clear any previously set options and set the options
      /// to an empty string (AMPS_OPTIONS_NONE).
      void setNone(void)
      {
        _optionStr.clear();
      }

      ///
      /// Set the live option for a bookmark subscription, which
      /// requests that the subscription receives messages before
      /// they are persisted to the transaction log after replay
      /// is complete. This can, in case of a simultaneous AMPS failure
      /// and publisher failure, lead to an application receiving
      /// messages that do not appear in the transaction log.
      /// See the AMPS User Guide and the AMPS %Command Reference for
      /// details.
      void setLive(void)
      {
        _optionStr += AMPS_OPTIONS_LIVE;
      }

      ///
      /// Set the option to receive out of focus (OOF) messages
      /// on a subscription, where applicable. See the AMPS User
      /// Guide and the AMPS %Command Reference for details.
      void setOOF(void)
      {
        _optionStr += AMPS_OPTIONS_OOF;
      }

      ///
      /// Set the option to replace a current subscription with this one.
      /// See the AMPS User Guide and the AMPS %Command Reference for
      /// details.
      void setReplace(void)
      {
        _optionStr += AMPS_OPTIONS_REPLACE;
      }

      ///
      /// Set the option to not send empty messages on a delta
      /// subscription. See the AMPS %Command Reference for details.
      void setNoEmpties(void)
      {
        _optionStr += AMPS_OPTIONS_NOEMPTIES;
      }

      ///
      /// Set the option to send key fields with a delta subscription.
      /// See the AMPS %Command Reference for details.
      void setSendKeys(void)
      {
        _optionStr += AMPS_OPTIONS_SENDKEYS;
      }

      ///
      /// Set the option to send a timestamp that the message was
      /// processed on a subscription or query. See the AMPS %Command
      /// Reference for details.
      void setTimestamp(void)
      {
        _optionStr += AMPS_OPTIONS_TIMESTAMP;
      }

      ///
      /// Set the option to not set the SowKey header on messages.
      /// See the AMPS %Command Reference for details.
      void setNoSowKey(void)
      {
        _optionStr += AMPS_OPTIONS_NOSOWKEY;
      }

      ///
      /// Set the cancel option, used on a sow_delete command to
      /// return a message to the queue.
      void setCancel(void)
      {
        _optionStr += AMPS_OPTIONS_CANCEL;
      }

      ///
      /// Set the option to resume a subscription. This option
      /// is only valid for bookmark subscriptions that do not use
      /// the "live" option that have been previously paused.
      /// See the AMPS User Guide and the AMPS %Command Reference for
      /// details.
      void setResume(void)
      {
        _optionStr += AMPS_OPTIONS_RESUME;
      }

      ///
      /// Set the option to pause a bookmark subscription. This option is
      /// typically used when entering a number of subscriptions for
      /// synchronized replay (for example, when duplicating message
      /// flow for analysis or testing purposes). The subscriptions
      /// are registered with the "pause" option one-by-one, and then
      /// simultaneously resumed to begin the replay. This option is
      /// only valid for bookmark subscriptions that do not use
      /// the "live" option.  See the AMPS User Guide and AMPS
      /// %Command Reference for details.
      void setPause(void)
      {
        _optionStr += AMPS_OPTIONS_PAUSE;
      }

      ///
      /// Set the option to only provide messages that have been
      /// persisted to all replication destinations that use
      /// synchronous acknowledgements. This option is only valid
      /// for bookmark subscriptions that do not use the "live" option.
      /// See the AMPS User Guide and AMPS %Command Reference for details.
      void setFullyDurable(void)
      {
        _optionStr += AMPS_OPTIONS_FULLY_DURABLE;
      }

      ///
      /// Set the option for maximum backlog this subscription is
      /// willing to accept. This option only applies to
      /// subscriptions to queue topics, and the server will only
      /// grant a backlog up to the maximum per subscription backlog
      /// configured for the queue. See the AMPS User Guide and the
      /// AMPS %Command Reference for a description of queue backlogs,
      /// and see the AMPS Configuration Reference for details on
      /// configuring the queue.
      /// \param maxBacklog_ The max unacked queue messages in backlog.
      void setMaxBacklog(int maxBacklog_)
      {
        char buf[24];
        AMPS_snprintf(buf, sizeof(buf), "max_backlog=%d,", maxBacklog_);
        _optionStr += buf;
        _maxBacklog = maxBacklog_;
      }

      ///
      /// Set the options for conflation on a subscription. See the
      /// AMPS User Guide for a description of conflation, and see
      /// the AMPS %Command Reference for details on this option.
      /// \param conflation_ The conflation interval, auto, or none.
      void setConflation(const char* conflation_)
      {
        char buf[64];
        AMPS_snprintf(buf, sizeof(buf), "conflation=%s,", conflation_);
        _optionStr += buf;
        _conflation = conflation_;
      }

      ///
      /// Set the options for the conflation key, the identifiers for
      /// the field or fields used by AMPS to determine which changes
      /// to the underlying topic are considered to be changes to a
      /// distinct record. The conflation key does not need to be the
      /// same as the keys configured for the topic. See the AMPS
      /// User Guide for details on conflation, and see the AMPS
      /// %Command Reference for details on this option.
      /// \param conflationKey_ The message key to use for conflation.
      void setConflationKey(const char* conflationKey_)
      {
        char buf[64];
        AMPS_snprintf(buf, sizeof(buf), "conflation_key=%s,", conflationKey_);
        _optionStr += buf;
        _conflationKey = conflationKey_;
      }

      ///
      /// Set the top N option, which specifies the maximum number
      /// of messages to return for this command. See the AMPS
      /// %Command Reference for details.
      /// \param topN_ The max number of messages to return.
      void setTopN(int topN_)
      {
        char buf[24];
        AMPS_snprintf(buf, sizeof(buf), "top_n=%d,", topN_);
        _optionStr += buf;
        _topN = topN_;
      }

      ///
      /// Set the option for the maximum rate at which messages are
      /// provided to the subscription. This option is only valid for
      /// bookmark subscriptions. See the AMPS %Command Reference for
      /// details.
      /// \param rate_ The rate for sending messages on the bookmark subscription.
      void setRate(const char* rate_)
      {
        char buf[64];
        AMPS_snprintf(buf, sizeof(buf), "rate=%s,", rate_);
        _optionStr += buf;
        _rate = rate_;
      }

      ///
      /// Set the option for the maximum amount of time that a bookmark
      /// replay with a specified rate will allow the subscription to
      /// go without producing messages. This
      /// option is only valid for bookmark subscriptions when a "rate" is
      /// specified, and prevents situations where a subscription would
      /// be idle for an extended period of time. For example, if a replay
      /// has a specified rate of "2x" and there is a quiet period in
      /// the transaction logs (overnight, weekends) where no matching
      /// messages were received for the subscription for 8 hours,
      /// the subscription would be idle for 4 hours unless a
      /// shorter maximum gap is specified using this option.
      /// See the AMPS %Command Reference for details.
      /// \param rateMaxGap_ The max gap between messages on the subscription.
      void setRateMaxGap(const char* rateMaxGap_)
      {
        char buf[64];
        AMPS_snprintf(buf, sizeof(buf), "rate_max_gap=%s,", rateMaxGap_);
        _optionStr += buf;
        _rateMaxGap = rateMaxGap_;
      }

      ///
      /// Set the option for skip N, the number of messages in the result
      /// set to skip before returning messages to a client. See the
      /// AMPS %Command Reference for details.
      /// \param skipN_ The number of messages to skip before sending to client.
      void setSkipN(int skipN_)
      {
        char buf[24];
        AMPS_snprintf(buf, sizeof(buf), "skip_n=%d,", skipN_);
        _optionStr += buf;
        _skipN = skipN_;
      }

      ///
      /// Set the option for projecting the results of an aggregated query
      /// or subscription.
      /// \param projection_ A string containing the projection specification, either a single field description or a comma-separated list of field descriptions.
      void setProjection(const std::string& projection_)
      {
        _projection = "projection=[" + projection_ + "],";
        _optionStr += _projection;
      }


      ///
      /// Set the option for projecting the results of an aggregated query
      /// or subscription.
      /// \param begin_ The starting forward iterator for field descriptions.
      /// \param end_ The ending forward iterator for field descriptions.
      template<class Iterator>
      void setProjection(Iterator begin_, Iterator end_)
      {
        _projection = "projection=[";
        for (Iterator i = begin_; i != end_; ++i)
        {
          _projection += *i;
          _projection += ',';
        }
        _projection.insert(_projection.length() - 1, "]");
        _optionStr += _projection;
      }

      ///
      /// Set the option for grouping the results of an aggregated query
      /// or subscription.
      /// \param grouping_ The comma-separated list of grouping field names.
      void setGrouping(const std::string& grouping_)
      {
        _grouping = "grouping=[" + grouping_ + "],";
        _optionStr += _grouping;
      }


      ///
      /// Set the option for grouping the results of an aggregated query
      /// or subscription.
      /// \param begin_ The starting forward iterator for field descriptions.
      /// \param end_ The ending forward iterator for field descriptions.
      template<class Iterator>
      void setGrouping(Iterator begin_, Iterator end_)
      {
        _grouping = "grouping=[";
        for (Iterator i = begin_; i != end_; ++i)
        {
          _grouping += *i;
          _grouping += ',';
        }
        _grouping.insert(_grouping.length() - 1, "]");
        _optionStr += _grouping;
      }

      ///
      /// Convert this object to a std::string, allows you to pass an
      /// Options object as the options_ argument directly
      operator const std::string()
      {
        return _optionStr.substr(0, _optionStr.length() - 1);
      }
      ///
      /// Return the length of this Options object as a string.
      /// \return The length of the string representation of self.
      size_t getLength() const
      {
          return (_optionStr.empty()?0:_optionStr.length()-1);
      }

      ///
      /// Return this Options object as a non-NULL-terminated string.
      /// \return The pointer to the beginning of self as a string. Only
      /// use this in conjunction with #getLength
      const char* getStr() const
      {
          return (_optionStr.empty()?0:_optionStr.data());
      }

    private:
      std::string _optionStr;
      int         _maxBacklog;
      std::string _conflation;
      std::string _conflationKey;
      int         _topN;
      std::string _rate;
      std::string _rateMaxGap;
      int         _skipN;
      std::string _projection;
      std::string _grouping;
    };

    /// Valid values for the setAckTypeEnum() and getAckTypeEnum() methods.
    /// These values may be bitwise OR'ed together to request multiple acks.
    struct AckType
    {
      typedef enum : unsigned
      {
        None = 0, Received = 1, Parsed = 2, Processed = 4, Persisted = 8, Completed = 16, Stats = 32
      } Type;
    };
    AMPS_FIELD(AckType)
    /// Decodes a single ack string. Use getAckTypeEnum for strings that may contain
    /// multiple acks (e.g. 'processed,persisted')
    static inline AckType::Type decodeSingleAckType(const char* begin, const char* end)
    {
      switch (end - begin)
      {
      case 5:
        return AckType::Stats;
      case 6:
        return AckType::Parsed;
      case 8:
        return AckType::Received;
      case 9:
        switch (begin[1])
        {
        case 'e': return AckType::Persisted;
        case 'r': return AckType::Processed;
        case 'o': return AckType::Completed;
        default: break;
        }
        break;
      default:
        break;
      }
      return AckType::None;
    }
    /// Decode self's "ack type" field and return the corresponding bitmask of values from
    /// AckType. This method returns unsigned instead of AckType::Type since AckType does
    /// not contain unique entries for every possible combination of ack types.
    unsigned getAckTypeEnum() const
    {
      unsigned result = AckType::None;
      const char* data = NULL; size_t len = 0;
      amps_message_get_field_value(_body.get().getMessage(), AMPS_AckType, &data, &len);
      const char* mark = data;
      for (const char* end = data + len; data != end; ++data)
      {
        if (*data == ',')
        {
          result |= decodeSingleAckType(mark, data);
          mark = data + 1;
        }
      }
      if (mark < data)
      {
        result |= decodeSingleAckType(mark, data);
      }
      return result;
    }
    /// Encode self's "ack type" field from a bitmask of values from AckType.
    /// This method is passed an unsigned instead of AckType::Type since AckType does
    /// not contain unique entries for every possible combination of ack types.
    Message& setAckTypeEnum(unsigned ackType_)
    {
      if (ackType_ < AckTypeConstants<0>::Entries)
      {
        amps_message_assign_field_value(_body.get().getMessage(), AMPS_AckType,
                                        AckTypeConstants<0>::Values[ackType_], AckTypeConstants<0>::Lengths[ackType_]);
      }
      return *this;
    }

    AMPS_FIELD(BatchSize)
    AMPS_FIELD(Bookmark)
    AMPS_FIELD(Command)

    ///
    /// Valid values for setCommandEnum() and getCommandEnum(). These
    /// values correspond to valid AMPS command types.
    struct Command
    {
      typedef enum
      {
        Unknown = 0,
        Publish = 1,
        Subscribe = 2,
        Unsubscribe = 4,
        SOW = 8,
        Heartbeat = 16,
        SOWDelete = 32,
        DeltaPublish = 64,
        Logon = 128,
        SOWAndSubscribe = 256,
        DeltaSubscribe = 512,
        SOWAndDeltaSubscribe = 1024,
        StartTimer = 2048,
        StopTimer = 4096,
        GroupBegin = 8192,
        GroupEnd = 16384,
        OOF = 32768,
        Ack = 65536,
        Flush = 131072,
        NoDataCommands = Publish | Unsubscribe | Heartbeat | SOWDelete | DeltaPublish
                         | Logon | StartTimer | StopTimer | Flush
      } Type;
    };
    /// Decode self's "command" field and return one of the values from Command.
    Command::Type getCommandEnum() const
    {
      const char* data = NULL; size_t len = 0;
      amps_message_get_field_value(_body.get().getMessage(), AMPS_Command, &data, &len);
      switch (len)
      {
      case 1: return Command::Publish; // -V1037
      case 3:
        switch (data[0])
        {
        case 's': return Command::SOW;
        case 'o': return Command::OOF;
        case 'a': return Command::Ack;
        }
        break;
      case 5:
        switch (data[0])
        {
        case 'l': return Command::Logon;
        case 'f': return Command::Flush;
        }
        break;
      case 7:
        return Command::Publish; // -V1037
        break;
      case 9:
        switch (data[0])
        {
        case 's': return Command::Subscribe;
        case 'h': return Command::Heartbeat;
        case 'g': return Command::GroupEnd;
        }
        break;
      case 10:
        switch (data[1])
        {
        case 'o': return Command::SOWDelete;
        case 't': return Command::StopTimer;
        }
        break;
      case 11:
        switch (data[0])
        {
        case 'g': return Command::GroupBegin;
        case 'u': return Command::Unsubscribe;
        }
        break;
      case 13:
        return Command::DeltaPublish;
      case 15:
        return Command::DeltaSubscribe;
      case 17:
        return Command::SOWAndSubscribe;
      case 23:
        return Command::SOWAndDeltaSubscribe;
      }
      return Command::Unknown;
    }

    /// Set self's "command" field from one of the values in Command.
    Message& setCommandEnum(Command::Type command_)
    {
      unsigned bits = 0;
      unsigned command = command_;
      while (command > 0)
      {
        ++bits;
        command >>= 1;
      }
      amps_message_assign_field_value(_body.get().getMessage(), AMPS_Command,
                                      CommandConstants<0>::Values[bits], CommandConstants<0>::Lengths[bits]);
      return *this;
    }

    AMPS_FIELD(CommandId)
    AMPS_FIELD(ClientName)
    AMPS_FIELD(CorrelationId)
    AMPS_FIELD(Expiration)
    AMPS_FIELD(Filter)
    AMPS_FIELD(GroupSequenceNumber)
    AMPS_FIELD(Heartbeat)
    AMPS_FIELD(LeasePeriod)
    AMPS_FIELD(Matches)
    AMPS_FIELD(MessageLength)
    AMPS_FIELD(MessageType)

    DOX_OPENGROUP(Options)
    DOX_MAKEGETCOMMENT(Options)
    Field getOptions() const
    {
      Field returnValue;
      const char* ptr;
      size_t sz;
      amps_message_get_field_value(_body.get().getMessage(),
                                   AMPS_Options, &ptr, &sz);
      if (sz && ptr[sz - 1] == ',')
      {
        --sz;
      }
      returnValue.assign(ptr, sz);
      return returnValue;
    }

    DOX_MAKEGETRAWCOMMENT(Options)
    void getRawOptions(const char** dataptr, size_t* sizeptr) const
    {
      amps_message_get_field_value(_body.get().getMessage(),
                                   AMPS_Options, dataptr, sizeptr);
      if (*sizeptr && *dataptr && (*dataptr)[*sizeptr - 1] == ',')
      {
        --*sizeptr;
      }
      return;
    }

    DOX_MAKESETCOMMENT(Options)
    Message& setOptions(const std::string& v)
    {
      size_t sz = v.length();
      if (sz && v[sz - 1] == ',')
      {
        --sz;
      }
      amps_message_set_field_value(_body.get().getMessage(),
                                   AMPS_Options, v.c_str(), sz);
      return *this;
    }

    DOX_MAKEASSIGNCOMMENT(Options)
    Message& assignOptions(const std::string& v)
    {
      size_t sz = v.length();
      if (sz && v[sz - 1] == ',')
      {
        --sz;
      }
      amps_message_assign_field_value(_body.get().getMessage(),
                                      AMPS_Options, v.c_str(), sz);
      return *this;
    }

    DOX_MAKEASSIGNCOMMENT(Options)
    Message& assignOptions(const char* data, size_t len)
    {
      if (len && data[len - 1] == ',')
      {
        --len;
      }
      amps_message_assign_field_value(_body.get().getMessage(),
                                      AMPS_Options, data, len);
      return *this;
    }

    DOX_MAKESETCOMMENT(Options)
    Message& setOptions(const char* str)
    {
      if (str)
      {
        size_t sz = strlen(str);
        if (sz && str[sz - 1] == ',')
        {
          --sz;
        }
        amps_message_set_field_value(_body.get().getMessage(),
                                     AMPS_Options, str, sz);
      }
      else
      {
        amps_message_set_field_value(_body.get().getMessage(),
                                     AMPS_Options, str, 0);
      }
      return *this;
    }

    DOX_MAKESETCOMMENT(Options)
    Message& setOptions(const char* str, size_t len)
    {
      if (len && str[len - 1] == ',')
      {
        --len;
      }
      amps_message_set_field_value(_body.get().getMessage(),
                                   AMPS_Options, str, len);
      return *this;
    }
    DOX_CLOSEGROUP()

    AMPS_FIELD(OrderBy)
    AMPS_FIELD(Password)
    AMPS_FIELD_ALIAS(QueryId, QueryID)
    AMPS_FIELD(Reason)
    AMPS_FIELD(RecordsInserted)
    AMPS_FIELD(RecordsReturned)
    AMPS_FIELD(RecordsUpdated)
    AMPS_FIELD(Sequence)
    AMPS_FIELD(SowDelete)
    AMPS_FIELD(SowKey)
    AMPS_FIELD(SowKeys)
    AMPS_FIELD(Status)
    AMPS_FIELD_ALIAS(SubId, SubscriptionId) // -V524
    AMPS_FIELD(SubscriptionIds)
    AMPS_FIELD(TimeoutInterval)
    AMPS_FIELD(Timestamp)

    /// \deprecated Use getTimestamp.
    /// Get self's "timestamp" field.
    /// \return The timestamp Field
    Field getTransmissionTime() const
    {
      return getTimestamp();
    }

    /// \deprecated Use getRawTimestamp.
    /// Get self's "timestamp" field.
    /// \param dataptr The pointer to be set to the timestamp.
    /// \param sizeptr The pointer to be set with the timestamp length.
    void getRawTransmissionTime(const char** dataptr, size_t* sizeptr) const
    {
      getRawTimestamp(dataptr, sizeptr);
    }

    AMPS_FIELD(Topic)
    AMPS_FIELD(TopicMatches)
    AMPS_FIELD(TopNRecordsReturned)
    AMPS_FIELD(Version)
    AMPS_FIELD(UserId)

    /// Returns the data from this message.
    ///
    /// This function returns a Field that contains a pointer to the data in
    /// the message. This function does not make a copy of the data.

    Field getData() const
    {
      Field returnValue;
      char* ptr;
      size_t sz;
      amps_message_get_data(_body.get().getMessage(), &ptr, &sz);
      returnValue.assign(ptr, sz);
      return returnValue;
    }

    void getRawData(const char** data, size_t* sz) const
    {
      amps_message_get_data(_body.get().getMessage(), (char**)data, sz);
    }
    /// Sets the data portion of self.
    /// \param v_ the string containing your data
    Message& setData(const std::string& v_)
    {
      amps_message_set_data(_body.get().getMessage(), v_.c_str(), v_.length());
      return *this;
    }
    Message& assignData(const std::string& v_)
    {
      amps_message_assign_data(_body.get().getMessage(), v_.c_str(), v_.length());
      return *this;
    }

    /// Sets the data portion of self from a char array.
    /// \param data_ a pointer to your data
    /// \param length_ the length, in bytes, of your data (excluding any null-terminator)
    Message& setData(const char* data_, size_t length_)
    {
      amps_message_set_data(_body.get().getMessage(), data_, length_);
      return *this;
    }
    Message& assignData(const char* data_, size_t length_)
    {
      amps_message_assign_data(_body.get().getMessage(), data_, length_);
      return *this;
    }

    /// Sets the data portion of self from a null-terminated string.
    /// \param data_ a pointer to your null-terminated data.
    Message& setData(const char* data_)
    {
      amps_message_set_data_nts(_body.get().getMessage(), data_);
      return *this;
    }
    Message& assignData(const char* data_)
    {
      amps_message_assign_data(_body.get().getMessage(), data_, strlen(data_));
      return *this;
    }
    amps_handle getMessage() const
    {
      return _body.get().getMessage();
    }
    void replace(amps_handle message, bool owner = false)
    {
      _body.get().replace(message, owner);
    }
    void disown()
    {
      _body.get().disown();
    }
    void invalidate()
    {
      _body = NULL;
    }
    bool isValid(void) const
    {
      return _body.isValid();
    }
    Message& reset()
    {
      _body.get().reset();
      return *this;
    }

    void setBookmarkSeqNo(size_t val)
    {
      _body.get().setBookmarkSeqNo(val);
    }

    size_t getBookmarkSeqNo() const
    {
      return _body.get().getBookmarkSeqNo();
    }

    void setSubscriptionHandle(amps_handle val)
    {
      _body.get().setSubscriptionHandle(val);
    }

    amps_handle getSubscriptionHandle() const
    {
      return _body.get().getSubscriptionHandle();
    }

    void ack(const char* options_ = NULL) const;

    void setClientImpl(ClientImpl* pClientImpl)
    {
      _body.get().setClientImpl(pClientImpl);
    }

    void setIgnoreAutoAck() const
    {
      _body.get().setIgnoreAutoAck();
    }

    bool getIgnoreAutoAck() const
    {
      return _body.get().getIgnoreAutoAck();
    }

    template <class T> // static
    void throwFor(const T& /*context_*/, const std::string& ackReason_) const
    {
      switch (ackReason_[0])
      {
      case 'a': // auth failure
        throw AuthenticationException("Logon failed for user \"" +
                                      (std::string)getUserId() + "\"");
        break;
      case 'b':
        switch (ackReason_.length())
        {
        case 10: // bad filter
          throw BadFilterException("bad filter '" +
                                   (std::string)getFilter() +
                                   "'");
          break;
        case 11: // bad sow key
          if (getSowKeys().len())
          {
            throw BadSowKeyException("bad sow key '" +
                                     (std::string)getSowKeys() +
                                     "'");
          }
          else
          {
            throw BadSowKeyException("bad sow key '" +
                                     (std::string)getSowKey() +
                                     "'");
          }
          break;
        case 15: // bad regex topic
          throw BadRegexTopicException("bad regex topic '" +
                                       (std::string)getTopic() +
                                       "'.");
          break;
        default:
          break;
        }
        break;
      case 'd':
        if (ackReason_.length() == 23) // duplicate logon attempt
        {
          throw DuplicateLogonException("Client '" +
                                        (std::string)getClientName() +
                                        "' with userid '" +
                                        (std::string)getUserId() +
                                        "' duplicate logon attempt");
        }
        break;
      case 'i':
        if (ackReason_.length() >= 9)
        {
          switch (ackReason_[8])
          {
          case 'b': // invalid bookmark
            throw InvalidBookmarkException("invalid bookmark '" +
                                           (std::string)getBookmark() +
                                           "'.");
            break;
          case 'm': // invalid message type
            throw CommandException(std::string("invalid message type '") +
                                   (std::string)getMessageType() +
                                   "'.");
            break;
          case 'o':
            if (ackReason_[9] == 'p') // invalid options
            {
              throw InvalidOptionsException("invalid options '" +
                                            (std::string)getOptions() +
                                            "'.");
            }
            else if (ackReason_[9] == 'r') // invalid order by
            {
              throw InvalidOrderByException("invalid order by '" +
                                            (std::string)getOrderBy() +
                                            "'.");
            }
            break;
          case 's': // invalid subId
            throw InvalidSubIdException("invalid subid '" +
                                        (std::string)getSubscriptionId() +
                                        "'.");
            break;
          case 't':
            if (ackReason_.length() == 13) // invalid topic
            {
              throw InvalidTopicException("invalid topic '" +
                                          (std::string)getTopic() +
                                          "'.");
            }
            else if (ackReason_.length() == 23) // invalid topic or filter
            {
              throw InvalidTopicException("invalid topic or filter. Topic '" +
                                          (std::string)getTopic() +
                                          "' Filter '" +
                                          (std::string)getFilter() +
                                          "'.");
            }
            break;
          default:
            break;
          }
        }
        break;
      case 'l':
        if (ackReason_.length() == 14) // logon required
        {
          throw LogonRequiredException("logon required before command");
        }
        break;
      case 'n':
        switch (ackReason_[4])
        {
        case ' ': // name in use
          throw NameInUseException("name in use '" +
                                   (std::string)getClientName() +
                                   "'.");
          break;
        case 'e': // not entitled
          throw NotEntitledException("User \"" +
                                     (std::string)getUserId() +
                                     "\" not entitled to topic \"" +
                                     (std::string)getTopic() +
                                     "\".");
          break;
        case 'i': // no filter or bookmark
          throw MissingFieldsException("command sent with no filter or bookmark.");
          break;
        case 'l': // no client name
          throw MissingFieldsException("command sent with no client name.");
          break;
        case 'o': // no topic or filter
          throw MissingFieldsException("command sent with no topic or filter.");
          break;
        case 's': // not supported
          throw CommandException("operation on topic '" +
                                 (std::string)getTopic() +
                                 "' with options '" +
                                 (std::string)getOptions() +
                                 "' not supported.");
          break;
        default:
          break;
        }
        break;
      case 'o':
        switch (ackReason_.length())
        {
        case 16: // orderby required
          throw MissingFieldsException("orderby required");
          break;
        case 17: // orderby too large
          throw CommandException("orderby too large '" +
                                 (std::string)getOrderBy() +
                                 "'.");
          break;
        }
        break;
      case 'p':
        throw CommandException("projection clause too large in options '" +
                               (std::string)getOptions() +
                               "'.");
        break;
      case 'r':
        switch (ackReason_[2])
        {
        case 'g': // regex topic not supported
          throw BadRegexTopicException("'regex topic not supported '" +
                                       (std::string)getTopic() +
                                       "'.");
          break;
        default:
          break;
        }
        break;
      case 's':
        switch (ackReason_[5])
        {
        case ' ': // subid in use
          throw SubidInUseException("subid in use '" +
                                    (std::string)getSubscriptionId() +
                                    "'.");
          break;
        case 'e': // sow_delete command only supports one of: filter, sow_keys, bookmark, or data
          throw CommandException("sow_delete command only supports one of: filter '" +
                                 (std::string)getFilter() +
                                 "', sow_keys '" +
                                 (std::string)getSowKeys() +
                                 "', bookmark '" +
                                 (std::string)getBookmark() +
                                 "', or data '" +
                                 (std::string)getData() +
                                 "'.");
          break;
        case 't': // sow store failed
          throw PublishException("sow store failed.");
          break;
        default:
          break;
        }
        break;
      case 't':
        switch (ackReason_[2])
        {
        case ' ': // tx store failure
          throw PublishException("tx store failure.");
          break;
        case 'n': // txn replay failed
          throw CommandException("txn replay failed for '" +
                                 (std::string)getSubId() +
                                 "'.");
          break;
        }
        break;
      default:
        break;
      }
      throw CommandException("Error from server while processing this command: '" +
                             ackReason_ + "'");
    }
  };

  inline std::string
  operator+(const std::string& lhs, const Message::Field& rhs)
  {
    return lhs + std::string(rhs);
  }

  inline std::basic_ostream<char>&
  operator<<(std::basic_ostream<char>& os, const Message::Field& rhs)
  {
    os.write(rhs.data(), (std::streamsize)rhs.len());
    return os;
  }
  inline bool
  AMPS::Field::operator<(const AMPS::Field& rhs) const
  {
    return std::lexicographical_compare(data(), data() + len(), rhs.data(), rhs.data() + rhs.len());
  }

}

#endif
