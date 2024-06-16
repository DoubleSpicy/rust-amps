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
#ifndef _MESSAGEROUTER_HPP_
#define _MESSAGEROUTER_HPP_
#include <map>
#include "amps/ampscrc.hpp"
#include "amps/util.hpp"
#include "amps/Message.hpp"

namespace AMPS
{
/// Wrapper for callback functions in AMPS.
/// Can be used to bind a function pointer with a void* to use as a callback,
/// or if available, can wrap a std::function<Object> object, so you
/// can supply the return value of std::bind, or even a lambda.
  template <typename Func, typename Object>
  class Handler
  {
  protected:
    friend class MessageStream;
    Func _func;
    void* _userData;
#ifdef AMPS_USE_FUNCTIONAL
    std::function<void(Object)> _callable;
#endif
    bool _isValid;

  public:
    // No op function for handlers
    static void noOpHandler(Object) {;}

    typedef Func FunctionType;
    ///
    /// Null constructor -- no function is wrapped
    Handler() : _func(NULL), _userData(NULL)
#ifdef AMPS_USE_FUNCTIONAL
      , _callable(Handler<Func, Object>::noOpHandler)
#endif
      , _isValid(false)
    {
    }

    /// Constructor for use with a bare function pointer.
    /// \param func_ The function pointer to bind.  func must be declared:j
    ///             void myFunction(Object o, void* userData);
    ///             for example, void myHandler(const Message&m, void *userData);
    /// \param userData_ a value you'd like passed in when you're called.
    Handler(Func func_, void* userData_)
      : _func(func_), _userData(userData_)
#ifdef AMPS_USE_FUNCTIONAL
      , _callable(noOpHandler)
#endif
      , _isValid(true)
    {
    }

    /// Copy constructor
    /// \param orig_ The handler to copy
    Handler(const Handler& orig_)
      : _func(orig_._func), _userData(orig_._userData)
#ifdef AMPS_USE_FUNCTIONAL
      , _callable(orig_._callable)
#endif
      , _isValid(true)
    {
    }
#ifdef AMPS_USE_FUNCTIONAL
    /// Constructor for use with a standard c++ library function object
    /// \param callback_ An object that can be assigned to a std::function
    template <typename T>
    Handler(const T& callback_)
      : _func(NULL), _userData(NULL), _callable(callback_), _isValid(true)
    {
    }
#endif
    void invoke(Object message)
    {
      if (_func)
      {
        _func(message, _userData);
      }
#ifdef AMPS_USE_FUNCTIONAL
      else
      {
        _callable(message);
      }
#endif
    }

    Handler& operator=(const Handler& rhs_)
    {
      if (this != &rhs_)
      {
        _func = rhs_._func;
        _userData = rhs_._userData;
#ifdef AMPS_USE_FUNCTIONAL
        _callable = rhs_._callable;
#endif
        _isValid = rhs_._isValid;
      }
      return *this;
    }

    bool  isValid(void) const
    {
      return _isValid;
    }
    Func  function(void) const
    {
      return _func;
    }
    void* userData(void) const
    {
      return _userData;
    }
  };
  class Message;
///
/// A function pointer type for message-handler functions.
  typedef void(*MessageHandlerFunc)(const Message&, void* userData);

  typedef Handler<MessageHandlerFunc, const Message&> MessageHandler;

///
/// This class multiplexes messages from AMPS to multiple subscribers
/// and uses the stream of acks from AMPS to remove routes as appropriate.
  class MessageRouter
  {
  private:
    MessageHandler _emptyMessageHandler;
    typedef amps_uint64_t (*CRCFunction)(const char*, size_t, amps_uint64_t);
    // Function used to calculate the CRC if one is used
    CRCFunction     _crc;

    class MessageRoute
    {
      MessageHandler _messageHandler;
      unsigned _requestedAcks, _systemAcks, _terminationAck;
    public:
      MessageRoute() : _requestedAcks(0), _systemAcks(0), _terminationAck(0) {;}
      MessageRoute(const MessageRoute& rhs_) :
        _messageHandler(rhs_._messageHandler),
        _requestedAcks (rhs_._requestedAcks),
        _systemAcks    (rhs_._systemAcks),
        _terminationAck(rhs_._terminationAck)
      {;}
      const MessageRoute& operator=(const MessageRoute& rhs_)
      {
        _messageHandler = rhs_._messageHandler;
        _requestedAcks  = rhs_._requestedAcks;
        _systemAcks     = rhs_._systemAcks;
        _terminationAck = rhs_._terminationAck;
        return *this;
      }
      MessageRoute(MessageHandler messageHandler_, unsigned requestedAcks_,
                   unsigned systemAcks_, bool isSubscribe_) :
        _messageHandler(messageHandler_),
        _requestedAcks(requestedAcks_),
        _systemAcks(systemAcks_),
        _terminationAck(0)
      {
        if (!isSubscribe_)
        {
          // The ack to terminate the route on is whatever the highest
          // bit set in requestedAcks is.
          unsigned bitCounter = (requestedAcks_ | systemAcks_) >> 1;
          _terminationAck = 1;
          while (bitCounter > 0)
          {
            bitCounter >>= 1;
            _terminationAck <<= 1;
          }
        }
      }

      // Deliver an ack to registered handler if the ack type was requested
      unsigned deliverAck(const Message& message_, unsigned ackType_)
      {
        if ( (_requestedAcks & ackType_) == 0)
        {
          return 0;
        }
        try
        {
          _messageHandler.invoke(message_);
        }
        catch (std::exception& ex)
        {
          std::cerr << ex.what() << std::endl;
        }
        return 1;
      }
      bool isTerminationAck(unsigned ackType_) const
      {
        return ackType_ == _terminationAck;
      }
      unsigned deliverData(const Message& message_)
      {
        _messageHandler.invoke(message_);
        return 1;
      }
      const MessageHandler& getMessageHandler() const
      {
        return _messageHandler;
      }
      MessageHandler& getMessageHandler()
      {
        return _messageHandler;
      }
    };

  public:
    MessageRouter()
      : _previousCommandId(0),
        _lookupGenerationCount(0),
        _generationCount(0)
    {
#ifndef AMPS_SSE_42
      _crc = AMPS::CRC<0>::crcNoSSE;
#else
      if (AMPS::CRC<0>::isSSE42Enabled())
      {
        _crc = AMPS::CRC<0>::crc;
      }
      else
      {
        _crc = AMPS::CRC<0>::crcNoSSE;
      }
#endif
    }

    int addRoute(const Field& commandId_, const AMPS::MessageHandler& messageHandler_,
                 unsigned requestedAcks_, unsigned systemAcks_, bool isSubscribe_)
    {
      Lock<Mutex> lock(_lock);
      RouteMap::iterator i = _routes.find(commandId_);
      if (i == _routes.end())
      {
        _routes[commandId_.deepCopy()] = MessageRoute(messageHandler_, requestedAcks_, systemAcks_, isSubscribe_);
        return 1;
      }
      else
      {
        // Only replace a non-subscribe with a subscribe
        if (isSubscribe_ && !i->second.isTerminationAck(0))
        {
          void* routeData = i->second.getMessageHandler().userData();;
          i->second = MessageRoute(messageHandler_, requestedAcks_, systemAcks_, isSubscribe_);
          if (routeData)
          {
            Unlock<Mutex> u(_lock);
            amps_invoke_remove_route_function(routeData);
          }
          return 1;
        }
      }
      return 0;
    }

    // returns true if a route was removed.
    bool removeRoute(const Field& commandId_)
    {
      Lock<Mutex> lock(_lock);
      RouteMap::iterator i = _routes.find(commandId_);
      if (i == _routes.end())
      {
        return false;
      }
      return _removeRoute(i);
    }

    void clear()
    {
      AMPS_FETCH_ADD(&_generationCount, 1);
      std::vector<void*> removeData;
      {
        Lock<Mutex> lock(_lock);
        for (RouteMap::iterator i = _routes.begin(); i != _routes.end(); ++i)
        {
          // Make a non-const copy of Field and clear it, which will clear i
          // as well but won't actually affect the map, which is unaware that
          // the key's shared pointer has been deleted.
          Field f = i->first;
          void* data = i->second.getMessageHandler().userData();
          removeData.push_back(data);
          f.clear();
        }
        _routes.clear();
      }
      for (size_t i = 0; i < removeData.size(); ++i)
      {
        amps_invoke_remove_route_function(removeData[i]);
      }
    }

    // Returns true if a route exists for a single id.
    bool hasRoute(const Field& commandId_) const
    {
      Lock<Mutex> lock(_lock);
      RouteMap::const_iterator it = _routes.find(commandId_);
      return it != _routes.end();
    }

    // Find a single route and return true if here, setting result_ to the handler.
    bool getRoute(const Field& commandId_, MessageHandler& result_) const
    {
      Lock<Mutex> lock(_lock);
      RouteMap::const_iterator it = _routes.find(commandId_);
      if (it != _routes.end())
      {
        result_ = it->second.getMessageHandler();
        return true;
      }
      else
      {
        result_ = _emptyMessageHandler;
        return false;
      }
    }

    // RouteCache is the result type for a parseRoutes(); we do extra work
    //   to avoid hitting the map or its lock when the subids field on
    //   publish messages does not change.
    struct RouteLookup
    {
      size_t         idOffset;
      size_t         idLength;
      MessageHandler handler;
    };
    class RouteCache : public std::vector<RouteLookup>
    {
      RouteCache(const RouteCache&);
      void operator=(const RouteCache&);
    public:
      RouteCache(void)
        : _generationCount(0),
          _hashVal(0)
      {;}

      void invalidateCache(void)
      {
#if __cplusplus >= 201100L || _MSC_VER >= 1900
        _generationCount.store(0);
#else
        _generationCount = 0;
#endif
        _hashVal = 0;
        clear();
      }
#if __cplusplus >= 201100L || _MSC_VER >= 1900
      void invalidateCache(const std::atomic<uint_fast64_t>& generationCount_, amps_uint64_t hashVal_)
      {
        _generationCount.store(generationCount_);
#else
      void invalidateCache(const AMPS_ATOMIC_TYPE& generationCount_, amps_uint64_t hashVal_)
      {
        _generationCount = generationCount_;
#endif
        _hashVal = hashVal_;
        clear();
      }

#if __cplusplus >= 201100L || _MSC_VER >= 1900
      bool isCacheHit(const std::atomic<uint_fast64_t>& generationCount_, amps_uint64_t hashVal_) const
#else
      bool isCacheHit(const AMPS_ATOMIC_TYPE& generationCount_, amps_uint64_t hashVal_) const
#endif
      {
        return _generationCount == generationCount_ && _hashVal == hashVal_;
      }

    private:
#if __cplusplus >= 201100L || _MSC_VER >= 1900
      std::atomic<uint_fast64_t> _generationCount;
#else
      AMPS_ATOMIC_TYPE _generationCount;
#endif
      amps_uint64_t _hashVal;
    };

    // Parses the command id list into the route lookup vector and assigns
    // the found handlers into the list. Only intended to be called by the
    // message handler thread. Returns the number of command/sub IDs parsed.
    size_t parseRoutes(const Field& commandIdList_, RouteCache& result_)
    {
      // Super shortcut: if the whole subID list is the same as the previous one,
      // then assume the result_ contains all the right handlers already, and that
      // the offsets and lengths of subIds are unchanged.
      amps_uint64_t listHash = _crc(commandIdList_.data(), commandIdList_.len(), 0);
      if (result_.isCacheHit(_generationCount, listHash))
      {
        return result_.size();
      }
      result_.invalidateCache(_generationCount, listHash);

      // Lock required now that we'll be using the route map.
      Lock<Mutex> lockGuard(_lock);
      size_t resultCount = 0;
      const char* pStart = commandIdList_.data();
      for (const char* p = pStart, *e = commandIdList_.len() + pStart; p < e;
           ++p, ++resultCount)
      {
        const char* delimiter = p;
        while (delimiter != e && *delimiter != ',')
        {
          ++delimiter;
        }
        AMPS::Field subId(p, (size_t)(delimiter - p));
#ifdef AMPS_USE_EMPLACE
        result_.emplace_back(RouteLookup());
#else
        result_.push_back(RouteLookup());
#endif
        // Push back and then copy over fields; would emplace_back if available on
        // all supported compilers.
        RouteLookup& result = result_[resultCount];
        result.idOffset = (size_t)(p - pStart);
        result.idLength = (size_t)(delimiter - p);

        RouteMap::const_iterator it = _routes.find(subId);
        if (it != _routes.end())
        {
          result.handler = it->second.getMessageHandler();
        }
        else
        {
          result.handler = _emptyMessageHandler;
        }
        p = delimiter;
      }
      return resultCount;
    }
    unsigned deliverAck(const Message& ackMessage_, unsigned ackType_)
    {
      assert(ackMessage_.getCommand() == "ack");
      unsigned messagesDelivered = 0;
      Field key;

      // Call _deliverAck, which will deliver to any waiting handlers
      // AND remove the route if it's a termination ack
      if (key = ackMessage_.getCommandId(), !key.empty())
      {
        messagesDelivered += _deliverAck(ackMessage_, ackType_, key);
      }
      if (key = ackMessage_.getQueryID(),
          !key.empty() && messagesDelivered == 0)
      {
        messagesDelivered += _deliverAck(ackMessage_, ackType_, key);
      }
      if (key = ackMessage_.getSubscriptionId(),
          !key.empty() && messagesDelivered == 0)
      {
        messagesDelivered += _deliverAck(ackMessage_, ackType_, key);
      }
      return messagesDelivered;
    }

    // deliverData may only be called by the message handler thread.
    unsigned deliverData(const Message& dataMessage_, const Field& commandId_)
    {
      unsigned messagesDelivered = 0;
      amps_uint64_t hval = _crc(commandId_.data(), commandId_.len(), 0);
      if (_previousCommandId == hval &&
          _lookupGenerationCount == _generationCount)
      {
        messagesDelivered += _previousHandler.deliverData(dataMessage_);
      }
      else
      {
        Lock<Mutex> lock(_lock);
        RouteMap::iterator it = _routes.find(commandId_);
        if (it != _routes.end())
        {
          _previousCommandId = hval;
#if __cplusplus >= 201100L || _MSC_VER >= 1900
          _lookupGenerationCount.store(_generationCount);
#else
          _lookupGenerationCount = _generationCount;
#endif
          _previousHandler = it->second;
          messagesDelivered += it->second.deliverData(dataMessage_);
        }
      }
      return messagesDelivered;
    }

    void invalidateCache(void)
    {
      _previousCommandId = 0;
    }

    void unsubscribeAll(void)
    {
      AMPS_FETCH_ADD(&_generationCount, 1);
      std::vector<Field> removeIds;
      std::vector<void*> removeData;
      Lock<Mutex> lock(_lock);
      for (RouteMap::iterator it = _routes.begin(); it != _routes.end(); ++it)
      {
        if (it->second.isTerminationAck(0))
        {
          removeIds.push_back(it->first);
          removeData.push_back(it->second.getMessageHandler().userData());
        }
      }
      for (size_t i = 0; i < removeIds.size(); ++i)
      {
        // it can't be end() b/c we have the lock and found id above
        RouteMap::iterator it = _routes.find(removeIds[i]);
        // Make a non-const copy of Field and clear it, which will clear i as well
        Field f = it->first; // -V783
        f.clear();
        _routes.erase(it);
      }
      Unlock<Mutex> u(_lock);
      for (size_t i = 0; i < removeData.size(); ++i)
      {
        amps_invoke_remove_route_function(removeData[i]);
      }
    }

  private:
    typedef std::map<Field, MessageRoute> RouteMap;
    RouteMap _routes;
    mutable Mutex _lock;

    MessageRoute                       _previousHandler;
    amps_uint64_t                      _previousCommandId;
#if __cplusplus >= 201100L || _MSC_VER >= 1900
    mutable std::atomic<uint_fast64_t> _lookupGenerationCount;
    mutable std::atomic<uint_fast64_t> _generationCount;
#else
    mutable AMPS_ATOMIC_TYPE                _lookupGenerationCount;
    mutable AMPS_ATOMIC_TYPE                _generationCount;
#endif


    // Deliver the ack to any waiting handlers
    // AND remove the route if it's a termination ack
    unsigned _deliverAck(const Message& ackMessage_, unsigned ackType_, Field& commandId_)
    {
      Lock<Mutex> lock(_lock);
      unsigned messagesDelivered = 0;
      RouteMap::iterator it = _routes.find(commandId_);
      if (it != _routes.end())
      {
        MessageRoute& route = it->second;
        messagesDelivered += route.deliverAck(ackMessage_, ackType_);
        if (route.isTerminationAck(ackType_))
        {
          _removeRoute(it);
          ++messagesDelivered;
        }
      }
      return messagesDelivered;
    }
    unsigned _processAckForRemoval(unsigned ackType_, Field& commandId_)
    {
      Lock<Mutex> lock(_lock);
      RouteMap::iterator it = _routes.find(commandId_);
      if (it != _routes.end())
      {
        MessageRoute& route = it->second;
        if (route.isTerminationAck(ackType_))
        {
          _removeRoute(it);
          return 1U;
        }
      }
      return 0U;
    }

    // returns true if a route was removed.
    bool _removeRoute(RouteMap::iterator& it_)
    {
      // Called with lock already held
      AMPS_FETCH_ADD(&_generationCount, 1);
      // Make a non-const copy of Field and clear it, which will clear i as well
      Field f = it_->first;
      void* routeData = it_->second.getMessageHandler().userData();
      _routes.erase(it_);
      f.clear();
      if (routeData)
      {
        Unlock<Mutex> u(_lock);
        amps_invoke_remove_route_function(routeData);
      }
      return true;
    }

  };


}

#endif
