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

#ifndef _MEMORYSUBSCRIPTIONMANAGER_H_
#define _MEMORYSUBSCRIPTIONMANAGER_H_

#include <amps/ampsplusplus.hpp>
#include <amps/Field.hpp>
#include <algorithm>
#ifdef AMPS_USE_FUNCTIONAL
  #include <forward_list>
#endif
#include <list>
#include <map>
#include <memory>
#include <set>

/// \file MemorySubscriptionManager.hpp
/// \brief Provides AMPS::MemorySubscriptionManager, used by an AMPS::HAClient
/// to resubmit subscriptions if connectivity is lost while the application
/// is running.

namespace AMPS
{

///
/// A SubscriptionManager implementation that maintains subscriptions
/// placed in memory so that they can be placed again after a reconnect.
  class MemorySubscriptionManager : public SubscriptionManager
  {
  protected:

    class SubscriptionInfo
    {
    public:
      SubscriptionInfo(MessageHandler messageHandler_,
                       const Message& message_,
                       unsigned requestedAckTypes_)
        : _handler(messageHandler_)
        , _m(message_)
        , _subId(message_.getSubscriptionId())
        , _requestedAckTypes(requestedAckTypes_)
        , _useBookmark(!message_.getBookmark().empty())
        , _clearSubId(false)
      {
        std::string options = _m.getOptions();
        size_t replace = options.find("replace");
        // AMPS should be ok if options contains ,,
        static const size_t replaceLen = 7;
        if (replace != std::string::npos)
        {
          options.erase(replace, replaceLen);
          _m.setOptions(options);
        }
        _paused = (options.find("pause") != std::string::npos);
      }

      ~SubscriptionInfo()
      {
        if (_clearSubId)
        {
          _m.getSubscriptionId().clear();
        }
      }

      void resubscribe(Client& client_, int timeout_, void* userData_)
      {
        // A previous NotEntitledException could have set userid on
        // the message and that field will no longer be valid.
        _m.setUserId((const char*)0, 0);
        if (_useBookmark)
        {
          // Use the same bookmark for all members of a pause group
          if (_paused && !_recent.empty())
          {
            _m.setBookmark(_recent);
          }
          else
          {
            _m.setBookmark(client_.getBookmarkStore().getMostRecent(_subId));
          }
        }
        _m.newCommandId();
        _m.setAckTypeEnum(_requestedAckTypes);
        if (!userData_)
        {
          client_.send(_handler, _m, timeout_);
        }
        else
        {
          MessageHandler handler(_handler.function(), userData_);
          client_.send(handler, _m, timeout_);
        }
      }

      Message message() const
      {
        return _m;
      }

      MessageHandler messageHandler() const
      {
        return _handler;
      }

      const Message::Field& subId() const
      {
        return _subId;
      }

      unsigned requestedAcks() const
      {
        return _requestedAckTypes;
      }

      // Returns true if the last subId is removed, false otherwise
      bool removeSubId(const Message::Field& subId_)
      {
        size_t subIdLen = subId_.len();
        const char* subIdData = subId_.data();
        while (subIdLen && *subIdData == ',')
        {
          ++subIdData;
          --subIdLen;
        }
        while (subIdLen && subIdData[subIdLen - 1] == ',')
        {
          --subIdLen;
        }
        if (subIdLen == 0 || subIdLen > _subId.len())
        {
          return _subId.empty();
        }
        bool match = true;
        size_t matchStart = 0;
        size_t matchCount = 0;
        for (size_t i = 0; i < _subId.len(); ++i)
        {
          if (_subId.data()[i] == ',')
          {
            if (matchCount == subIdLen)
            {
              break;
            }
            matchStart = i + 1;
            matchCount = 0;
            match = true;
          }
          else if (match)
          {
            if (_subId.data()[i] == subIdData[matchCount])
            {
              ++matchCount;
            }
            else
            {
              matchCount = 0;
              match = false;
            }
          }
        }
        if (match && matchCount == subIdLen)
        {
          size_t newLen = _subId.len() - matchCount;
          if (newLen > 1) // More than just ,
          {
            while (matchStart + matchCount < _subId.len() &&
                   _subId.data()[matchStart + matchCount] == ',')
            {
              ++matchCount;
              --newLen;
            }
            char* buffer = new char[newLen];
            // Match is not first
            if (matchStart > 0)
            {
              memcpy(buffer, _subId.data(), matchStart);
            }
            // Match is not last
            if (matchStart + matchCount < _subId.len())
            {
              memcpy(buffer + matchStart,
                     _subId.data() + matchStart + matchCount,
                     _subId.len() - matchStart - matchCount);
            }
            if (_clearSubId)
            {
              _m.getSubscriptionId().clear();
            }
            else
            {
              _clearSubId = true;
            }
            _m.assignSubscriptionId(buffer, newLen);
            _subId = _m.getSubscriptionId();
            return false;
          }
          else
          {
            if (_clearSubId)
            {
              _m.getSubscriptionId().clear();
              _clearSubId = false;
            }
            else
            {
              _m.getSubscriptionId().assign(NULL, 0);
            }
            _subId = _m.getSubscriptionId();
            return true;
          }
        }
        return _subId.empty();
      }

      bool paused() const
      {
        return _paused;
      }

      void pause()
      {
        if (_paused)
        {
          return;
        }
        std::string opts(Message::Options::Pause());
        opts.append(_m.getOptions());
        _m.setOptions(opts);
        _paused = true;
      }

      std::string getMostRecent(Client& client_)
      {
        if (!_recent.empty())
        {
          return _recent;
        }
        std::map<amps_uint64_t, amps_uint64_t> publishers;
        const char* start = _subId.data();
        const char* end = _subId.data() + _subId.len();
        while (start < end)
        {
          const char* comma = (const char*)memchr(start, ',',
                                                  (size_t)(end - start));
          // No more commas found, just use start->end
          if (!comma)
          {
            comma = end;
          }
          if (comma == start)
          {
            start = comma + 1;
            continue;
          }
          Message::Field sid(start, (size_t)(comma - start));
          Message::Field sidRecent = client_.getBookmarkStore().getMostRecent(sid);
          const char* sidRecentStart = sidRecent.data();
          const char* sidRecentEnd = sidRecent.data() + sidRecent.len();
          while (sidRecentStart < sidRecentEnd)
          {
            const char* sidRecentComma = (const char*)
                                         memchr(sidRecentStart, ',',
                                                (size_t)(sidRecentEnd - sidRecentStart));
            // No more commas found, just use start->end
            if (!sidRecentComma)
            {
              sidRecentComma = sidRecentEnd;
            }
            if (sidRecentComma == sidRecentStart)
            {
              sidRecentStart = sidRecentComma + 1;
              continue;
            }
            Message::Field bookmark(sidRecentStart,
                                    (size_t)(sidRecentComma - sidRecentStart));
            amps_uint64_t publisher = (amps_uint64_t)0;
            amps_uint64_t seq = (amps_uint64_t)0;
            Field::parseBookmark(bookmark, publisher, seq);
            if (publishers.count(publisher) == 0
                || publishers[publisher] > seq)
            {
              publishers[publisher] = seq;
            }
            // Move past comma
            sidRecentStart = sidRecentComma + 1;
          }
          // Move past comma
          start = comma + 1;
        }
        std::ostringstream os;
        for (std::map<amps_uint64_t, amps_uint64_t>::iterator i = publishers.begin();
             i != publishers.end();
             ++i)
        {
          if (i->first == 0 && i->second == 0)
          {
            continue;
          }
          if (os.tellp() > 0)
          {
            os << ',';
          }
          os << i->first << '|' << i->second << "|";
        }
        _recent = os.str();
        return _recent;
      }

      void setMostRecent(const std::string& recent_)
      {
        _recent = recent_;
      }

    private:
      std::string    _recent;
      MessageHandler _handler;
      Message        _m;
      Message::Field _subId;
      unsigned       _requestedAckTypes;
      bool           _useBookmark;
      bool           _paused;
      bool           _clearSubId;

    };//class SubscriptionInfo

    typedef std::map<SubscriptionInfo*, AMPSException> FailedResubscribeMap;

    class Resubscriber
    {
      Client& _client;
      int _timeout;
    public:
      FailedResubscribeMap* _failures;

      Resubscriber(Client& client_, int timeout_)
        : _client(client_)
        , _timeout(timeout_)
      {
        _failures = new FailedResubscribeMap();
      }
      // We want the same pointer
      Resubscriber(const Resubscriber& rhs_)
      : _client(rhs_._client)
      , _timeout(rhs_._timeout)
      , _failures(rhs_._failures)
      { }
      void operator()(std::pair<Message::Field, SubscriptionInfo*> iter_)
      {
        void* data = amps_invoke_copy_route_function(iter_.second->messageHandler().userData());
        iter_.second->resubscribe(_client, _timeout, data);
      }
      void operator()(SubscriptionInfo* iter_)
      {
        try
        {
          void* data = amps_invoke_copy_route_function(iter_->messageHandler().userData());
          iter_->resubscribe(_client, _timeout, data);
        }
        catch (const AMPSException& ex)
        {
          _failures->insert(std::make_pair(iter_, ex));
        }
      }
    };

    class Deleter
    {
      bool _clearSubId;
    public:
      Deleter(bool clearSubId_ = false)
        : _clearSubId(clearSubId_)
      { }
      void operator()(std::pair<Message::Field, SubscriptionInfo*> iter_)
      {
        if (_clearSubId)
        {
          iter_.first.clear();
        }
        else
        {
          amps_invoke_remove_route_function(iter_.second->messageHandler().userData());
          delete iter_.second;
        }
      }
      void operator()(SubscriptionInfo* iter_)
      {
        delete iter_;
      }
    };

    virtual SubscriptionInfo* createSubscriptionInfo(MessageHandler messageHandler_,
                                                     const Message& message_,
                                                     unsigned requestedAckTypes_)
    {
      return new SubscriptionInfo(messageHandler_, message_,
                                  requestedAckTypes_);
    }

  public:
    typedef std::map<Message::Field, SubscriptionInfo*, Message::Field::FieldHash> SubscriptionMap;

    MemorySubscriptionManager()
      : _resubscribing(0)
      , _resubscriptionTimeout(getDefaultResubscriptionTimeout())
    { ; }

    ~MemorySubscriptionManager()
    {
      _clear();
    }

    ///
    /// Save a subscription so it can be placed again if a disconnect occurs.
    /// Generally used only internally by Client when subscriptions are placed.
    /// \param messageHandler_ The MessageHandler used for the subscription.
    /// \param message_ The Message containing the subscription to reissue.
    /// \param requestedAckTypes_ The ack types requested for the handler.
    void subscribe(MessageHandler messageHandler_,
                   const Message& message_, unsigned requestedAckTypes_)
    {
      const Message::Field& subId = message_.getSubscriptionId();
      if (!subId.empty())
      {
        Lock<Mutex> l(_lock);
        while (_resubscribing != 0)
        {
          _lock.wait(10);
        }
        std::string options = message_.getOptions();
        if (options.find("resume") != std::string::npos)
        {
          // For a resume, we store each sub id with a single Subscription
          SubscriptionInfo* subInfo = createSubscriptionInfo(MessageHandler(),
                                                             message_,
                                                             requestedAckTypes_);
          bool saved = false;
          Field fullSubId = subInfo->subId();
          const char* start = fullSubId.data();
          const char* end = fullSubId.data() + fullSubId.len();
          while (start < end)
          {
            const char* comma = (const char*)memchr(start, ',',
                                                    (size_t)(end - start));
            // No more commas found, just use start->end
            if (!comma)
            {
              comma = end;
            }
            if (comma == start)
            {
              start = comma + 1;
              continue;
            }
            Message::Field sid = Message::Field(start,
                                                (size_t)(comma - start));
            // Calling resume on something already resumed is ignored,
            // so don't update anything that exists.
            if (_resumed.find(sid) == _resumed.end())
            {
              _resumed[sid.deepCopy()] = subInfo;
              saved = true;
            }
            // Move past comma
            start = comma + 1;
          }
          if (saved)
          {
            _resumedSet.insert(subInfo);
          }
          else
          {
            delete subInfo;
          }
        }
        else if (options.find("pause") != std::string::npos)
        {
          const char* start = subId.data();
          const char* end = subId.data() + subId.len();
          while (start < end)
          {
            MessageHandler messageHandler = messageHandler_;
            const char* comma = (const char*)memchr(start, ',',
                                                    (size_t)(end - start));
            // No more commas found, just use start->end
            if (!comma)
            {
              comma = end;
            }
            if (comma == start)
            {
              start = comma + 1;
              continue;
            }
            Message::Field sid = Message::Field(start,
                                                (size_t)(comma - start));
            SubscriptionMap::iterator resume = _resumed.find(sid);
            if (resume != _resumed.end())
            {
              SubscriptionInfo* subPtr = resume->second;
              Message::Field subField(resume->first);
              _resumed.erase(resume); // Remove mapping for sid
              subField.clear();
              // If last subId, remove completely
              if (subPtr->removeSubId(sid))
              {
                _resumedSet.erase(subPtr);
                delete subPtr;
              }
            }
            // Move past comma
            start = comma + 1;
            SubscriptionMap::iterator item = _active.find(sid);
            if (item != _active.end())
            {
              if (options.find("replace") != std::string::npos)
              {
                messageHandler = item->second->messageHandler();
                delete item->second;
                _active.erase(item);
              }
              else
              {
                item->second->pause();
                continue; // Leave current one
              }
            }
            else
            {
              Unlock<Mutex> u(_lock);
              void* data = amps_invoke_copy_route_function(
                             messageHandler_.userData());
              if (data)
              {
                messageHandler = MessageHandler(messageHandler_.function(), data);
              }
            }
            Message m = message_.deepCopy();
            m.setSubscriptionId(sid.data(), sid.len());
            SubscriptionInfo* s = createSubscriptionInfo(messageHandler, m,
                                                         requestedAckTypes_);
            // Insert using the subId from s, which is deep copy of original
            _active[s->subId()] = s;
          }
        }
        else // Not a pause or resume
        {
          MessageHandler messageHandler = messageHandler_;
          SubscriptionMap::iterator item = _active.find(subId);
          if (item != _active.end())
          {
            messageHandler = item->second->messageHandler();
            delete item->second;
            _active.erase(item);
          }
          else
          {
            Unlock<Mutex> u(_lock);
            void* data = amps_invoke_copy_route_function(
                           messageHandler_.userData());
            if (data)
            {
              messageHandler = MessageHandler(messageHandler_.function(), data);
            }
          }
          SubscriptionInfo* s = createSubscriptionInfo(messageHandler,
                                                       message_,
                                                       requestedAckTypes_);
          // Insert using the subId from s, which is deep copy of original
          _active[s->subId()] = s;
        }
      }
    }

    ///
    /// Remove the subscription from the manager.
    /// \param subId_ The subscription ID of the subscription to remove.
    void unsubscribe(const Message::Field& subId_)
    {
      Lock<Mutex> l(_lock);
      SubscriptionMap::iterator item = _active.find(subId_);
      if (item != _active.end())
      {
        SubscriptionInfo* subPtr = item->second;
        _active.erase(item);
        while (_resubscribing != 0)
        {
          _lock.wait(10);
        }
        Unlock<Mutex> u(_lock);
        amps_invoke_remove_route_function(subPtr->messageHandler().userData());
        delete subPtr;
      }
      item = _resumed.find(subId_);
      if (item != _resumed.end())
      {
        SubscriptionInfo* subPtr = item->second;
        Message::Field subField(item->first);
        _resumed.erase(item);
        subField.clear();
        // If last subId, remove completely
        if (subPtr->removeSubId(subId_))
        {
          _resumedSet.erase(subPtr);
          while (_resubscribing != 0)
          {
            _lock.wait(10);
          }
          delete subPtr;
        }
      }
    }

    ///
    /// Clear all subscriptions from the manager.
    void clear()
    {
      _clear();
    }

    ///
    /// Clear all subscriptions from the manager.
    void _clear()
    {
      Lock<Mutex> l(_lock);
      while (_resubscribing != 0)
      {
        _lock.wait(10);
      }
      // Settting _resubscribing keeps other threads from touching data
      // even if lock isn't held. Don't want to hold lock when
      // amps_invoke_remove_route_function is called.
      AtomicFlagFlip resubFlip(&_resubscribing);
      {
        Unlock<Mutex> u(_lock);
        std::for_each(_active.begin(), _active.end(), Deleter());
        std::for_each(_resumedSet.begin(), _resumedSet.end(), Deleter());
        std::for_each(_resumed.begin(), _resumed.end(), Deleter(true));
      }
      _active.clear();
      _resumed.clear();
      _resumedSet.clear();
    }

    ///
    /// Place all saved subscriptions on the provided Client.
    /// \param client_ The Client on which to place the subscriptions.
    void resubscribe(Client& client_)
    {
      // At this point, it's better to throw an exception back to disconnect
      // handling than to attempt a reconnect in send, so turn off retry.
      bool retry = client_.getRetryOnDisconnect();
      client_.setRetryOnDisconnect(false);
#ifdef AMPS_USE_FUNCTIONAL
      std::forward_list<SubscriptionInfo*> subscriptions;
#else
      std::list<SubscriptionInfo*> subscriptions;
#endif
      Resubscriber resubscriber(client_, _resubscriptionTimeout);
      try
      {
        AtomicFlagFlip resubFlip(&_resubscribing);
        {
          Lock<Mutex> l(_lock);
          subscriptions.assign(_resumedSet.begin(), _resumedSet.end());
          for (SubscriptionMap::iterator iter = _active.begin();
               iter != _active.end(); ++iter)
          {
            SubscriptionInfo* sub = iter->second;
            if (sub->paused())
            {
              SubscriptionMap::iterator resIter = _resumed.find(sub->subId());
              // All pause subs resuming together should be sent with
              // bookmark as list of the resumes' most recents
              if (resIter != _resumed.end())
              {
                sub->setMostRecent(resIter->second->getMostRecent(client_));
              }
            }
            subscriptions.push_front(iter->second);
          }
        }
        std::for_each(subscriptions.begin(), subscriptions.end(),
                      resubscriber);
        std::vector<SubscriptionInfo*> removals;
        bool throwExcept = false;
        AMPSException except("None", AMPS_E_OK);
        if (_failedResubscribeHandler)
        {
          try
          {
            for (auto failedSub = resubscriber._failures->begin();
                 failedSub != resubscriber._failures->end(); ++failedSub)
            {
              SubscriptionInfo* pSubInfo = failedSub->first;
              if (_failedResubscribeHandler->failure(pSubInfo->message(),
                                                     pSubInfo->messageHandler(),
                                                     pSubInfo->requestedAcks(),
                                                     failedSub->second))
              {
                removals.push_back(pSubInfo);
              }
              else // We'll rethrow an exception for failure left in place
              {
                except = failedSub->second;
                throwExcept = true;
              }
            }
          }
          catch (const AMPSException& ex_)
          {
            except = ex_;
            throwExcept = true;
          }
          catch (const std::exception& ex_)
          {
            except = AMPSException(ex_.what(), AMPS_E_RETRY);
            throwExcept = true;
          }
          catch (...)
          {
            except = AMPSException("Unknown Exception thrown by FailedResubscribeHandler", AMPS_E_RETRY);
            throwExcept = true;
          }
        }
        else
        {
          throwExcept = !resubscriber._failures->empty();
          if (throwExcept)
          {
            except = resubscriber._failures->begin()->second;
          }
        }
        // Remove any failiures that should be removed
        if (_failedResubscribeHandler && !removals.empty())
        {
          Lock<Mutex> l(_lock);
          for (std::vector<SubscriptionInfo*>::iterator pSubInfo = removals.begin();
               pSubInfo != removals.end(); ++pSubInfo)
          {
            _active.erase((*pSubInfo)->subId());
            _resumed.erase((*pSubInfo)->subId());
            _resumedSet.erase(*pSubInfo);
            delete *pSubInfo;
          }
        }
        // Throw an exception not removed
        if (throwExcept)
        {
          throw except;
        }
        delete resubscriber._failures;
        resubscriber._failures = 0;
        client_.setRetryOnDisconnect(retry);
      }
      catch (const AMPSException&)
      {
        delete resubscriber._failures;
        resubscriber._failures = 0;
        client_.setRetryOnDisconnect(retry);
        throw;
      }
      catch (const std::exception&)
      {
        delete resubscriber._failures;
        resubscriber._failures = 0;
        client_.setRetryOnDisconnect(retry);
        throw;
      }
    }

    ///
    /// Sets the timeout used when trying to resubscribe after disconnect.
    /// \param timeout_ The timeout to use in milliseconds.
    void setResubscriptionTimeout(int timeout_)
    {
      if (timeout_ >= 0)
      {
        _resubscriptionTimeout = timeout_;
      }
    }

    ///
    /// Gets the timeout used when trying to resubscribe after disconnect.
    /// \return The timeout used in milliseconds.
    int getResubscriptionTimeout(void)
    {
      return _resubscriptionTimeout;
    }

    ///
    /// Sets the default timeout used by new MemorySubscriptionManager objects
    /// when trying to resubscribe after disconnect.
    /// \param timeout_ The timeout to use in milliseconds.
    static int setDefaultResubscriptionTimeout(int timeout_)
    {
      static int _defaultResubscriptionTimeout =
        AMPS_SUBSCRIPTION_MANAGER_DEFAULT_TIMEOUT;
      if (timeout_ >= 0)
      {
        _defaultResubscriptionTimeout = timeout_;
      }
      return _defaultResubscriptionTimeout;
    }

    ///
    /// Gets the default timeout used by new MemorySubscriptionManager objects
    /// when trying to resubscribe after disconnect.
    /// \return The timeout used in milliseconds.
    static int getDefaultResubscriptionTimeout(void)
    {
      return setDefaultResubscriptionTimeout(-1);
    }

  private:

    SubscriptionMap _active;
    SubscriptionMap _resumed;
    std::set<SubscriptionInfo*> _resumedSet;
    Mutex _lock;
    AMPS_ATOMIC_TYPE_8 _resubscribing;
    int _resubscriptionTimeout;
  }; //class MemorySubscriptionManager

} // namespace AMPS

#endif //_MEMORYSUBSCRIPTIONMANAGER_H_

