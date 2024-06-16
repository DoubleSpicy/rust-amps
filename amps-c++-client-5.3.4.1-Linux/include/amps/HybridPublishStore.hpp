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

#ifndef _HYBRIDPUBLISHSTORE_H_
#define _HYBRIDPUBLISHSTORE_H_

#include <amps/ampsplusplus.hpp>
#include <amps/MemoryPublishStore.hpp>
#include <amps/PublishStore.hpp>
#if __cplusplus >= 201100L || _MSC_VER >= 1900
#include <atomic>
#endif

/// \file HybridPublishStore.hpp
/// \brief Provides AMPS::HybridPublishStore, a publish store that uses
/// memory up to a specified maximum capacity then writes to a file if
/// the store will exceed that capacity.

namespace AMPS
{
///
/// An implementation of StoreImpl for publication. This store uses memory
/// up to a specified capacity, then starts using a file if it will exceed
/// the set maximum memory capacity. This store does not provide guaranteed
/// durable publication if the application is restarted. However, this store
/// does help to limit the amount of memory consumed by a publisher when the
/// publisher cannot connect to AMPS.
  class HybridPublishStore : public StoreImpl
  {
    class HandlerData
    {
    public:
      HybridPublishStore* _store;
      PublishStoreResizeHandler _handler;
      void* _data;

      HandlerData()
        : _store(NULL), _handler(NULL), _data(NULL)
      { ; }

      void init(PublishStoreResizeHandler handler_, void* data_)
      {
        _handler = handler_;
        _data = data_;
      }
    };

    class SwappingOutReplayer : public StoreReplayer
    {
      PublishStore* _pStore;
#if __cplusplus >= 201100L || _MSC_VER >= 1900
      std::atomic<size_t> _entries;
      std::atomic<size_t> _errorCount;
      std::atomic<amps_uint64_t> _lastIndex;
#else
      volatile size_t _entries;
      volatile size_t _errorCount;
      volatile amps_uint64_t _lastIndex;
#endif
    public:
      SwappingOutReplayer(PublishStore* pStore_, size_t entries_)
        : _pStore(pStore_), _entries(entries_)
        , _errorCount(0), _lastIndex(0)
      { }

      size_t getErrors()
      {
        return _errorCount;
      }

      amps_uint64_t lastIndex()
      {
        return _lastIndex;
      }

      void execute(Message& message_)
      {
        if (_entries > 0 && _errorCount == 0)
        {
          try
          {
            {
              _pStore->store(message_, false);
            }
            _lastIndex = amps_message_get_field_uint64(
                           message_.getMessage(),
                           AMPS_Sequence);
          }
          catch (...)
          {
            ++_errorCount;
          }
          --_entries;
        }
      }
    };

  public:
    ///
    /// Create a HybridPublishStore that will use fileName_ as its file
    /// storage and stores at most maxMemoryCapacity_ messages in memory
    /// before offloading some messages to the file.
    /// \param fileName_ The name to use for the file-based storage.
    /// \param maxMemoryCapacity_ The maximum number of messages to store in
    /// in memory before starting to use a file.
    /// \param errorOnPublishGap_ If true, PublishStoreGapException can be
    /// thrown by the store if the client logs onto a server that appears
    /// to be missing messages no longer held in the store.
    HybridPublishStore(const char* fileName_, size_t maxMemoryCapacity_,
                       bool errorOnPublishGap_ = false)
      : StoreImpl(errorOnPublishGap_)
      , _memStore(maxMemoryCapacity_, errorOnPublishGap_)
      , _fileStore(fileName_, errorOnPublishGap_)
      , _cap(maxMemoryCapacity_)
      , _lowWatermark((size_t)((double)maxMemoryCapacity_ * 0.5))
      , _lowestIndexInMemory(0)
      , _holdSwapping(false)
    {
      _handlerData._store = this;
      _memStore.addRef();
      _fileStore.addRef();
    }

    ///
    /// Create a HybridPublishStore that will use fileName_ as its file
    /// storage and stores at most maxMemoryCapacity_ messages in memory
    /// before offloading some messages to the file.
    /// \param fileName_ The name to use for the file-based storage.
    /// \param maxMemoryCapacity_ The maximum number of messages to store in
    /// in memory before starting to use a file.
    /// \param errorOnPublishGap_ If true, PublishStoreGapException can be
    /// thrown by the store if the client logs onto a server that appears
    /// to be missing messages no longer held in the store.
    HybridPublishStore(const std::string& fileName_, size_t maxMemoryCapacity_,
                       bool errorOnPublishGap_ = false)
      : StoreImpl(errorOnPublishGap_)
      , _memStore(maxMemoryCapacity_, errorOnPublishGap_)
      , _fileStore(fileName_, errorOnPublishGap_)
      , _cap(maxMemoryCapacity_)
      , _lowWatermark((size_t)((double)maxMemoryCapacity_ * 0.5))
      , _lowestIndexInMemory(0)
      , _holdSwapping(false)
    {
      _handlerData._store = this;
      _memStore.addRef();
      _fileStore.addRef();
    }

    ///
    /// Set how many messags remain in memory after messages get offlined.
    /// When memory storage reaches its cap, it will write its oldest messages
    /// to its file until it is holding only lowWatermark_ messages.
    /// \param lowWatermark_ The number of messages that remain in memory after
    /// offlining completes.
    void setLowWatermark(size_t lowWatermark_)
    {
      Lock<Mutex> guard(_lock);
      _lowWatermark = lowWatermark_;
    }

    ///
    /// Get how many messags remain in memory after messages get offlined.
    /// When memory storage reaches its cap, it will write its oldest messages
    /// to its file until it is holding only lowWatermark_ messages.
    /// \return The number of messages that remain in memory after
    /// offlining completes.
    size_t getLowWatermark()
    {
      Lock<Mutex> guard(_lock);
      return _lowWatermark;
    }

    ///
    /// Discard all messages in the store up to and including index_.
    /// \param index_ The maximum index to remove from storage.
    void discardUpTo(amps_uint64_t index_)
    {
      Lock<Mutex> guard(_lock);
      while (_holdSwapping)
      {
        if (!_lock.wait(1000))
        {
          Unlock<Mutex> u(_lock);
          amps_invoke_waiting_function();
        }
      }
      // Set _holdSwapping true to end of function
      FlagFlip flip(&_holdSwapping);
      {
        Unlock<Mutex> u(_lock);
        if (!index_)
        {
          _memStore.discardUpTo(_fileStore.getLastPersisted());
          Lock<Mutex> l(_lock);
          _lock.signalAll();
          return;
        }
        _fileStore.discardUpTo(index_);
        if (_lowestIndexInMemory <= index_)
        {
          _memStore.discardUpTo(index_);
          _lowestIndexInMemory = index_ + 1;
        }
      }
      _lock.signalAll();
    }

    ///
    /// Used internally by Client to replay messages in the store to AMPS
    /// after a successful connection.
    /// \param replayer_ The StoreReplayer that replays the messages.
    void replay(StoreReplayer& replayer_)
    {
      Lock<Mutex> guard(_lock);
      while (_holdSwapping)
      {
        if (!_lock.wait(1000))
        {
          amps_invoke_waiting_function();
        }
      }
      // Set _holdSwapping true to end of function
      FlagFlip flip(&_holdSwapping);
      {
        Unlock<Mutex> u(_lock);
        _fileStore.replay(replayer_);
        _memStore.replay(replayer_);
      }
      _lock.signalAll();
    }

    /// The number of messages in the Store that have not been discarded.
    /// \return The number of messages still in the Store.
    size_t unpersistedCount() const
    {
      return _fileStore.unpersistedCount() + _memStore.unpersistedCount();
    }

    ///
    /// Method to wait for the Store to discard everything that has been
    /// stored up to the point in time when flush is called. It will get
    /// the current max and wait up to timeout for that message to be discarded
    /// \param timeout_ The number of milliseconds to wait.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException The publish command was not acked in the allowed time.
    virtual void flush(long timeout_)
    {
      Lock<Mutex> guard(_lock);
      amps_uint64_t waitFor = _getHybridHighestUnpersisted();
      amps_uint64_t unset = getUnsetSequence();
      // Check that we aren't already empty
      if (waitFor == unset)
      {
        return;
      }
      if (timeout_ > 0)
      {
        bool timedOut = false;
        long waitTime = (timeout_ < 1000) ? timeout_ : 1000;
        AMPS_START_TIMER(timeout_)
        // While timeout hasn't expired and we haven't had everything acked
        while (!timedOut && waitFor >= _getHybridLowestUnpersisted() &&
               _getHybridLowestUnpersisted() != unset)
        {
          if (!_lock.wait(waitTime))
          {
            // May have woken up early, check real time
            AMPS_RESET_TIMER(timedOut, timeout_);
            waitTime = (timeout_ < 1000) ? timeout_ : 1000;
            Unlock<Mutex> unlck(_lock);
            amps_invoke_waiting_function();
          }
        }
        // If we timed out and still haven't caught up with the acks
        if (timedOut && waitFor >= _getHybridLowestUnpersisted() &&
            _getHybridLowestUnpersisted() != unset)
        {
          throw TimedOutException("Timed out waiting to flush publish store.");
        }
      }
      else
      {
        while (waitFor >= _getHybridLowestUnpersisted() &&
               _getHybridLowestUnpersisted() != unset)
        {
          // Use timeout version so python can interrupt
          _lock.wait(1000);
          Unlock<Mutex> unlck(_lock);
          amps_invoke_waiting_function();
        }
      }
    }

    bool replaySingle(StoreReplayer& replayer_, amps_uint64_t index_)
    {
      amps_uint64_t lowestIndexInMemory;
      {
        Lock<Mutex> guard(_lock);
        lowestIndexInMemory = _lowestIndexInMemory;
      }
      if (index_ < lowestIndexInMemory)
      {
        return _fileStore.replaySingle(replayer_, index_);
      }
      else
      {
        return _memStore.replaySingle(replayer_, index_);
      }
    }

    ///
    /// Used internally by Client to put messages into the Store.
    /// \param message_ The Message to be stored.
    amps_uint64_t store(const Message& message_)
    {
      Lock<Mutex> guard(_lock);
      while (_holdSwapping)
      {
        if (!_lock.wait(1000))
        {
          Unlock<Mutex> u(_lock);
          amps_invoke_waiting_function();
        }
      }
      if (_memStore.unpersistedCount() >= _cap && !_holdSwapping)
      {
        // Set _holdSwapping true to end of function
        FlagFlip flip(&_holdSwapping);
        SwappingOutReplayer swapper(&_fileStore,
                                    _memStore.unpersistedCount() - _lowWatermark);
        {
          Unlock<Mutex> u(_lock);
          _memStore.replay(swapper);
        }
        _lock.signalAll();
        if (swapper.getErrors() == 0)
        {
          _lowestIndexInMemory = swapper.lastIndex();
          _memStore.discardUpTo(_lowestIndexInMemory++);
        }
      }
      return _memStore.store(message_);
    }

    void setResizeHandler(PublishStoreResizeHandler handler_, void* data_)
    {
      _handlerData.init(handler_, data_);
      _fileStore.setResizeHandler(HybridPublishStore::resizeHandler,
                                  (void*)&_handlerData);
    }

    inline virtual PublishStoreResizeHandler getResizeHandler() const
    {
      return _handlerData._handler;
    }

    amps_uint64_t getLowestUnpersisted() const
    {
      Lock<Mutex> guard(_lock);
      return _getHybridLowestUnpersisted();
    }

    amps_uint64_t getHighestUnpersisted() const
    {
      Lock<Mutex> guard(_lock);
      return _getHybridHighestUnpersisted();
    }

    amps_uint64_t getLastPersisted(void)
    {
      Lock<Mutex> guard(_lock);
      return _getHybridLastPersisted();
    }

    inline virtual void setErrorOnPublishGap(bool errorOnPublishGap_)
    {
      StoreImpl::setErrorOnPublishGap(errorOnPublishGap_);
      _memStore.setErrorOnPublishGap(errorOnPublishGap_);
      _fileStore.setErrorOnPublishGap(errorOnPublishGap_);
    }

  private:

    // Resize handlers are invoked with Store not const Store&
    static bool resizeHandler(Store store_, size_t size_, void* data_) // -V813
    {
      HandlerData* handlerData = (HandlerData*)data_;
      //Unlock<Mutex> hybridUnlock(handlerData->_store->_lock);
      return handlerData->_handler(store_, size_, handlerData->_data);
    }

    // Lock should be held
    amps_uint64_t _getHybridLowestUnpersisted() const
    {
      amps_uint64_t filemin = _fileStore.getLowestUnpersisted();
      amps_uint64_t memmin = _memStore.getLowestUnpersisted();
      if (filemin == AMPS_UNSET_SEQUENCE)
      {
        return memmin;
      }
      if (memmin == AMPS_UNSET_SEQUENCE || memmin > filemin)
      {
        return filemin;
      }
      // Only left with memmin <= filemin
      return memmin;
    }

    // Lock should be held
    amps_uint64_t _getHybridHighestUnpersisted() const
    {
      amps_uint64_t filemax = _fileStore.getHighestUnpersisted();
      amps_uint64_t memmax = _memStore.getHighestUnpersisted();
      if (filemax == AMPS_UNSET_SEQUENCE)
      {
        return memmax;
      }
      if (memmax == AMPS_UNSET_SEQUENCE || memmax < filemax)
      {
        return filemax;
      }
      // Only left with memmax >= filemax
      return memmax;
    }

    amps_uint64_t _getHybridLastPersisted()
    {
      // If we've never swapped and nothing is in file
      if (!_lowestIndexInMemory &&
          _fileStore.unpersistedCount() == 0)
      {
        _fileStore.discardUpTo(_memStore.getLastPersisted());
        return _fileStore.getLastPersisted();
      }
      amps_uint64_t memLast = _memStore.getLastPersisted();
      amps_uint64_t fileLast = _fileStore.getLastPersisted();
      return (memLast < fileLast) ? memLast : fileLast;
    }

    MemoryPublishStore _memStore;
    PublishStore _fileStore;
    size_t _cap;
    size_t _lowWatermark;
    amps_uint64_t _lowestIndexInMemory;
    mutable Mutex  _lock;
    HandlerData _handlerData;
#if __cplusplus >= 201100L || _MSC_VER >= 1900
    std::atomic<bool> _holdSwapping;
#else
    volatile bool _holdSwapping;
#endif

  };//end HybridPublishStore

}//end namespace AMPS

#endif //_HYBRIDPUBLISHSTORE_H_

