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

#ifndef _RECOVERYPOINTADAPTER_H_
#define _RECOVERYPOINTADAPTER_H_

#include <amps/Field.hpp>
#include <amps/RecoveryPoint.hpp>
#include <vector>
#include <iterator>
#include <memory>
#include <thread>
#include <unordered_map>
#if __cplusplus >= 201100L || _MSC_VER >= 1900
#include <atomic>
#endif

/// \file RecoveryPointAdapter.hpp
/// \brief Provides AMPS::RecoveryPointAdapter, an iterface for implementing
/// external storage of bookmark subscription recovery data
/// and for recovering from that external storage.

namespace AMPS
{

  class RecoveryPointAdapter;

/// RecoveryPointAdapterImpl virtual base class for implementing external
/// storage of subscription recovery points and recovery from storage.
  class RecoveryPointAdapterImpl : public RefBody
  {
  public:
    virtual ~RecoveryPointAdapterImpl() { }
    /// Recovery is done by iteration over elements in storage. This function
    /// modifies the passed in argument to be the next stored RecoveryPoint
    /// or an empty RecoveryPoint to indicate completion.
    /// \param current_ The RecoveryPoint to set as the next recovery item.
    virtual bool next(RecoveryPoint& current_) = 0;
    /// Update the storage information with the given recovery point.
    /// \param recoveryPoint_ The new/updated RecoveryPoint to save.
    virtual void update(RecoveryPoint& recoveryPoint_) = 0;
    /// Remove all data from the storage
    virtual void purge() = 0;
    /// Remove the specified subId_ from the storage
    /// \param subId_ The sub id to remove
    virtual void purge(const Field& subId_) = 0;
    /// Take any necessary actions to close the associated storage.
    virtual void close() = 0;
    /// Take any necessary actions to reduce associated storage size.
    virtual void prune() { ; }
  };

/// RecoveryPointAdapter a handle class for implementing external
/// storage of subscription recovery points and recovery from storage.
  class RecoveryPointAdapter // -V690
  {
  public:
    class iterator
    {
      RecoveryPointAdapterImpl* _pAdapter;
      RecoveryPoint _current;
      inline void advance()
      {
        if (!_pAdapter || !_pAdapter->next(_current))
        {
          _pAdapter = NULL;
        }
      }

    public:
      iterator() // end
        : _pAdapter(NULL)
      {;}
      iterator(RecoveryPointAdapterImpl* pAdapter_)
        : _pAdapter(pAdapter_)
      {
        advance();
      }

      bool operator==(const iterator& rhs) const
      {
        return _pAdapter == rhs._pAdapter;
      }
      bool operator!=(const iterator& rhs) const
      {
        return _pAdapter != rhs._pAdapter;
      }
      void operator++(void)
      {
        advance();
      }
      RecoveryPoint operator*(void)
      {
        return _current;
      }
      RecoveryPoint* operator->(void)
      {
        return &_current;
      }
    };

    RecoveryPointAdapter() : _body() { }
    RecoveryPointAdapter(RecoveryPointAdapterImpl* body_, bool isRef_ = true)
      : _body(body_, isRef_) { }
    RecoveryPointAdapter(const RecoveryPointAdapter& rhs_)
      : _body(rhs_._body)
    { }

    /// To recover from an adapter, iterate over the adapter from begin() to
    /// end() with a RecoveryPointIterator. Calling *iter should yield a
    /// RecoveryPoint&.
    /// Begin recovery from storage and return an iterator to the first
    /// RecoveryPoint or end() if there are none.
    /// \return The iterator for the first RecoveryPoint or end() if empty.
    iterator begin()
    {
      return iterator(&(_body.get()));
    }

    /// Return the end of recovery marker.
    /// \return The iterator marking the end of recovery.
    iterator end()
    {
      return iterator();
    }

    /// Update the storage information with the given recovery point.
    /// \param recoveryPoint_ The new/updated RecoveryPoint to save.
    void update(RecoveryPoint& recoveryPoint_)
    {
      _body.get().update(recoveryPoint_);
    }

    /// Remove all data from the storage
    void purge()
    {
      _body.get().purge();
    }

    /// Remove the specified subId_ from the storage
    /// \param subId_ The sub id to remove
    void purge(const Field& subId_)
    {
      _body.get().purge(subId_);
    }

    /// Take any necessary actions to close the associated storage.
    void close()
    {
      _body.get().close();
    }

    /// Take any necessary actions to close the associated storage.
    void prune()
    {
      _body.get().prune();
    }

    /// Return if this has a valid implementation.
    bool isValid() const
    {
      return _body.isValid();
    }
  private:
    BorrowRefHandle<RecoveryPointAdapterImpl> _body;
  };

/// RecoveryPointAdapter implementation that delegates storage to another
/// RecoveryPointAdapter but provides conflation so that only every X updates
/// are saved and/or only the last update every Y milliseconds is saved.
  class ConflatingRecoveryPointAdapter : public RecoveryPointAdapterImpl
  {
  public:
    /// Conflate updates to delegate_ where they will only be processed
    /// every updateIntervalMillis_ for subscriptions that have been updated
    /// at least updateThreshold_ times or haven't been updated for at least
    /// timeoutMillis_ milliseconds.
    /// \param delegate_ A shared pointer to the underlying delegate
    /// RecoveryPointAdapter.
    /// \param updateThreshold_ The minimum number of updates for a given subId
    /// before a conflated update is delivered. Setting to 1 will prevent
    /// conflation from occurring.
    /// \param timeoutMillis_ The maximum amount of time between conflated
    /// updates for a given subId. Setting to 0 will force all updates to be
    /// sent any time one update needs to be sent or every updateInterval.
    /// \param updateIntervalMillis_ The amount of time the update thread can
    /// sit idle between sending conflated updates and checking for timeouts.
    ConflatingRecoveryPointAdapter(
      const std::shared_ptr<RecoveryPointAdapterImpl>& delegate_,
      unsigned updateThreshold_ = 10,
      double timeoutMillis_ = 2000.0,
      long updateIntervalMillis_ = 2000
    )
      : _delegate(delegate_)
      , _updateThreshold(updateThreshold_)
      , _timeoutMillis(timeoutMillis_)
      , _updateIntervalMillis(updateIntervalMillis_)
      , _closed(false)
      , _updateAll(false)
    {
      // Start the update thread
      _thread = std::thread(&ConflatingRecoveryPointAdapter::updateThread,
                            this);
    }

    virtual ~ConflatingRecoveryPointAdapter()
    {
      _close();
      _thread.join();
      for (UpdateIter purged = _latestUpdates.begin();
           purged != _latestUpdates.end(); ++purged)
      {
        Field clearableSubId = purged->first;
        purged->second.clear();
        clearableSubId.clear();
      }
    }

    /// Recovery is done by iteration over elements in storage. This function
    /// modifies the passed in argument to be the next stored RecoveryPoint
    /// or an empty RecoveryPoint to indicate completion.
    /// \param current_ The RecoveryPoint to set as the next recovery item.
    virtual bool next(RecoveryPoint& current_)
    {
      return _delegate->next(current_);
    }

    /// Update the storage information with the given recovery point.
    /// \param recoveryPoint_ The new/updated RecoveryPoint to save.
    virtual void update(RecoveryPoint& recoveryPoint_)
    {
      Field subId = recoveryPoint_.getSubId();
      Lock<Mutex> lock(_lock);
      UpdateIter lastUpdate = _latestUpdates.find(subId);
      if (lastUpdate == _latestUpdates.end())
      {
        // New sub id, use deep copies and a new Timer.
        subId = subId.deepCopy();
        _latestUpdates[subId] = recoveryPoint_.deepCopy();
        _counts[subId] = 1;
        if (_timeoutMillis != 0.0) // -V550
        {
          Timer timer(_timeoutMillis);
          timer.start();
          _timers[subId] = timer;
        }
      }
      else
      {
        // SubId already exists, set to new recovery point.
        lastUpdate->second.deepCopy(recoveryPoint_);
        // Increment and check the count.
        if (++_counts[subId] >= _updateThreshold)
        {
          // Time to update, make sure update thread wakes up.
          _lock.signalAll();
        }
      }
    }

    /// Remove all data from the storage.
    virtual void purge()
    {
      _delegate->purge();
      Lock<Mutex> lock(_lock);
      _counts.clear();
      _timers.clear();
      for (UpdateIter purged = _latestUpdates.begin();
           purged != _latestUpdates.end(); ++purged)
      {
        Field clearableSubId = purged->first;
        purged->second.clear();
        clearableSubId.clear();
      }
      _latestUpdates.clear();
    }

    /// Remove the specified subId_ from the storage.
    /// \param subId_ The sub id to remove.
    virtual void purge(const Field& subId_)
    {
      _delegate->purge(subId_);
      Lock<Mutex> lock(_lock);
      UpdateIter purged = _latestUpdates.find(subId_);
      if (purged != _latestUpdates.end())
      {
        Field clearableSubId = purged->first;
        purged->second.clear();
        _latestUpdates.erase(purged);
        _counts.erase(subId_);
        _timers.erase(subId_);
        clearableSubId.clear();
      }
    }

    /// Take any necessary actions to close the associated storage.
    virtual void close()
    {
      _close();
    }

    /// Push all updates to underlying adapter.
    virtual void updateAll()
    {
      Lock<Mutex> lock(_lock);
      _runUpdateAll();
    }

    /// Lock is already held
    virtual void _runUpdateAll()
    {
      _updateAll = true;
      while (!_counts.empty())
      {
        _lock.signalAll();
        _lock.wait(250);
      }
    }

  protected:
    void _close()
    {
      // Save all cached updates before shutting down update thread.
      if (!_closed)
      {
        Lock<Mutex> lock(_lock);
        _runUpdateAll();
        _closed = true;
        _lock.signalAll();
      }
      _delegate->close();
    }
    void updateThread()
    {
      // A place to hold updates to save
      std::vector<SavedUpdate> _queuedUpdates;
      while (!_closed)
      {
        DeferLock<Mutex> lock(_lock);
        lock.lock();
        // Wait for a signal or update interval
        _lock.wait(_updateIntervalMillis);

        // Check for timeouts
        for (TimerMap::iterator timer = _timers.begin();
             timer != _timers.end(); )
        {
          if (timer->second.check())
          {
            UpdateIter update = _latestUpdates.find(timer->first);
            if (update != _latestUpdates.end())
            {
              // Remove subId from all, clear of subId will
              // occur after save.
              _queuedUpdates.push_back(*update);
              _counts.erase(update->first);
              timer = _timers.erase(timer);
              _latestUpdates.erase(update);
            }
            else
            {
              ++timer;
            }
          }
          else
          {
            ++timer;
          }
        }

        // Need a local version so it doesn't change after we unlock to
        // deliver updates.
        bool updateAll = (bool)_updateAll;
        // Check for update counts
        for (CountMap::iterator count = _counts.begin();
             count != _counts.end(); )
        {
          if (updateAll || _timeoutMillis == 0.0 // -V550
              || count->second >= _updateThreshold)
          {
            UpdateIter update = _latestUpdates.find(count->first);
            if (update != _latestUpdates.end())
            {
              // Remove subId from all, clear of subId will
              // occur after save.
              _queuedUpdates.push_back(*update);
              count = _counts.erase(count);
              _timers.erase(update->first);
              _latestUpdates.erase(update);
            }
            else
            {
              ++count;
            }
          }
          else
          {
            ++count;
          }
        }
        // Release the lock unless we're doing an update all, then we
        // hold it until updates are completed and signal when done.
        if (!updateAll)
        {
          lock.unlock();
        }
        // Shouldn't need the lock to send the updates
        for (std::vector<SavedUpdate>::iterator update = _queuedUpdates.begin(), end = _queuedUpdates.end(); update != end; ++update)
        {
          _delegate->update(update->second);
          Field clearableSubId(update->first);
          clearableSubId.clear();
          update->second.clear();
        }
        _queuedUpdates.clear();
        if (updateAll)
        {
          _updateAll = false;
          _lock.signalAll();
        }
      } // -V1020
    }

    // The adapter doing the real saves
    std::shared_ptr<RecoveryPointAdapterImpl> _delegate;

    // Lock used to protect _latestUpdates, _timers, and _counts.
    Mutex                                     _lock;

    // Types for our maps
    typedef std::unordered_map<Field, RecoveryPoint, Field::FieldHash> UpdateMap;
    typedef std::pair<Field, RecoveryPoint>                       SavedUpdate;
    typedef UpdateMap::value_type                                 Update;
    typedef UpdateMap::iterator                                   UpdateIter;
    typedef std::unordered_map<Field, Timer, Field::FieldHash>    TimerMap;
    typedef TimerMap::iterator                                    TimerIter;
    typedef std::unordered_map<Field, unsigned, Field::FieldHash> CountMap;
    typedef CountMap::iterator                                    CountIter;

    // Saves the most recent update for each sub id.
    UpdateMap                                 _latestUpdates;

    // Saves a timer for each sub id that is reset each time we save.
    TimerMap                                  _timers;

    // Saves a count of how many updates have come in since last save.
    CountMap                                  _counts;

    // The thread doing the saves.
    std::thread                               _thread;

    // How many updates before we force a save.
    unsigned                                  _updateThreshold;

    // How long between getting first cached update and save.
    double                                    _timeoutMillis;

    // How long between automatic checks of the timers.
    long                                      _updateIntervalMillis;

#if __cplusplus >= 201100L || _MSC_VER >= 1900
    // The update thread runs until this is true.
    std::atomic<bool>                         _closed;

    // Flag to tell update thread to save everything.
    std::atomic<bool>                         _updateAll;
#else
    // The update thread runs until this is true.
    volatile bool                             _closed;

    // Flag to tell update thread to save everything.
    volatile bool                             _updateAll;
#endif
  };

}

#endif //_RECOVERYPOINTADAPTER_H_

