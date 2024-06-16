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

#ifndef _MEMORYBOOKMARKSTORE_H_
#define _MEMORYBOOKMARKSTORE_H_

#include <amps/BookmarkStore.hpp>
#include <amps/Field.hpp>
#include <amps/Message.hpp>
#include <amps/RecoveryPoint.hpp>
#include <amps/RecoveryPointAdapter.hpp>
#include <map>
#include <sstream>
#include <vector>
#include <stdlib.h>
#include <assert.h>

#define AMPS_MIN_BOOKMARK_LEN 3
#define AMPS_INITIAL_MEMORY_BOOKMARK_SIZE   16384UL

/// \file MemoryBookmarkStore.hpp
/// \brief Provides AMPS::MemoryBookmarkStore, a bookmark store that holds
/// bookmarks in memory. This bookmark store protects against connectivity
/// failures.

namespace AMPS
{

///
/// A BookmarkStoreImpl implementation that stores bookmarks in memory. This
/// store protects against connectivity failures. This store does not
/// persist the bookmark store, so this store cannot be used to resume
/// subscriptions after application restart.
  class MemoryBookmarkStore : public BookmarkStoreImpl
  {
  protected:
    class Subscription
    {
    public:
      typedef std::map<Message::Field, size_t, Message::Field::FieldHash> RecoveryMap;
      typedef std::map<amps_uint64_t, amps_uint64_t> PublisherMap;
      typedef std::map<Message::Field, size_t, Message::Field::FieldHash>::iterator
      RecoveryIterator;
      typedef std::map<amps_uint64_t, amps_uint64_t>::iterator PublisherIterator;

      // Start sequence at 1 so that 0 can be used during file subclasses
      // recovery as an indicator that a message wasn't logged because
      // isDiscarded() was true.
      Subscription(MemoryBookmarkStore* store_, const Message::Field& id_)
        : _current(1), _currentBase(0), _least(1), _leastBase(0)
        , _recoveryMin(AMPS_UNSET_INDEX), _recoveryBase(AMPS_UNSET_INDEX)
        , _recoveryMax(AMPS_UNSET_INDEX), _recoveryMaxBase(AMPS_UNSET_INDEX)
        , _entriesLength(AMPS_INITIAL_MEMORY_BOOKMARK_SIZE), _entries(NULL)
        , _store(store_)
      {
        // Need our own memory for the sub id
        _id.deepCopy(id_);
        _store->resize(_id, (char**)&_entries,
                       sizeof(Entry)*AMPS_INITIAL_MEMORY_BOOKMARK_SIZE, false);
        setLastPersistedToEpoch();
      }

      ~Subscription()
      {
        Lock<Mutex> guard(_subLock);
        if (_entries)
        {
          for (size_t i = 0; i < _entriesLength; ++i)
          {
            _entries[i]._val.clear();
          }
          // resize to 0 will free _entries
          _store->resize(_id, (char**)&_entries, 0);
        }
        _id.clear();
        _recent.clear();
        _lastPersisted.clear();
        _recentList.clear();
        _range.clear();
        _recoveryTimestamp.clear();
      }

      size_t log(const Message::Field& bookmark_)
      {
        if (bookmark_ == AMPS_BOOKMARK_NOW)
        {
          return 0;
        }
        Lock<Mutex> guard(_subLock);
        // Either relog the recovery or log it
        size_t index = recover(bookmark_, true);
        if (index == AMPS_UNSET_INDEX)
        {
          // Check for wrap
          if (_current >= _entriesLength)
          {
            _current = 0;
            _currentBase += _entriesLength;
          }
          // Check for resize
          // If list is too small, double it
          if ((_current == _least && _leastBase < _currentBase) ||
              (_current == _recoveryMin && _recoveryBase < _currentBase))
          {
            if (!_store->resize(_id, (char**)&_entries,
                                sizeof(Entry) * _entriesLength * 2))
            {
              //Try again
              return log(bookmark_);
            }
            // Length was doubled
            _entriesLength *= 2;
          }

          // Add this entry to the end of our list
          /*
          if (bookmark_ == AMPS_BOOKMARK_NOW)
          {
              // Save a now timestamp bookmark
              char* nowTimestamp = new char[AMPS_TIMESTAMP_LEN];
              struct tm timeInfo;
              time_t now;
              time(&now);
          #ifdef _WIN32
              gmtime_s(&timeInfo, &now);
          #else
              gmtime_r(&now, &timeInfo);
          #endif
              strftime(nowTimestamp, AMPS_TIMESTAMP_LEN,
                       "%Y%m%dT%H%M%S", &timeInfo);
              nowTimestamp[AMPS_TIMESTAMP_LEN-1] = 'Z';
              _entries[_current]._val.assign(nowTimestamp,
                                             AMPS_TIMESTAMP_LEN);
              _entries[_current]._active = false;
              index = _current++;
              return index + _currentBase;
          }
          else
          */
          {
            // Is this an attempt at a range?
            if (!BookmarkRange::isRange(bookmark_))
            {
              _entries[_current]._val.deepCopy(bookmark_);
            }
            else
            {
              // Deep copy of the range is saved
              _range.set(bookmark_);
              // Stricter check on range syntax
              if (!_range.isValid())
              {
                throw CommandException("Invalid bookmark range specified.");
              }
              _store->updateAdapter(this);
              if (!_range.isStartInclusive())
              {
                // Put it in our publishers map
                amps_uint64_t publisher, sequence;
                std::vector<Field> bmList = Field::parseBookmarkList(_range.getStart());
                for (std::vector<Field>::iterator bkmk = bmList.begin(); bkmk != bmList.end(); ++bkmk)
                {
                  parseBookmark(*bkmk, publisher, sequence);
                  if (publisher != (amps_uint64_t)0)
                  {
                    // Compare it to our publishers map
                    PublisherIterator pub = _publishers.find(publisher);
                    if (pub == _publishers.end() || pub->second < sequence)
                    {
                      _publishers[publisher] = sequence;
                    }
                  }
                }
              }
              // Don't actually log a range
              return 0;
            }
          }
          _entries[_current]._active = true;
          index = _current++;
        }
        return index + _currentBase;
      }

      bool discard(size_t index_)
      {
        Lock<Mutex> guard(_subLock);
        return _discard(index_);
      }

      bool discard(const Message::Field& bookmark_)
      {
        // These are discarded when logged or not logged
        if (bookmark_ == AMPS_BOOKMARK_NOW)
        {
          return false;
        }
        Lock<Mutex> guard(_subLock);
        size_t search = _least;
        size_t searchBase = _leastBase;
        size_t searchMax = _current;
        size_t searchMaxBase = _currentBase;
        if (_least + _leastBase == _current + _currentBase)
        {
          if (_recoveryMin != AMPS_UNSET_INDEX)
          {
            search = _recoveryMin;
            searchBase = _recoveryBase;
            searchMax = _recoveryMax;
            searchMaxBase = _recoveryMaxBase;
          }
          else // Store is empty, so nothing to do
          {
            return false;
          }
        }
        assert(searchMax != AMPS_UNSET_INDEX);
        assert(searchMaxBase != AMPS_UNSET_INDEX);
        assert(search != AMPS_UNSET_INDEX);
        assert(searchBase != AMPS_UNSET_INDEX);
        // Search while we don't find the provided bookmark and we're in valid range
        while (search + searchBase < searchMax + searchMaxBase)
        {
          if (_entries[search]._val == bookmark_)
          {
            return _discard(search + searchBase);
          }
          if (++search == _entriesLength)
          {
            // Least has now loooped around
            searchBase += _entriesLength;
            search = 0;
          }
        }
        return false;
      }

      // Get sequence number from a Field that is a bookmark
      static void parseBookmark(const Message::Field& field_,
                                amps_uint64_t& publisherId_,
                                amps_uint64_t& sequenceNumber_)
      {
        Message::Field::parseBookmark(field_, publisherId_, sequenceNumber_);
      }

      // Check to see if this message is older than the most recent one seen,
      // and if it is, check if it discarded.
      bool isDiscarded(const Message::Field& bookmark_)
      {
        Lock<Mutex> guard(_subLock);
        if (BookmarkRange::isRange(bookmark_))
        {
          return false;
        }
        // Check if we've already recovered this bookmark
        size_t recoveredIdx = recover(bookmark_, false);

        amps_uint64_t publisher, sequence;
        parseBookmark(bookmark_, publisher, sequence);
        // Compare it to our publishers map
        PublisherIterator pub = _publishers.find(publisher);
        if (pub == _publishers.end() || pub->second < sequence)
        {
          _publishers[publisher] = sequence;
          if (recoveredIdx == AMPS_UNSET_INDEX)
          {
            return false;
          }
        }
        if (recoveredIdx != AMPS_UNSET_INDEX)
        {
          if (!_entries[recoveredIdx]._active)
          {
            _recovered.erase(bookmark_);
            return true;
          }
          return false;
        }
        // During recovery, we don't really care if it's been discarded
        // or not. We just want _publishers updated. No need for the
        // costly linear search.
        if (_store->_recovering)
        {
          return false;
        }
        // During failure and recovery scenarios, we'll see out of order
        // bookmarks arrive, either because (a) we're replaying or (b)
        // a publisher has cut over, and we've cut over to a new server.
        // Scan the list to see if we have a match.
        size_t base = _leastBase;
        for (size_t i = _least; i + base < _current + _currentBase; i++)
        {
          if ( i >= _entriesLength )
          {
            i = 0;
            base = _currentBase;
          }
          if (_entries[i]._val == bookmark_)
          {
            return !_entries[i]._active;
          }
        }

        return true; // message is totally discarded
      }

      bool empty(void) const
      {
        if (_least == AMPS_UNSET_INDEX ||
            ((_least + _leastBase) == (_current + _currentBase) &&
             _recoveryMin == AMPS_UNSET_INDEX))
        {
          return true;
        }
        return false;
      }

      void updateMostRecent()
      {
        Lock<Mutex> guard(_subLock);
        _updateMostRecent();
      }

      const BookmarkRange& getRange() const
      {
        return _range;
      }

      Message::Field getMostRecentList(bool usePublishersList_ = true)
      {
        Lock<Mutex> guard(_subLock);
        bool useLastPersisted = !_lastPersisted.empty() &&
                                _lastPersisted.len() > 1;
        // when this is called, we'll take a moment to update the list
        // of things recovered,
        // so we don't accidentally log anything we ought not to.
        _updateMostRecent();
        bool useRecent = !_recent.empty() && _recent.len() > 1;
        amps_uint64_t lastPublisher = 0;
        amps_uint64_t lastSeq = 0;
        amps_uint64_t recentPublisher = 0;
        amps_uint64_t recentSeq = 0;
        if (useLastPersisted)
        {
          parseBookmark(_lastPersisted, lastPublisher, lastSeq);
        }
        if (useRecent)
        {
          parseBookmark(_recent, recentPublisher, recentSeq);
          if (empty() && useLastPersisted)
          {
            useRecent = false;
          }
          else
          {
            if (useLastPersisted && lastPublisher == recentPublisher)
            {
              if (lastSeq <= recentSeq)
              {
                useRecent = false;
              }
              else
              {
                useLastPersisted = false;
              }
            }
          }
        }
        // Set size for all bookmarks that will be used
        size_t totalLen = (useLastPersisted ? _lastPersisted.len() + 1 : 0);
        if (useRecent)
        {
          totalLen += _recent.len() + 1;
        }
        // If we don't have a non-EPOCH persisted ack and we don't have a
        // non-EPOCH most recent bookmark, OR we have a range
        // we can build a list based on all the publishers instead.
        if (usePublishersList_
            && ((!useLastPersisted && !useRecent)
                || _lastPersisted == AMPS_BOOKMARK_EPOCH))
        {
          std::ostringstream os;
          for (PublisherIterator pub = _publishers.begin();
               pub != _publishers.end(); ++pub)
          {
            if (pub->first == 0 && pub->second == 0)
            {
              continue;
            }
            if (pub->first == recentPublisher && recentSeq < pub->second)
            {
              os << recentPublisher << '|' << recentSeq << "|,";
            }
            else
            {
              os << pub->first << '|' << pub->second << "|,";
            }
          }
          std::string recent = os.str();
          totalLen = recent.length();
          if (!recent.empty())
          {
            if (!_recoveryTimestamp.empty())
            {
              totalLen += _recoveryTimestamp.len();
              recent += std::string(_recoveryTimestamp);
            }
            else
            {
              // Remove trailing ,
              recent.erase(--totalLen);
            }
            // Reset _recentList to new value and return it
            _recentList.clear();
            _recentList = Message::Field(recent).deepCopy();
            if (_range.isValid())
            {
              if (_range.getStart() != recent
                  && _recentList != AMPS_BOOKMARK_EPOCH)
              {
                _range.replaceStart(_recentList, true);
              }
              else if (_range.isStartInclusive())
              {
                amps_uint64_t publisher, sequence;
                parseBookmark(_range.getStart(), publisher,
                              sequence);
                PublisherIterator pub = _publishers.find(publisher);
                if (pub != _publishers.end()
                    && pub->second >= sequence)
                {
                  _range.makeStartExclusive();
                }
              }
              return _range;
            }
            return _recentList;
          }
          if (_range.isValid())
          {
            return _range;
          }
        }
        if (!_recoveryTimestamp.empty() && !_range.isValid())
        {
          totalLen += _recoveryTimestamp.len() + 1;
        }
        // If we have nothing discarded, return EPOCH
        if (totalLen == 0
            || (_recent.len() < 2 && !empty()))
        {
          if (_range.isValid())
          {
            return _range;
          }
          if (!useRecent)
          {
            return Message::Field(AMPS_BOOKMARK_EPOCH);
          }
          _setLastPersistedToEpoch();
          return _lastPersisted;
        }
        // Remove the trailing , from the length
        totalLen -= 1;
        char* field = new char[totalLen];
        size_t len = 0;
        if (useRecent)
        {
          len = _recent.len();
          memcpy(field, _recent.data(), len);
          if (len < totalLen)
          {
            field[len++] = ',';
          }
        }
        if (useLastPersisted)
        {
          memcpy(field + len, _lastPersisted.data(), _lastPersisted.len());
          len += _lastPersisted.len();
          if (len < totalLen)
          {
            field[len++] = ',';
          }
        }
        if (!_recoveryTimestamp.empty() && !_range.isValid())
        {
          memcpy(field + len, _recoveryTimestamp.data(),
                 _recoveryTimestamp.len());
          // If more is to be written after this, uncomment the following
          //len += _lastPersisted.len();
          //if (len < totalLen) field[len++] = ',';
        }
        // _recentList clear will delete[] current buffer and assign will get cleared
        _recentList.clear();
        _recentList.assign(field, totalLen);
        if (_range.isValid())
        {
          if (_recentList != AMPS_BOOKMARK_EPOCH)
          {
            if (_range.getStart() != _recentList)
            {
              _range.replaceStart(_recentList, true);
            }
            else if (_range.isStartInclusive())
            {
              amps_uint64_t publisher, sequence;
              parseBookmark(_range.getStart(), publisher,
                            sequence);
              PublisherIterator pub = _publishers.find(publisher);
              if (pub != _publishers.end()
                  && pub->second >= sequence)
              {
                _range.makeStartExclusive();
              }
            }
          }
          return _range;
        }
        return _recentList;
      }

      Message::Field getMostRecent(bool update_ = false)
      {
        Lock<Mutex> guard(_subLock);
        // Return the same as last time if nothing's changed
        // _recent is the most recent bookmark.
        if (update_ && _store->_recentChanged)
        {
          _updateMostRecent();
        }
        if (_recent.empty())
        {
          return Message::Field(AMPS_BOOKMARK_EPOCH);
        }
        else
        {
          return _recent;
        }
      }

      Message::Field getLastPersisted()
      {
        Lock<Mutex> guard(_subLock);
        return _lastPersisted;
      }

      void setMostRecent(const Message::Field& recent_)
      {
        _recent.clear();
        _recent.deepCopy(recent_);
      }

      void setRecoveryTimestamp(const char* recoveryTimestamp_,
                                size_t len_ = 0)
      {
        _recoveryTimestamp.clear();
        size_t len = (len_ == 0) ? AMPS_TIMESTAMP_LEN : len_;
        char* ts = new char[len];
        memcpy((void*)ts, (const void*)recoveryTimestamp_, len);
        _recoveryTimestamp.assign(ts, len);
      }

      void moveEntries(char* old_, char* new_, size_t newSize_)
      {
        size_t least = _least;
        size_t leastBase = _leastBase;
        if (_recoveryMin != AMPS_UNSET_INDEX)
        {
          least = _recoveryMin;
          leastBase = _recoveryBase;
        }
        // First check if we grew in place, if so, just move current after least
        if (old_ == new_)
        {
          if (newSize_ - (sizeof(Entry)*_entriesLength) > sizeof(Entry)*least)
          {
            memcpy(new_ + (sizeof(Entry)*_entriesLength),
                   old_, (sizeof(Entry)*least));
            // Clear the beginning where those entries were
            memset(old_, 0, sizeof(Entry)*least);
          }
          else // We have to use an intermediate buffer
          {
            Entry* buffer = new Entry[least];
            memcpy((void*)buffer, (void*)old_, sizeof(Entry)*least);
            //Put the beginning entries at the start of the new buffer
            memcpy((void*)new_, (void*)((char*)old_ + (sizeof(Entry)*least)),
                   (_entriesLength - least)*sizeof(Entry));
            //Put the end entries after the beginning entries
            memcpy((void*)((char*)new_ + ((_entriesLength - least)*sizeof(Entry))),
                   (void*)buffer, least * sizeof(Entry));
            // Least is now at 0 so base must be increased
            leastBase += least;
            least = 0;
            delete [] buffer;
          }
        }
        else
        {
          //Put the beginning entries at the start of the new buffer
          memcpy((void*)new_, (void*)((char*)old_ + (sizeof(Entry)*least)),
                 (_entriesLength - least)*sizeof(Entry));
          //Put the end entries after the beginning entries
          memcpy((void*)((char*)new_ + ((_entriesLength - least)*sizeof(Entry))),
                 (void*)old_, least * sizeof(Entry));
          // Least is now at 0 so base must be increased
          leastBase += least;
          least = 0;
        }
        if (_recoveryMin != AMPS_UNSET_INDEX)
        {
          _least = least + (_least + _leastBase) - (_recoveryMin + _recoveryBase);
          _recoveryMax = least + (_recoveryMax + _recoveryMaxBase) -
                         (_recoveryMin + _recoveryBase);
          _recoveryMaxBase = leastBase;
          _recoveryMin = least;
          _recoveryBase = leastBase;
        }
        else
        {
          _least = least;
        }
        _leastBase = leastBase;
        // Current is now after everything and using the same base
        _currentBase = _leastBase;
        _current = least + _entriesLength;
      }

      inline size_t getOldestBookmarkSeq()
      {
        Lock<Mutex> guard(_subLock);
        // If there is nothing in the store, return -1, otherwise return lowest
        return ((_least + _leastBase) == (_current + _currentBase)) ? AMPS_UNSET_INDEX :
               _least + _leastBase;
      }

      bool lastPersisted(const Message::Field& bookmark_)
      {
        // These shouldn't be persisted
        if (bookmark_ == AMPS_BOOKMARK_NOW
            || BookmarkRange::isRange(bookmark_))
        {
          return false;
        }
        Lock<Mutex> guard(_subLock);
        return _setLastPersisted(bookmark_);
      }

      bool _setLastPersisted(const Message::Field& bookmark_)
      {
        if (!_lastPersisted.empty())
        {
          amps_uint64_t publisher, publisher_lastPersisted;
          amps_uint64_t sequence, sequence_lastPersisted;
          parseBookmark(bookmark_, publisher, sequence);
          parseBookmark(_lastPersisted, publisher_lastPersisted,
                        sequence_lastPersisted);
          if (publisher == publisher_lastPersisted &&
              sequence <= sequence_lastPersisted)
          {
            return false;
          }
        }
        // deepCopy will clear what's in _lastPersisted
        _lastPersisted.deepCopy(bookmark_);
        _store->_recentChanged = true;
        _recoveryTimestamp.clear();
        return true;
      }

      Message::Field lastPersisted(size_t bookmark_)
      {
        Lock<Mutex> guard(_subLock);
        Message::Field& bookmark = _entries[bookmark_]._val;
        // These shouldn't be persisted
        if (bookmark == AMPS_BOOKMARK_NOW
            || BookmarkRange::isRange(bookmark))
        {
          return bookmark;
        }
        _setLastPersisted(bookmark);
        return bookmark;
      }

      // Returns the index of the recovered item, either the index where it
      // was first stored prior to getMostRecent, or the new index if it is
      // relogged either because this is called from log() or because it was
      // not active but also not persisted.
      size_t recover(const Message::Field& bookmark_, bool relogIfNotDiscarded)
      {
        size_t retVal = AMPS_UNSET_INDEX;
        if (_recovered.empty() || _recoveryBase == AMPS_UNSET_INDEX)
        {
          return retVal;
        }
        // Check if this is a recovered bookmark.
        // If so, copy the existing one to the new location
        RecoveryIterator item = _recovered.find(bookmark_);
        if (item != _recovered.end())
        {
          size_t seqNo = item->second;
          size_t index = (seqNo - _recoveryBase) % _entriesLength;
          // If we only have recovery entries and isDiscarded is
          // checking on an already discarded entry, update recent.
          if (_least + _leastBase == _current + _currentBase &&
              !_entries[index]._active)
          {
            _store->_recentChanged = true;
            _recent.clear();
            _recent = _entries[index]._val.deepCopy();
            retVal = moveEntry(index);
            if (retVal == AMPS_UNSET_INDEX)
            {
              recover(bookmark_, relogIfNotDiscarded);
            }
            _least = _current;
            _leastBase = _currentBase;
          }
          else if (!_entries[index]._active || relogIfNotDiscarded)
          {
            retVal = moveEntry(index);
            if (retVal == AMPS_UNSET_INDEX)
            {
              recover(bookmark_, relogIfNotDiscarded);
            }
          }
          else
          {
            return index;
          }
          _recovered.erase(item);
          if (_recovered.empty())
          {
            _recoveryMin = AMPS_UNSET_INDEX;
            _recoveryBase = AMPS_UNSET_INDEX;
            _recoveryMax = AMPS_UNSET_INDEX;
            _recoveryMaxBase = AMPS_UNSET_INDEX;
          }
          else if (index == _recoveryMin)
          {
            while (_entries[_recoveryMin]._val.empty() &&
                   (_recoveryMin + _recoveryBase) < (_recoveryMax + _recoveryMaxBase))
            {
              if (++_recoveryMin == _entriesLength)
              {
                _recoveryMin = 0;
                _recoveryBase += _entriesLength;
              }
            }
          }
        }
        return retVal;
      }

      // Return the id of this Subscription
      Message::Field id() const
      {
        return _id;
      }

      struct Entry
      {
        Message::Field _val;                                           //16
        bool _active;                                                  //17
        char _padding[32 - sizeof(Message::Field) - sizeof(bool)];     //32

        Entry() : _active(false)
        {
          ;
        }
      };

      struct EntryHash
      {
        Field::FieldHash _hasher;

        size_t operator()(const Entry* entryPtr_) const
        {
          return _hasher(entryPtr_->_val);
        }

        bool operator()(const Entry* lhsPtr_, const Entry* rhsPtr_) const
        {
          return _hasher(lhsPtr_->_val, rhsPtr_->_val);
        }
      };

      //typedef std::set<Entry*, EntryHash> EntryPtrList;
      typedef std::vector<Entry*> EntryPtrList;

      void getRecoveryEntries(EntryPtrList& list_)
      {
        if (_recoveryMin == AMPS_UNSET_INDEX ||
            _recoveryMax == AMPS_UNSET_INDEX)
        {
          return;
        }
        size_t base = _recoveryBase;
        size_t max = _recoveryMax + _recoveryMaxBase;
        for (size_t i = _recoveryMin; i + base < max; ++i)
        {
          if (i == _entriesLength)
          {
            i = 0;
            base = _recoveryMaxBase;
          }
          //list_.insert(&(_entries[i]));
          list_.push_back(&(_entries[i]));
        }
        return;
      }

      void getActiveEntries(EntryPtrList& list_)
      {
        size_t base = _leastBase;
        for (size_t i = _least; i + base < _current + _currentBase; ++i)
        {
          if (i >= _entriesLength)
          {
            i = 0;
            base = _currentBase;
          }
          //list_.insert(&(_entries[i]));
          list_.push_back(&(_entries[i]));
        }
        return;
      }

      Entry* getEntryByIndex(size_t index_)
      {
        Lock<Mutex> guard(_subLock);
        size_t base = (_recoveryBase == AMPS_UNSET_INDEX ||
                       index_ >= _least + _leastBase)
                      ? _leastBase : _recoveryBase;
        // Return NULL if not a valid index
        size_t min = (_recoveryMin == AMPS_UNSET_INDEX ?
                      _least + _leastBase :
                      _recoveryMin + _recoveryBase);
        if (index_ >= _current + _currentBase || index_ < min)
        {
          return NULL;
        }
        return &(_entries[(index_ - base) % _entriesLength]);
      }

      void justRecovered()
      {
        Lock<Mutex> guard(_subLock);
        _updateMostRecent();
        EntryPtrList list;
        getRecoveryEntries(list);
        setPublishersToDiscarded(&list, &_publishers);
      }

      void setPublishersToDiscarded(EntryPtrList* recovered_,
                                    PublisherMap* publishers_)
      {
        // Need to reset publishers to only have up to the last
        // discarded sequence number. Messages that were in transit
        // during previous run but not discarded should be considered
        // new and not duplicate after a restart/recovery.
        for (EntryPtrList::iterator i = recovered_->begin();
             i != recovered_->end(); ++i)
        {
          if ((*i)->_val.empty())
          {
            continue;
          }
          amps_uint64_t publisher = (amps_uint64_t)0;
          amps_uint64_t sequence = (amps_uint64_t)0;
          parseBookmark((*i)->_val, publisher, sequence);
          if (publisher && sequence && (*i)->_active &&
              (*publishers_)[publisher] >= sequence)
          {
            (*publishers_)[publisher] = sequence - 1;
          }
        }
      }

      void clearLastPersisted()
      {
        Lock<Mutex> guard(_subLock);
        _lastPersisted.clear();
      }

      void setLastPersistedToEpoch()
      {
        Lock<Mutex> guard(_subLock);
        _setLastPersistedToEpoch();
      }

    private:
      Subscription(const Subscription&);
      Subscription& operator=(const Subscription&);

      size_t moveEntry(size_t index_)
      {
        // Check for wrap
        if (_current >= _entriesLength)
        {
          _current = 0;
          _currentBase += _entriesLength;
        }
        // Check for resize
        // If list is too small, double it
        if ((_current == _least % _entriesLength &&
             _leastBase < _currentBase) ||
            (_current == _recoveryMin && _recoveryBase < _currentBase))
        {
          if (!_store->resize(_id, (char**)&_entries,
                              sizeof(Entry) * _entriesLength * 2))
          {
            return AMPS_UNSET_INDEX;
          }
          // Length was doubled
          _entriesLength *= 2;
        }
        _entries[_current]._val = _entries[index_]._val;
        _entries[_current]._active = _entries[index_]._active;
        // No need to clear Field, just set it to empty
        _entries[index_]._val.assign(NULL, 0);
        _entries[index_]._active = false;
        return _current++;
      }

      void _setLastPersistedToEpoch()
      {
        size_t fieldLen = strlen(AMPS_BOOKMARK_EPOCH);
        char* field = new char[fieldLen];
        memcpy(field, AMPS_BOOKMARK_EPOCH, fieldLen);
        _lastPersisted.clear();
        _lastPersisted.assign(field, fieldLen);
      }

      bool _discard(size_t index_)
      {
        bool retVal = false;
        // Lock should already be held
        assert((_recoveryBase == AMPS_UNSET_INDEX && _recoveryMin == AMPS_UNSET_INDEX) ||
               (_recoveryBase != AMPS_UNSET_INDEX && _recoveryMin != AMPS_UNSET_INDEX));
        size_t base = (_recoveryBase == AMPS_UNSET_INDEX
                       || index_ >= _least + _leastBase)
                      ? _leastBase : _recoveryBase;
        // discard of a record not in the log is a no-op
        size_t min = (_recoveryMin == AMPS_UNSET_INDEX ? _least + _leastBase :
                      _recoveryMin + _recoveryBase);
        if (index_ >= _current + _currentBase || index_ < min)
        {
          return retVal;
        }

        // log that this one is discarded, then
        // recalculate what the most recent entry is.
        Entry& e = _entries[(index_ - base) % _entriesLength];
        e._active = false;

        size_t index = index_;
        if (_recoveryMin != AMPS_UNSET_INDEX &&
            index_ == _recoveryMin + _recoveryBase)
        {
          // Find all to discard
          size_t j = _recoveryMin;
          while (j + _recoveryBase < _recoveryMax + _recoveryMaxBase &&
                 !_entries[j]._active)
          {
            // This index might be left-over from a slow discard and we
            // may have reconnected. We have a few possibilities at this point.
            // 1. If we re-logged this bookmark, this index will point at an
            // empty bookmark. This could happen if the discard thread was slow
            // and the reconnect was fast. We wouldn't report the
            // the re-arrival of the bookmark as a duplicate because it
            // hadn't been marked as discarded. In this case, we have to
            // simply move past this in the recovery area.
            // 2. This bookmark should become _recent because we haven't
            // yet received anything since our last call to getMostRecent.
            // In this case, we need to take it out of recovered but not
            // clear it. The publishers map should report it as duplicate.
            // 3. This is the 'oldest' recovered, but we have received new
            // bookmarks since we got this one. We can clear it because the
            // publishers map should report it as a duplicate if/when it
            // does arrive again. Move the _recoveryMin ahead and remove it
            // from recovered.
            Message::Field& bookmark = _entries[j]._val;
            // Option 1 skips this and just moves on
            if (!bookmark.empty())
            {
              _recovered.erase(bookmark);
              // Make sure our publishers map will mark it discarded
              amps_uint64_t publisher, sequence;
              parseBookmark(bookmark, publisher, sequence);
              PublisherIterator pub = _publishers.find(publisher);
              if (pub == _publishers.end() || pub->second < sequence)
              {
                _publishers[publisher] = sequence;
              }
              if (_least + _leastBase == _current + _currentBase ||
                  ((_least + _leastBase) % _entriesLength) ==
                  ((_recoveryMin + _recoveryBase + 1)) % _entriesLength)
              {
                // Option 2, reset recent
                retVal = true;
                _store->_recentChanged = true;
                _recoveryTimestamp.clear();
                _recent.clear();
                _recent = bookmark;
                bookmark.assign(NULL, 0);
              }
              else
              {
                // Option 3, simply clear this one
                bookmark.clear();
              }
            }
            // If we reach the buffer end,
            // keep checking from the beginning
            if (++j == _entriesLength)
            {
              // Least has now loooped around
              _recoveryBase += _entriesLength;
              j = 0;
            }
          }
          assert(j + _recoveryBase != _recoveryMax + _recoveryMaxBase ||
                 _recovered.empty());
          if (_recovered.empty())
          {
            _recoveryMin = AMPS_UNSET_INDEX;
            _recoveryBase = AMPS_UNSET_INDEX;
            _recoveryMax = AMPS_UNSET_INDEX;
            _recoveryMaxBase = AMPS_UNSET_INDEX;
            // Cleared recovered, want to check onward
            index = _least + _leastBase;
          }
          else
          {
            _recoveryMin = j;
          }
        }
        // if this is the first item in the list, discard all inactive ones
        // as long as recovery also says its okay
        if (index == _least + _leastBase)
        {
          // Find all to discard
          size_t j = _least;
          while (j + _leastBase < _current + _currentBase &&
                 !_entries[j]._active)
          {
            //Must free associated memory
            _recent.clear();
            _recent = _entries[j]._val;
            _entries[j]._val.assign(NULL, 0);
            _store->_recentChanged = true;
            retVal = true;
            _recoveryTimestamp.clear();
            // If we reach the buffer end,
            // keep checking from the beginning
            if (++j == _entriesLength)
            {
              // Least has now loooped around
              _leastBase += _entriesLength;
              j = 0;
            }
          }
          _least = j;
        }
        return retVal;
      }

      void _updateMostRecent()
      {
        // Lock is already held
        _recovered.clear();
        assert((_recoveryBase == AMPS_UNSET_INDEX && _recoveryMin == AMPS_UNSET_INDEX) ||
               (_recoveryBase != AMPS_UNSET_INDEX && _recoveryMin != AMPS_UNSET_INDEX));
        size_t base = (_recoveryMin == AMPS_UNSET_INDEX) ? _leastBase : _recoveryBase;
        size_t start = (_recoveryMin == AMPS_UNSET_INDEX) ? _least : _recoveryMin;
        _recoveryMin = AMPS_UNSET_INDEX;
        _recoveryBase = AMPS_UNSET_INDEX;
        _recoveryMax = AMPS_UNSET_INDEX;
        _recoveryMaxBase = AMPS_UNSET_INDEX;
        for (size_t i = start; i + base < _current + _currentBase; i++)
        {
          if ( i >= _entriesLength )
          {
            i = 0;
            base = _currentBase;
          }
          if (i >= _recoveryMax + _recoveryBase && i < _least + _leastBase)
          {
            continue;
          }
          Entry& entry = _entries[i];
          if (!entry._val.empty())
          {
            _recovered[entry._val] = i + base;
            if (_recoveryMin == AMPS_UNSET_INDEX)
            {
              _recoveryMin = i;
              _recoveryBase = base;
              _recoveryMax = _current;
              _recoveryMaxBase = _currentBase;
            }
          }
        }
        if (_current == _entriesLength)
        {
          _current = 0;
          _currentBase += _entriesLength;
        }
        _least = _current;
        _leastBase = _currentBase;
      }

      Message::Field _id;
      Message::Field _recent;
      Message::Field _lastPersisted;
      Message::Field _recentList;
      BookmarkRange  _range;
      Message::Field _recoveryTimestamp;
      size_t _current;
      size_t _currentBase;
      size_t _least;
      size_t _leastBase;
      size_t _recoveryMin;
      size_t _recoveryBase;
      size_t _recoveryMax;
      size_t _recoveryMaxBase;
      size_t _entriesLength;
      Entry* _entries;
      MemoryBookmarkStore* _store;
      Mutex  _subLock;
      RecoveryMap _recovered;
    public:
      PublisherMap _publishers;
    };

  public:
    ///
    /// Creates a MemoryBookmarkStore
    MemoryBookmarkStore() : BookmarkStoreImpl(),
      _subsLock(),
      _lock(),
      _serverVersion(AMPS_DEFAULT_MIN_VERSION),
      _recentChanged(true),
      _recovering(false),
      _recoveryPointAdapter(NULL),
      _recoveryPointFactory(NULL)
    { ; }

    typedef RecoveryPointAdapter::iterator RecoveryIterator;

    ///
    /// Creates a MemoryBookmarkStore
    /// \param adapter_ The {@link RecoveryPointAdapter} to notify
    /// about updates.
    /// \param factory_ An optional factory function to use
    /// to create the {@link RecoveryPoint} objects sent to the recoveryPointAdapter_.
    MemoryBookmarkStore(const RecoveryPointAdapter& adapter_,
                        RecoveryPointFactory factory_ = NULL)
      : BookmarkStoreImpl()
      , _subsLock()
      , _lock()
      , _serverVersion(AMPS_DEFAULT_MIN_VERSION)
      , _recentChanged(true)
      , _recovering(true)
      , _recoveryPointAdapter(adapter_)
      , _recoveryPointFactory(factory_)
    {
      Message msg;
      for (RecoveryIterator recoveryPoint = _recoveryPointAdapter.begin();
           recoveryPoint != _recoveryPointAdapter.end();
           ++recoveryPoint)
      {
        Field subId(recoveryPoint->getSubId());
        msg.setSubscriptionHandle(static_cast<amps_subscription_handle>(0));
        msg.setSubId(subId);
        Field bookmark = recoveryPoint->getBookmark();
        if (BookmarkRange::isRange(bookmark))
        {
          msg.setBookmark(bookmark);
          _log(msg);
        }
        else
        {
          std::vector<Field> bmList = Field::parseBookmarkList(bookmark);
          for (std::vector<Field>::iterator bkmk = bmList.begin(); bkmk != bmList.end(); ++bkmk)
          {
            if (Field::isTimestamp(*bkmk))
            {
              find(subId)->setRecoveryTimestamp(bkmk->data(), bkmk->len());
            }
            else
            {
              msg.assignBookmark(bkmk->data(), bkmk->len());
              _isDiscarded(msg);
              _log(msg);
              _discard(msg);
            }
          }
          // Reset to original bookmark
          msg.setBookmark(bookmark);
        }
      }
      _recovering = false;
    }

    virtual ~MemoryBookmarkStore()
    {
      __purge();
    }

    ///
    /// Log a bookmark to the persistent log and return the corresponding
    /// sequence number for this bookmark.
    /// \param message_ The Message to log.
    ///
    virtual size_t log(Message& message_)
    {
      Lock<Mutex> guard(_lock);
      return _log(message_);
    }

    ///
    /// Log a discard-bookmark entry to the persistent log
    /// based on a bookmark sequence number.
    /// \param message_ The Message to discard.
    ///
    virtual void discard(const Message& message_)
    {
      Lock<Mutex> guard(_lock);
      (void)_discard(message_);
    }

    ///
    /// Log a discard-bookmark entry to the persistent log
    /// based on a bookmark sequence number. Use the \ref discard(const Message&)
    /// function instead when you have the full Message object available..
    /// \param subId_ The id of the subscription to which the bookmark applies.
    /// \param bookmarkSeqNo_ The bookmark sequence number to discard.
    ///
    virtual void discard(const Message::Field& subId_, size_t bookmarkSeqNo_)
    {
      Lock<Mutex> guard(_lock);
      (void)_discard(subId_, bookmarkSeqNo_);
    }

    ///
    /// Returns the most recent bookmark from the log that
    /// ought to be used for (re-)subscriptions.
    /// \param subId_ The id of the subscription to check.
    ///
    virtual Message::Field getMostRecent(const Message::Field& subId_)
    {
      Lock<Mutex> guard(_lock);
      return _getMostRecent(subId_);
    }

    ///
    /// Called for each arriving message to determine if
    /// the application has already seen this bookmark and should
    /// not be reprocessed.  Returns 'true' if the bookmark is
    /// in the log and should not be re-processed, false otherwise.
    /// \param message_ The Message to check.
    /// \return Whether or not the Message has been discarded from this store.
    ///
    virtual bool isDiscarded(Message& message_)
    {
      Lock<Mutex> guard(_lock);
      return _isDiscarded(message_);
    }

    ///
    /// Called to purge the contents of this store.
    /// Removes any tracking history associated with publishers and received
    /// messages, and may delete or truncate on-disk representations as well.
    ///
    virtual void purge()
    {
      Lock<Mutex> guard(_lock);
      _purge();
    }

    ///
    /// Called to purge the contents of this store for particular subId.
    /// Removes any tracking history associated with publishers and received
    /// messages, and will remove the subId from the file as well.
    ///
    virtual void purge(const Message::Field& subId_)
    {
      Lock<Mutex> guard(_lock);
      _purge(subId_);
    }

    ///
    /// Called to find the oldest bookmark in the store.
    /// \param subId_ The subscription ID on which to find the oldest bookmark.
    /// \return The bookmark that is oldest in the store for subId_
    virtual size_t getOldestBookmarkSeq(const Message::Field& subId_)
    {
      Lock<Mutex> guard(_lock);
      return _getOldestBookmarkSeq(subId_);
    }

    ///
    /// Mark the bookmark provided as replicated to all sync replication
    /// destinations for the given subscription.
    /// \param subId_ The subscription Id to which the bookmark applies.
    /// \param bookmark_ The most recent replicated bookmark.
    virtual void persisted(const Message::Field& subId_,
                           const Message::Field& bookmark_)
    {
      Lock<Mutex> guard(_lock);
      _persisted(find(subId_), bookmark_);
    }

    ///
    /// Mark the bookmark provided as replicated to all sync replication
    /// destinations for the given subscription.
    /// \param subId_ The subscription Id to which the bookmark applies.
    /// \param bookmark_ The most recent bookmark's sequence number.
    /// \return The bookmark field that was just marked persisted.
    virtual Message::Field persisted(const Message::Field& subId_,
                                     size_t bookmark_)
    {
      Lock<Mutex> guard(_lock);
      return _persisted(find(subId_), bookmark_);
    }

    ///
    /// Internally used to set the server version so the store knows how to deal
    /// with persisted acks and calls to getMostRecent().
    /// \param version_ The version of the server being used.
    void setServerVersion(const VersionInfo& version_)
    {
      setServerVersion(version_.getOldStyleVersion());
    }

    ///
    /// Internally used to set the server version so the store knows how to deal
    /// with persisted acks and calls to getMostRecent().
    /// \param version_ The version of the server being used.
    void setServerVersion(size_t version_)
    {
      Lock<Mutex> guard(_subsLock);
      _serverVersion = version_;
    }

    inline bool isWritableBookmark(size_t length)
    {
      return length >= AMPS_MIN_BOOKMARK_LEN;
    }

    typedef Subscription::EntryPtrList EntryPtrList;

  protected:

    // Called once lock is acquired
    size_t _log(Message& message_)
    {
      Message::Field bookmark = message_.getBookmark();
      Subscription* pSub = (Subscription*)(message_.getSubscriptionHandle());
      if (!pSub)
      {
        Message::Field subId = message_.getSubscriptionId();
        if (subId.empty())
        {
          subId = message_.getSubscriptionIds();
        }
        pSub = find(subId);
        message_.setSubscriptionHandle(
          static_cast<amps_subscription_handle>(pSub));
      }
      size_t retVal = pSub->log(bookmark);
      message_.setBookmarkSeqNo(retVal);
      return retVal;
    }

    // Called once lock is acquired
    bool _discard(const Message& message_)
    {
      size_t bookmarkSeqNo = message_.getBookmarkSeqNo();
      Subscription* pSub = (Subscription*)(message_.getSubscriptionHandle());
      if (!pSub)
      {
        Message::Field subId = message_.getSubscriptionId();
        if (subId.empty())
        {
          subId = message_.getSubscriptionIds();
        }
        pSub = find(subId);
      }
      bool retVal = pSub->discard(bookmarkSeqNo);
      if (retVal)
      {
        updateAdapter(pSub);
      }
      return retVal;
    }

    // Called once lock is acquired
    bool _discard(const Message::Field& subId_, size_t bookmarkSeqNo_)
    {
      Subscription* pSub = find(subId_);
      bool retVal = pSub->discard(bookmarkSeqNo_);
      if (retVal)
      {
        updateAdapter(pSub);
      }
      return retVal;
    }

    // Called once lock is acquired
    Message::Field _getMostRecent(const Message::Field& subId_,
                                  bool usePublishersList_ = true)
    {
      Subscription* pSub = find(subId_);
      return pSub->getMostRecentList(usePublishersList_);
    }

    // Called once lock is acquired
    bool _isDiscarded(Message& message_)
    {
      Message::Field subId = message_.getSubscriptionId();
      if (subId.empty())
      {
        subId = message_.getSubscriptionIds();
      }
      Subscription* pSub = find(subId);
      message_.setSubscriptionHandle(
        static_cast<amps_subscription_handle>(pSub));
      return pSub->isDiscarded(message_.getBookmark());
    }

    // Called once lock is acquired
    size_t _getOldestBookmarkSeq(const Message::Field& subId_)
    {
      Subscription* pSub = find(subId_);
      return pSub->getOldestBookmarkSeq();
    }

    // Called once lock is acquired
    virtual void _persisted(Subscription* pSub_,
                            const Message::Field& bookmark_)
    {
      if (pSub_->lastPersisted(bookmark_))
      {
        updateAdapter(pSub_);
      }
    }

    // Called once lock is acquired
    virtual Message::Field _persisted(Subscription* pSub_, size_t bookmark_)
    {
      return pSub_->lastPersisted(bookmark_);
    }

    // Called once lock is acquired
    void _purge()
    {
      if (_recoveryPointAdapter.isValid())
      {
        _recoveryPointAdapter.purge();
      }
      __purge();
    }

    // Called once lock is acquired
    void __purge()
    {
      // Walk through list and clear Fields before calling clear
      while (!_subs.empty())
      {
        SubscriptionMap::iterator iter = _subs.begin();
        //The subId key is cleared when deleting the Subscription, which shares
        //the _data pointer in its id field.
        const_cast<Message::Field&>(iter->first).clear();
        delete (iter->second);
        _subs.erase(iter);
      }
      _subs.clear();
    }

    // Called once lock is acquired
    virtual void _purge(const Message::Field& subId_)
    {
      if (_recoveryPointAdapter.isValid())
      {
        _recoveryPointAdapter.purge(subId_);
      }
      __purge(subId_);
    }

    // Called once lock is acquired
    virtual void __purge(const Message::Field& subId_)
    {
      Lock<Mutex> guard(_subsLock);
      SubscriptionMap::iterator iter = _subs.find(subId_);
      if (iter == _subs.end())
      {
        return;
      }
      const_cast<Message::Field&>(iter->first).clear();
      delete (iter->second);
      _subs.erase(iter);
    }

    // Can be used by subclasses during recovery
    void setMostRecent(const Message::Field& subId_,
                       const Message::Field& recent_)
    {
      find(subId_)->setMostRecent(recent_);
    }

    Mutex  _subsLock;
    Mutex _lock;
    static const char ENTRY_BOOKMARK  = 'b';
    static const char ENTRY_DISCARD   = 'd';
    static const char ENTRY_PERSISTED = 'p';

    virtual Subscription* find(const Message::Field& subId_)
    {
      if (subId_.empty())
      {
        throw StoreException("A valid subscription ID must be provided to the Bookmark Store");
      }
      Lock<Mutex> guard(_subsLock);
      if (_subs.count(subId_) == 0)
      {
        // Subscription will be created
        Message::Field id;
        id.deepCopy(subId_);
        _subs[id] = new Subscription(this, id);
        return _subs[id];
      }
      return _subs[subId_];
    }

    virtual bool resize(const Message::Field& subId_, char** newBuffer_, size_t size_,
                        bool callResizeHandler_ = true)
    {
      assert(newBuffer_ != 0);
      if (size_ == 0) // Delete the buffer
      {
        if (*newBuffer_)
        {
          free(*newBuffer_);
          *newBuffer_ = NULL;
        }
        return true;
      }
      if (callResizeHandler_ && !callResizeHandler(subId_, size_))
      {
        return false;
      }
      char* oldBuffer = *newBuffer_ ? *newBuffer_ : NULL;
      *newBuffer_ = (char*)malloc(size_);
      memset(*newBuffer_, 0, size_);
      if (oldBuffer)
      {
        find(subId_)->moveEntries(oldBuffer, *newBuffer_, size_);
        free(oldBuffer);
      }
      return true;
    }

  protected:
    void updateAdapter(Subscription* pSub_)
    {
      if (_recovering || !_recentChanged || !_recoveryPointAdapter.isValid())
      {
        return;
      }
      if (_recoveryPointFactory)
      {
        RecoveryPoint update(_recoveryPointFactory(pSub_->id(),
                                                   pSub_->getMostRecentList(false)));
        _recoveryPointAdapter.update(update);
      }
      else
      {
        RecoveryPoint update(new FixedRecoveryPoint(pSub_->id(),
                                                    pSub_->getMostRecentList(false)));
        _recoveryPointAdapter.update(update);
      }
    }

    typedef std::map<Message::Field, Subscription*, Message::Field::FieldHash> SubscriptionMap;
    SubscriptionMap _subs;
    size_t _serverVersion;
    bool _recentChanged;
    bool _recovering;
    typedef std::set<Subscription*> SubscriptionSet;
    RecoveryPointAdapter _recoveryPointAdapter;
    RecoveryPointFactory _recoveryPointFactory;
  };

} // end namespace AMPS

#endif //_MEMORYBOOKMARKSTORE_H_

