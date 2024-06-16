///////////////////////////////////////////////////////////////////////////
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

#ifndef _RINGBOOKMARKSTORE_H_
#define _RINGBOOKMARKSTORE_H_

#define AMPS_RING_POSITIONS 3
// Setting bookmark max at 6 bookmarks in a range (5 commas, open, :, and close)
#define AMPS_RING_BYTES_BOOKMARK (AMPS_MAX_BOOKMARK_LEN * 6 + 8)
#define AMPS_RING_ENTRY_SIZE  1024
#define AMPS_RING_BYTES_SUBID ( AMPS_RING_ENTRY_SIZE - ( AMPS_RING_POSITIONS * AMPS_RING_BYTES_BOOKMARK ) )
#define AMPS_RING_ENTRIES 32

#include <amps/MemoryBookmarkStore.hpp>
#ifdef _WIN32
  #include <windows.h>
#else
  #include <sys/mman.h>
  #include <unistd.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#if !defined(MREMAP_MAYMOVE)
  #define MREMAP_MAYMOVE 0
#endif

/// \file RingBookmarkStore.hpp
/// \brief Provides AMPS::RingBookmarkStore, a bookmark store that stores
/// only the MOST_RECENT bookmark to a file and keeps any bookmarks later
/// than the most recent in memory.

namespace AMPS
{
///
/// A BookmarkStoreImpl that stores only the MOST_RECENT bookmark to a file
/// for recovery and keeps any bookmarks later than most recent in memory.
/// This class is best used when you want permanent storage for recovery on
/// the client side, but expect to process all messages in order. It will not
/// protect you from possibly missing or duplicate bookmarks that can occur
/// if you are using a bookmark live subscription and you have to fail over
/// to another AMPS instance which receives messages in a different order.
/// The storage actually has 3 storage positions to allow for protection
/// against crashes while updating storage.
  class RingBookmarkStore : public MemoryBookmarkStore
  {
    struct SubscriptionPosition
    {
      size_t _index;
      size_t _current;
    };

  public:
    ///
    /// Create a RingBookmarkStore using fileName_ for storage of most recent.
    /// If the file exists and has a valid bookmark, it will be recovered.
    /// \param fileName_ The name of the file to use for storage.
    RingBookmarkStore(const char* fileName_)
      : MemoryBookmarkStore(), _fileSize(0), _currentIndex(0), _log(0)
#ifdef _WIN32
      , _file(INVALID_HANDLE_VALUE), _mapFile(INVALID_HANDLE_VALUE)
#else
      , _fd(0)
#endif
      , _ringRecovering(true)
    {
      init(fileName_);
    }

    RingBookmarkStore(const std::string& fileName_)
      : MemoryBookmarkStore(), _fileSize(0), _currentIndex(0), _log(0)
#ifdef _WIN32
      , _file(INVALID_HANDLE_VALUE), _mapFile(INVALID_HANDLE_VALUE)
#else
      , _fd(0)
#endif
      , _ringRecovering(true)
    {
      init(fileName_.c_str());
    }

    virtual ~RingBookmarkStore()
    {
#ifdef _WIN32
      UnmapViewOfFile(_log);
      _log = 0;
      CloseHandle(_mapFile);
      _mapFile = INVALID_HANDLE_VALUE;
      CloseHandle(_file);
      _file = INVALID_HANDLE_VALUE;
#else
      munmap(_log, _fileSize);
      _log = 0;
      close(_fd);
      _fd = 0;
#endif
      // In case _lock gets acquired by reader thread between end of this
      // destructor and start of base class destructor, prevent write()
      _ringRecovering = true;
    }

    ///
    /// Return the corresponding sequence number for this bookmark.
    /// \param message_ The Message to check.
    ///
    virtual size_t log(Message& message_)
    {
      Lock<Mutex> guard(_lock);
      size_t ret = MemoryBookmarkStore::_log(message_);
      if (BookmarkRange::isRange(message_.getBookmark()))
      {
        Message::Field subId = message_.getSubscriptionId();
        if (subId.empty())
        {
          subId = message_.getSubscriptionIds();
        }
        write(subId, MemoryBookmarkStore::_getMostRecent(subId, false));
      }
      return ret;
    }

    ///
    /// Log a discard-bookmark entry to the persistent log
    /// based on a bookmark sequence number.
    /// \param message_ The Message to discard.
    ///
    virtual void discard(const Message& message_)
    {
      Lock<Mutex> guard(_lock);
      if (MemoryBookmarkStore::_discard(message_) && _recentChanged)
      {
        Message::Field subId = message_.getSubscriptionId();
        if (subId.empty())
        {
          subId = message_.getSubscriptionIds();
        }
        write(subId, MemoryBookmarkStore::_getMostRecent(subId, false));
        _recentChanged = false;
      }
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
      if (MemoryBookmarkStore::_discard(subId_, bookmarkSeqNo_)
          && _recentChanged)
      {
        write(subId_, MemoryBookmarkStore::_getMostRecent(subId_, false));
        _recentChanged = false;
      }
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
      MemoryBookmarkStore::_persisted(find(subId_), bookmark_);
      if (_recentChanged)
      {
        write(subId_, MemoryBookmarkStore::_getMostRecent(subId_, false));
        _recentChanged = false;
      }
    }

    ///
    /// Returns the most recent bookmark from the log that
    /// ought to be used for (re-)subscriptions.
    /// \param subId_ The id of the subscription to check.
    ///
    virtual Message::Field getMostRecent(const Message::Field& subId_)
    {
      Lock<Mutex> guard(_lock);
      return MemoryBookmarkStore::_getMostRecent(subId_);
    }

    ///
    /// Called to purge the contents of this store.
    /// Removes any tracking history associated with publishers and received
    /// messages, and may delete or truncate on-disk representations as well.
    ///
    virtual void purge()
    {
      Lock<Mutex> guard(_lock);
      _positionMap.clear();
      memset(_log, 0, _fileSize);
      MemoryBookmarkStore::_purge();
      _currentIndex = 0;
    }

    ///
    /// Called to purge the contents of this store for particular subId.
    /// Removes any tracking history associated with publishers and received
    /// messages, and will remove the subId from the file as well.
    ///
    virtual void purge(const Message::Field& subId_)
    {
      Lock<Mutex> guard(_lock);
      Lock<Mutex> fileGuard(_fileLock);
      Lock<Mutex> posGuard(_posLock);
      if (_positionMap.count(subId_) == 0)
      {
        return;
      }
      // Remove from memory
      MemoryBookmarkStore::_purge(subId_);
      // Remove from the file
      SubscriptionPosition pos = _positionMap[subId_];
      memset(_log + (pos._index * AMPS_RING_ENTRY_SIZE), 0,
             AMPS_RING_ENTRY_SIZE);
      // Move any following subs back an index
      Message::Field sub;
      for (size_t index = pos._index; index < _currentIndex - 1; ++index)
      {
        char* start = _log + (index * AMPS_RING_ENTRY_SIZE);
        memcpy(start, start + AMPS_RING_ENTRY_SIZE, AMPS_RING_ENTRY_SIZE);
        char* end = (char*)memchr(start, '\0', AMPS_RING_BYTES_SUBID);
        if (!end)
        {
          break;
        }
        sub.assign(start, (size_t)(end - start));
        _positionMap[sub]._index = index;
      }
      _positionMap.erase(subId_);
      // We have one less sub
      --_currentIndex;
      // Clear the end
      memset(_log + (_currentIndex * AMPS_RING_ENTRY_SIZE), 0,
             AMPS_RING_ENTRY_SIZE);
    }

  private:
    void init(const char* fileName_)
    {
#ifdef _WIN32
      _file = CreateFileA(fileName_, GENERIC_READ | GENERIC_WRITE, 0,
                          NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
      if ( _file == INVALID_HANDLE_VALUE )
      {
        DWORD err = getErrorNo();
        std::ostringstream os;
        os << "Failed to create file " << fileName_ << " for RingBookmarkStore\n";
        error(os.str(), err);
      }
      LARGE_INTEGER liFileSize;
      if (GetFileSizeEx(_file, &liFileSize) == 0)
      {
        error("Failure getting file size for RingBookmarkStore.", getErrorNo());
      }
      DWORD fsLow = liFileSize.LowPart;
      DWORD fsHigh = liFileSize.HighPart;
#ifdef _WIN64
      size_t fileSize = liFileSize.QuadPart;
#else
      size_t fileSize = liFileSize.LowPart;
#endif
      size_t existingSize = AMPS_RING_ENTRIES * AMPS_RING_ENTRY_SIZE;
      if (existingSize > fileSize)
      {
        fsLow = (DWORD)existingSize;
#ifdef _WIN64
        fsHigh = (DWORD)(existingSize >> 32);
#endif
      }
      setFileSize(fsHigh, fsLow);
#else
      _fd = open(fileName_, O_RDWR | O_CREAT, (mode_t)0644);
      if (_fd == -1)
      {
        int err = getErrorNo();
        std::ostringstream os;
        os << "Failed to open log file " << fileName_ << " for RingBookmarkStore";
        error(os.str(), err);
      }
      struct stat statBuf;
      if (fstat(_fd, &statBuf) == -1)
      {
        int err = getErrorNo();
        std::ostringstream os;
        os << "Failed to stat log file " << fileName_ << " for RingBookmarkStore";
        error(os.str(), err);
      }
      size_t fSize = (size_t)statBuf.st_size;
      if (fSize == 0)
        if (::write(_fd, "\0\0\0\0", 4) != 4)
        {
          error("Failed to initialize empty file.", getErrorNo());
        }
      setFileSize((fSize > AMPS_RING_ENTRIES * AMPS_RING_ENTRY_SIZE ?
                   fSize - 1 : AMPS_RING_ENTRIES * AMPS_RING_ENTRY_SIZE));
#endif
      recover();
    }

#ifdef _WIN32
    DWORD getErrorNo() const
    {
      return GetLastError();
    }

    void error(const std::string& message_, DWORD err)
    {
      std::ostringstream os;
      static const DWORD msgSize = 2048;
      char pMsg[msgSize];
      DWORD sz = FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM |
                                FORMAT_MESSAGE_ARGUMENT_ARRAY,
                                NULL, err, LANG_NEUTRAL,
                                pMsg, msgSize, NULL);
      os << message_ << ". Error is " << pMsg;
      throw StoreException(os.str());
    }
#else
    int getErrorNo() const
    {
      return errno;
    }

    void error(const std::string& message_, int err)
    {
      std::ostringstream os;
      os << message_ << ". Error is " << strerror(err);
      throw StoreException(os.str());
    }
#endif

    // Used to log the new _mostRecent bookmark
    void write(const Message::Field& subId_,
               const Message::Field& bookmark_)
    {
      Lock<Mutex> guard(_fileLock);
      if ( !_ringRecovering)
      {
        if (bookmark_.len() > AMPS_RING_BYTES_BOOKMARK)
        {
          throw StoreException("Bookmark is too large for fixed size storage. Consider rebuilding after changing AMPS_RING_BYTES_BOOKMARK in include/RingBookmarkStore.hpp");
        }
        SubscriptionPosition& pos = findPos(subId_);
        size_t nextPos = (pos._current + 1) % AMPS_RING_POSITIONS;
        // Get pointer to start of next position for cursor
        char* offset = _log + (pos._index * AMPS_RING_ENTRY_SIZE) + AMPS_RING_BYTES_SUBID + (nextPos * AMPS_RING_BYTES_BOOKMARK);
        // Write the 'cursor' to start of following entry and advance offset
        *offset = '*';
        // Change offset to beginning of current bookmark
        offset = _log + ((pos._index * AMPS_RING_ENTRY_SIZE) + AMPS_RING_BYTES_SUBID + (pos._current * AMPS_RING_BYTES_BOOKMARK) + 1);
        size_t len = bookmark_.len();
        // Write the bookmark and advance offset
        memcpy(offset, static_cast<const void*>(bookmark_.data()), len);
        offset += len;
        // Set extra bytes to NULL
        memset(offset, 0, AMPS_RING_BYTES_BOOKMARK - (len + 2));
        // Return to beginning and change the cursor
        offset = offset - len - 1;
        *offset = '+';
        // Update current for the next write
        pos._current = nextPos;

        // Sync the changes to disk
#ifdef _WIN32
#ifdef _WIN64
        size_t syncStart = (pos._index * AMPS_RING_ENTRY_SIZE) & ~((getPageSize() - 1) & 0xFFFFFFFFFFFFFFFF);
#else
        size_t syncStart = (pos._index * AMPS_RING_ENTRY_SIZE) & (size_t)~(getPageSize() - 1);
#endif
        if (!FlushViewOfFile(_log + syncStart, (pos._index * AMPS_RING_ENTRY_SIZE) - syncStart + AMPS_RING_ENTRY_SIZE))
#else
        size_t syncStart = (pos._index * AMPS_RING_ENTRY_SIZE) & ~(getPageSize() - 1);
        if (msync(_log + syncStart, (pos._index * AMPS_RING_ENTRY_SIZE) - syncStart + AMPS_RING_ENTRY_SIZE, MS_ASYNC) != 0)
#endif
        {
          error("Failed to sync mapped memory", getErrorNo());
        }
      }
    }

#ifdef _WIN32
    void setFileSize(DWORD newSizeHigh_, DWORD newSizeLow_)
    {
      bool remap = (_mapFile && _mapFile != INVALID_HANDLE_VALUE);
      if (remap)
      {
        UnmapViewOfFile(_log);
        CloseHandle(_mapFile);
        _positionMap.clear();
      }
      _mapFile = CreateFileMappingA( _file, NULL, PAGE_READWRITE, newSizeHigh_, newSizeLow_, NULL);
      if (_mapFile == NULL || _mapFile == INVALID_HANDLE_VALUE)
      {
        error("Failed to create map of log file", getErrorNo());
        _log = 0;
        _fileSize = 0;
      }
#ifdef _WIN64
      size_t sz = ((size_t)newSizeHigh_ << 32) | (size_t)newSizeLow_;
#else
      size_t sz = (size_t)newSizeLow_;
#endif
      _log = (char*)MapViewOfFile(_mapFile, FILE_MAP_ALL_ACCESS, 0, 0, sz);
      if (_log == NULL)
      {
        error("Failed to map log file to memory", getErrorNo());
        _log = 0;
        _fileSize = 0;
        return;
      }
      _fileSize = sz;
      // Call recover to reset the _positionMap
      if (remap)
      {
        recover();
      }
    }
#else
    void setFileSize(size_t newSize_)
    {
      bool remap = (_log != 0);
      // Make sure we're using a multiple of page size
      size_t sz = newSize_ & (size_t)(~(getPageSize() - 1));
      if (sz < newSize_)
      {
        sz += getPageSize();
      }
      // Improper resize attempt
      if (newSize_ <= _fileSize)
      {
        return;
      }
      // Extend the underlying file
      if (lseek(_fd, (off_t)sz, SEEK_SET) == -1)
      {
        error("Seek failed for RingBookmarkStore", getErrorNo());
      }
      if (::write(_fd, "", 1) == -1)
      {
        error("Failed to grow RingBookmarkStore", getErrorNo());
      }
      void* newLog = MAP_FAILED;
      if (_log)
      {
        _positionMap.clear();

#ifdef linux
        newLog = (mremap(_log, _fileSize, sz, MREMAP_MAYMOVE));
#else
        // try making a new mmap right after the current one.
        newLog = mmap(_log + _fileSize, sz, PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_FIXED, _fd, (off_t)sz);
        if (newLog != _log)
        {
          // this mmap is relatively small; better to just close the old mmap and reset.
          munmap(_log, _fileSize);
          newLog = mmap(0, sz, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
        }
#endif
      }
      else // New mapping
      {
        // New mapping, map the full file size for recovery or else it std size
        newLog = (mmap(0, sz, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0));
      }
      _fileSize = sz;

      if (newLog == MAP_FAILED)
      {
        _log = 0;
        _fileSize = 0;
        error("Failed to map log file to memory", getErrorNo());
      }
      _log = static_cast<char*>(newLog);
      if (remap)
      {
        recover();
      }
    }
#endif

    void recover(void)
    {
      //Lock<Mutex> guard(_lock);
      _ringRecovering = true;
      Message::Field sub;
      Message::Field bookmarkField;

      _currentIndex = 0;
      size_t maxEntries = _fileSize / AMPS_RING_ENTRY_SIZE > AMPS_RING_ENTRIES ? _fileSize / AMPS_RING_ENTRY_SIZE : AMPS_RING_ENTRIES;
      for (; _currentIndex < maxEntries; ++_currentIndex)
      {
        char* offset = _log + (_currentIndex * AMPS_RING_ENTRY_SIZE);
        if (*offset == '\0')
        {
          break;
        }
        //It's possible we wrote the subId and not NULLs, so be careful
        char* end = (char*)memchr(offset, '\0', AMPS_RING_BYTES_SUBID);
        if (!end)
        {
          // Failed subscription id read, we're done
          break;
        }
        // Safe to continue
        sub.assign(offset, (size_t)(end - offset));
        // Put this sub into the MemoryBookmarkStore
        Subscription* subPtr = MemoryBookmarkStore::find(sub);
        // Put this sub into the _positionMap
        // This is recovery, so do it directly and not with findPos
        SubscriptionPosition& pos = _positionMap[sub];
        pos._index = _currentIndex;
        offset += AMPS_RING_BYTES_SUBID;
        size_t foundCursor = AMPS_RING_POSITIONS;
        for (pos._current = 0; pos._current < AMPS_RING_POSITIONS; pos._current++)
        {
          if (offset[pos._current * AMPS_RING_BYTES_BOOKMARK] == '*')
          {
            // Subtract one position
            pos._current = (pos._current + (AMPS_RING_POSITIONS - 1)) % AMPS_RING_POSITIONS;
            // Subtract one more if a second bookmark is found
            if (offset[foundCursor * AMPS_RING_BYTES_BOOKMARK] == '*')
            {
              pos._current = (pos._current + (AMPS_RING_POSITIONS - 1)) % AMPS_RING_POSITIONS;
            }
            break;
          }
        }
        if (pos._current >= AMPS_RING_POSITIONS)
        {
          // No valid bookmark found, just use 0
          pos._current = 0;
        }
        else
        {
          // We found a cursor
          offset += pos._current * AMPS_RING_BYTES_BOOKMARK;
          //It's possible we wrote bookmark and not NULLs, so be careful
          end = (char*)memchr(offset, '\0', AMPS_RING_BYTES_BOOKMARK);
          if (end && end != offset)
          {
            // add 1 to account for leading '+'
            bookmarkField.assign(offset + 1, (size_t)(end - offset - 1));
            // log, discard to make it the most recent
            if (!BookmarkRange::isRange(bookmarkField))
            {
              // This adds bookmark to _publishers
              subPtr->isDiscarded(bookmarkField);
              subPtr->discard(subPtr->log(bookmarkField));
            }
            else
            {
              subPtr->log(bookmarkField);
            }
          }
        }
      }
      _ringRecovering = false;
    }

    SubscriptionPosition& findPos(const Message::Field& subId_)
    {
      Lock<Mutex> guard(_posLock);
      if (_positionMap.count(subId_) == 0)
      {
        // New subid
        // Move to its start position and write the sub id
        char* offset = _log + (_currentIndex * AMPS_RING_ENTRY_SIZE);
        size_t len = subId_.len();
        memcpy(offset, static_cast<const void*>(subId_.data()), len);
        // Add it to the map with the current index
        // Use the data written to the mmap for the subid
        Message::Field subId;
        subId.assign(offset, len);
        _positionMap[subId]._index = _currentIndex;
        _positionMap[subId]._current = 0;
        // Fill extra spaces with NULL
        offset += len;
        memset(offset, 0, AMPS_RING_BYTES_SUBID - len);
        // Advance current index
        ++_currentIndex;
      }
      return _positionMap[subId_];
    }

    Subscription* find(const Message::Field& subId_)
    {
      if (subId_.empty())
      {
        throw StoreException("A valid subscription ID must be provided to the RingBookmarkStore");
      }
      findPos(subId_);
      return MemoryBookmarkStore::find(subId_);
    }


    Mutex _fileLock;
    size_t _fileSize;
    size_t _currentIndex;
    char* _log;
#ifdef _WIN32
    HANDLE _file;
    HANDLE _mapFile;
#else
    int _fd;
#endif
    Mutex _posLock;
    typedef std::map<Message::Field, SubscriptionPosition, Message::Field::FieldHash> PositionMap;
    PositionMap _positionMap;
    bool _ringRecovering;
#ifdef _WIN32
    static DWORD getPageSize()
    {
      static DWORD pageSize = 0;
      if (pageSize == 0)
      {
        SYSTEM_INFO SYS_INFO;
        GetSystemInfo(&SYS_INFO);
        pageSize = SYS_INFO.dwPageSize;
#else
    static size_t getPageSize()
    {
      static size_t pageSize = 0UL;
      if (pageSize == 0)
      {
        pageSize = (size_t)sysconf(_SC_PAGESIZE);
#endif
      }
      return pageSize;
    }


  };

} // end namespace AMPS


#endif // _RINGBOOKMARKSTORE_H_

