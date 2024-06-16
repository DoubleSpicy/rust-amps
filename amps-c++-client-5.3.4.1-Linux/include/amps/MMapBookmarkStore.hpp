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

#ifndef _MMAPBOOKMARKSTORE_H_
#define _MMAPBOOKMARKSTORE_H_

#include <amps/MemoryBookmarkStore.hpp>
#include <amps/RecoveryPoint.hpp>
#include <amps/RecoveryPointAdapter.hpp>
#ifdef _WIN32
  #include <windows.h>
#else
  #include <sys/mman.h>
  #include <unistd.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <set>

#define AMPS_INITIAL_LOG_SIZE  40960UL

/// \file MMapBookmarkStore.hpp
/// \brief Provides AMPS::MMapBookmarkStore, a bookmark store that
/// uses a memory mapped file to provide efficient message tracking that
/// persists across application restarts.

namespace AMPS
{
///
/// A BookmarkStoreImpl implementation that uses a memory mapped file for
/// storage of the bookmarks. This implementation provides efficient
/// bookmark tracking that persists across application restarts.
  class MMapBookmarkStore : public MemoryBookmarkStore
  {
  private:
#ifdef _WIN32
    typedef HANDLE FileType;
    HANDLE _mapFile;
#else
    typedef int FileType;
#endif
    static void _clearBookmark(std::pair<const Message::Field, size_t>& pair)
    {
      Message::Field f(pair.first);
      f.clear();
    }

  public:
    ///
    /// Create an MMapBookmarkStore that uses fileName_ as its file storage.
    /// If the file already exists and has bookmarks stored, they will be
    /// recovered to determine the most recent bookmark and any messages
    /// since then that have been discarded.
    /// \param fileName_ The name of the file to use for storage.
    /// \param useLastModifiedTime_ If the file already exists, the last
    /// modification time of the file will be included in the bookmark
    /// list returned from `getMostRecent` for any subscription ID that exists
    /// in the file upon recovery.
    MMapBookmarkStore(const char* fileName_, bool useLastModifiedTime_ = false)
      : MemoryBookmarkStore(), _fileName(fileName_), _fileSize(0)
      , _logOffset(0), _log(0), _fileTimestamp(0)
#ifdef _WIN32
      , _file(INVALID_HANDLE_VALUE), _mapFile(INVALID_HANDLE_VALUE)
#else
      , _file(0)
#endif
    {
      if (init(useLastModifiedTime_))
      {
        recover(useLastModifiedTime_, false);
      }
    }

    ///
    /// Create an MMapBookmarkStore that uses fileName_ as its file storage.
    /// If the file already exists and has bookmarks stored, they will be
    /// recovered to determine the most recent bookmark and any messages
    /// since then that have been discarded.
    /// \param fileName_ The name of the file to use.
    /// \param useLastModifiedTime_ If the file already exists, the last
    /// modification time of the file will be included in the bookmark
    /// list returned from `getMostRecent` for any subscription ID that exists
    /// in the file upon recovery.
    MMapBookmarkStore(const std::string& fileName_,
                      bool useLastModifiedTime_ = false)
      : MemoryBookmarkStore(), _fileName(fileName_), _fileSize(0)
      , _logOffset(0), _log(0), _fileTimestamp(0)
#ifdef _WIN32
      , _file(INVALID_HANDLE_VALUE), _mapFile(INVALID_HANDLE_VALUE)
#else
      , _file(0)
#endif
    {
      if (init(useLastModifiedTime_))
      {
        recover(useLastModifiedTime_, false);
      }
    }

    ///
    /// Create an MMapBookmarkStore that uses fileName_ as its file storage.
    /// If the file already exists and has bookmarks stored, they will be
    /// recovered to determine the most recent bookmark and any messages
    /// since then that have been discarded.
    /// \param adapter_ The {@link RecoveryPointAdapter} to notify
    /// about updates.
    /// \param fileName_ The name of the file to use.
    /// \param factory_ An optional factory function to use
    /// to create the {@link RecoveryPoint} objects sent to the recoveryPointAdapter_.
    /// \param useLastModifiedTime_ If the file already exists, the last
    /// modification time of the file will be included in the bookmark
    /// list returned from `getMostRecent` for any subscription ID that exists
    /// in the file upon recovery.
    MMapBookmarkStore(const RecoveryPointAdapter& adapter_,
                      const char* fileName_,
                      RecoveryPointFactory factory_ = NULL,
                      bool useLastModifiedTime_ = false)
      : MemoryBookmarkStore(adapter_, factory_)
      , _fileName(fileName_), _fileSize(0)
      , _logOffset(0), _log(0), _fileTimestamp(0)
#ifdef _WIN32
      , _file(INVALID_HANDLE_VALUE), _mapFile(INVALID_HANDLE_VALUE)
#else
      , _file(0)
#endif
    {
      if (init(useLastModifiedTime_))
      {
        recover(useLastModifiedTime_, true);
      }
    }

    ///
    /// Create an MMapBookmarkStore that uses fileName_ as its file storage.
    /// If the file already exists and has bookmarks stored, they will be
    /// recovered to determine the most recent bookmark and any messages
    /// since then that have been discarded.
    /// \param adapter_ The {@link RecoveryPointAdapter} to notify
    /// about updates.
    /// \param fileName_ The name of the file to use for storage.
    /// \param factory_ An optional factory function to use
    /// to create the {@link RecoveryPoint} objects sent to the recoveryPointAdapter_.
    /// \param useLastModifiedTime_ If the file already exists, the last
    /// modification time of the file will be included in the bookmark
    /// list returned from `getMostRecent` for any subscription ID that exists
    /// in the file upon recovery.
    MMapBookmarkStore(const RecoveryPointAdapter& adapter_,
                      const std::string& fileName_,
                      RecoveryPointFactory factory_ = NULL,
                      bool useLastModifiedTime_ = false)
      : MemoryBookmarkStore(adapter_, factory_)
      , _fileName(fileName_), _fileSize(0)
      , _logOffset(0), _log(0), _fileTimestamp(0)
#ifdef _WIN32
      , _file(INVALID_HANDLE_VALUE), _mapFile(INVALID_HANDLE_VALUE)
#else
      , _file(0)
#endif
    {
      if (init(useLastModifiedTime_))
      {
        recover(useLastModifiedTime_, true);
      }
    }

    virtual ~MMapBookmarkStore()
    {
#ifdef _WIN32
      UnmapViewOfFile(_log);
      CloseHandle(_mapFile);
      CloseHandle(_file);
#else
      munmap(_log, _fileSize);
      ::close(_file);
#endif
      // In case _lock gets acquired by reader thread between end of this
      // destructor and start of base class destructor, prevent write()
      _recovering = true;
    }

    ///
    /// Log a bookmark to the persistent log and return the corresponding
    /// sequence number for this bookmark.
    /// \param message_ The Message to log.
    ///
    virtual size_t log(Message& message_)
    {
      Message::Field bookmark = message_.getBookmark();
      Subscription* sub = (Subscription*)(message_.getSubscriptionHandle());
      Lock<Mutex> guard(_lock);
      if (!sub)
      {
        Message::Field subId = message_.getSubscriptionId();
        if (subId.empty())
        {
          subId = message_.getSubscriptionIds();
        }
        sub = find(subId);
        message_.setSubscriptionHandle(static_cast<amps_subscription_handle>(sub));
      }
      write(sub->id(), ENTRY_BOOKMARK, bookmark);
      return MemoryBookmarkStore::_log(message_);
    }

    ///
    /// Log a Message as discarded from the store.
    /// \param message_ The Message to discard.
    ///
    virtual void discard(const Message& message_)
    {
      Message::Field bookmark = message_.getBookmark();
      Message::Field subId = message_.getSubscriptionId();
      if (subId.empty())
      {
        subId = message_.getSubscriptionIds();
      }
      Lock<Mutex> guard(_lock);
      write(subId, ENTRY_DISCARD, bookmark);
      MemoryBookmarkStore::_discard(message_);
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
      Subscription::Entry* entry = find(subId_)->getEntryByIndex(bookmarkSeqNo_);
      if (!entry || entry->_val.empty())
      {
        return;
      }
      write(subId_, ENTRY_DISCARD, entry->_val);
      MemoryBookmarkStore::_discard(subId_, bookmarkSeqNo_);
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
    /// Called for each arriving message to determine if
    /// the application has already seen this bookmark and should
    /// not be reprocessed.  Returns 'true' if the bookmark is
    /// in the log and should not be re-processed, false otherwise.
    /// \param message_ The Message to check.
    /// \return Whether or not the Message has been discarded from this store.
    ///
    virtual bool isDiscarded(Message& message_)
    {
      Lock<Mutex> l(_lock);
      bool retVal = MemoryBookmarkStore::_isDiscarded(message_);
      if (retVal)
      {
        Message::Field subId = message_.getSubscriptionId();
        if (subId.empty())
        {
          subId = message_.getSubscriptionIds();
        }
        write(subId, ENTRY_BOOKMARK, message_.getBookmark());
        write(subId, ENTRY_DISCARD, message_.getBookmark());
      }
      return retVal;
    }

    ///
    /// Called to purge the contents of this store.
    /// Removes any tracking history associated with publishers and received
    /// messages, and may delete or truncate on-disk representations as well.
    ///
    virtual void purge()
    {
      Lock<Mutex> guard(_lock);
      Lock<Mutex> fileGuard(_fileLock);
      memset(_log, 0, _logOffset);
      _logOffset = 0;
      MemoryBookmarkStore::_purge();
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
      MemoryBookmarkStore::_purge(subId_);
      std::string tmpFileName = _fileName + ".tmp";
      __prune(tmpFileName);
    }

    void setServerVersion(const VersionInfo& version_)
    {
      Lock<Mutex> guard(_lock);
      MemoryBookmarkStore::setServerVersion(version_);
    }

    void setServerVersion(size_t version_)
    {
      Lock<Mutex> guard(_lock);
      MemoryBookmarkStore::setServerVersion(version_);
    }

    // Yes, the argument is a non-const copy of what is passed in
    void _prune(const std::string& tmpFileName_)
    {
      Lock<Mutex> guard(_lock);
      Lock<Mutex> fileGuard(_fileLock);
      // If nothing's changed with most recent, don't rewrite the file
      if (!_recentChanged)
      {
        return;
      }
      if (tmpFileName_.empty())
      {
        __prune(_fileName + ".tmp");
      }
      else
      {
        __prune(tmpFileName_);
      }
      _recentChanged = false;
    }

  private:
    void __prune(const std::string& tmpFileName_)
    {
      size_t sz = AMPS_INITIAL_LOG_SIZE;
      FileType file;
      char* log = NULL;
      size_t bytesWritten = 0;
#ifdef _WIN32
      file = CreateFileA(tmpFileName_.c_str(), GENERIC_READ | GENERIC_WRITE, 0,
                         NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
      if ( file == INVALID_HANDLE_VALUE )
      {
        DWORD err = getErrorNo();
        std::ostringstream os;
        os <<  "Failed to create temp store file " << tmpFileName_ <<
           " to prune MMapBookmarkStore " << _fileName;
        error(os.str(), err);
      }
      HANDLE mapFile = NULL;
      try
      {
        sz = _setFileSize(sz, &log, file, &mapFile);
      }
      catch (StoreException& ex)
      {
        if (mapFile == NULL || mapFile == INVALID_HANDLE_VALUE)
        {
          CloseHandle(file);
          std::ostringstream os;
          os << "Failed to create map of temp file " << tmpFileName_
             << " while resizing it to prune MMapBookmarkStore " << _fileName
             << ": " << ex.what();
          throw StoreException(os.str());
          return;
        }
        if (log == NULL)
        {
          CloseHandle(mapFile);
          CloseHandle(file);
          std::ostringstream os;
          os << "Failed to map temp file " << tmpFileName_
             << " to memory while resizing it to prune MMapBookmarkStore "
             << _fileName << ": " << ex.what();
          throw StoreException(os.str());
          return;
        }
      }
      if (sz == 0)
      {
        DWORD err = getErrorNo();
        UnmapViewOfFile(log);
        CloseHandle(mapFile);
        CloseHandle(file);
        std::ostringstream os;
        os << "Failed to grow tmp file " << tmpFileName_
           << " to prune MMapBookmarkStore " << _fileName;
        error(os.str(), err);
      }
#else
      file = open(tmpFileName_.c_str(), O_RDWR | O_CREAT, (mode_t)0644);
      if (file == -1)
      {
        int err = getErrorNo();
        std::ostringstream os;
        os <<  "Failed to create temp store file " << tmpFileName_ <<
           " to prune MMapBookmarkStore " << _fileName;
        error(os.str(), err);
        return;
      }
      if (::write(file, "\0\0\0\0", 4) == -1)
      {
        int err = getErrorNo();
        std::ostringstream os;
        os << "Failed to write header to temp file " << tmpFileName_
           << " to prune MMapBookmarkStore " << _fileName;
        error(os.str(), err);
        return;
      }
      try
      {
        sz = _setFileSize(sz, &log, file, 0);
      }
      catch (StoreException& ex)
      {
        std::ostringstream os;
        os << "Failed to grow tmp file " << tmpFileName_
           << " to prune MMapBookmarkStore " << _fileName << ex.what();
        throw StoreException(os.str());
      }
      if (sz == 0)
      {
        int err = getErrorNo();
        log = NULL;
        ::close(file);
        std::ostringstream os;
        os << "Failed to grow tmp file " << tmpFileName_
           << " to prune MMapBookmarkStore " << _fileName;
        error(os.str(), err);
      }
#endif
      try
      {
        for (SubscriptionMap::iterator i = _subs.begin(); i != _subs.end(); ++i)
        {
          Message::Field subId = i->first;
          assert(!subId.empty());
          size_t subIdLen = subId.len();
          Subscription* mapSubPtr = i->second;
          const BookmarkRange& range = mapSubPtr->getRange();
          if (range.isValid())
          {
            write(&log, &bytesWritten, subId, ENTRY_BOOKMARK, range);
          }
          Message::Field recent = mapSubPtr->getMostRecent(false);
          amps_uint64_t recentPub, recentSeq;
          Subscription::parseBookmark(recent, recentPub, recentSeq);
          Subscription::PublisherMap publishersDiscarded =
            mapSubPtr->_publishers;
          MemoryBookmarkStore::EntryPtrList recovered;
          mapSubPtr->getRecoveryEntries(recovered);
          mapSubPtr->setPublishersToDiscarded(&recovered,
                                              &publishersDiscarded);
          char tmpBookmarkBuffer[128];
          for (Subscription::PublisherIterator pub =
                 publishersDiscarded.begin(),
               e = publishersDiscarded.end();
               pub != e; ++pub)
          {
            // Don't log EPOCH if it got in the map
            if (pub->first == 0 || pub->second == 0)
            {
              continue;
            }
            // Don't log the most recent yet
            if (pub->first == recentPub)
            {
              continue;
            }
            int written = AMPS_snprintf_amps_uint64_t(
                            tmpBookmarkBuffer,
                            sizeof(tmpBookmarkBuffer),
                            pub->first);
            *(tmpBookmarkBuffer + written++) = '|';
            written += AMPS_snprintf_amps_uint64_t(
                         tmpBookmarkBuffer + written,
                         sizeof(tmpBookmarkBuffer)
                         - (size_t)written,
                         pub->second);
            *(tmpBookmarkBuffer + written++) = '|';
            Message::Field tmpBookmark(tmpBookmarkBuffer, (size_t)written);
            // Check we'll be in the current boundaries
            size_t blockLen = subIdLen + 2 * sizeof(size_t) + tmpBookmark.len() + 1;
            if (bytesWritten + blockLen + blockLen >= sz)
            {
#ifdef _WIN32
              sz = _setFileSize(sz * 2, &log, file, &mapFile);
#else
              sz = _setFileSize(sz * 2, &log, file, sz);
#endif
            }
            write(&log, &bytesWritten, subId, ENTRY_BOOKMARK, tmpBookmark);
            write(&log, &bytesWritten, subId, ENTRY_DISCARD, tmpBookmark);
          }
          if (isWritableBookmark(recent.len()))
          {
            // Check we'll be in the current boundaries
            size_t blockLen = subIdLen + 2 * sizeof(size_t) + recent.len() + 1;
            if (bytesWritten + blockLen + blockLen >= sz)
            {
#ifdef _WIN32
              sz = _setFileSize(sz * 2, &log, file, &mapFile);
#else
              sz = _setFileSize(sz * 2, &log, file, sz);
#endif
            }
            write(&log, &bytesWritten, subId, ENTRY_BOOKMARK, recent);
            write(&log, &bytesWritten, subId, ENTRY_DISCARD, recent);
          }
          else // set up _recentList
          {
            mapSubPtr->getMostRecentList();
          }
          Message::Field bookmark = mapSubPtr->getLastPersisted();
          if (isWritableBookmark(bookmark.len()))
          {
            // Check we'll be in the current boundaries
            size_t blockLen = subIdLen + 2 * sizeof(size_t) +
                              bookmark.len() + 1;
            if (bytesWritten + blockLen >= sz)
            {
#ifdef _WIN32
              sz = _setFileSize(sz * 2, &log, file, &mapFile);
#else
              sz = _setFileSize(sz * 2, &log, file, sz);
#endif
            }
            write(&log, &bytesWritten, subId, ENTRY_PERSISTED,
                  mapSubPtr->getLastPersisted());
          }
          mapSubPtr->getActiveEntries(recovered);
          for (MemoryBookmarkStore::EntryPtrList::iterator entry =
                 recovered.begin();
               entry != recovered.end(); ++entry)
          {
            if ((*entry)->_val.empty() ||
                !isWritableBookmark((*entry)->_val.len()))
            {
              continue;
            }
            // Check we'll be in the current boundaries
            size_t blockLen = subIdLen + 2 * sizeof(size_t) +
                              (*entry)->_val.len() + 1;
            if (bytesWritten + blockLen >= sz)
            {
#ifdef _WIN32
              sz = _setFileSize(sz * 2, &log, file, &mapFile);
#else
              sz = _setFileSize(sz * 2, &log, file, sz);
#endif
            }
            write(&log, &bytesWritten, subId, ENTRY_BOOKMARK,
                  (*entry)->_val);
            if (!(*entry)->_active)
            {
              // Check we'll be in the current boundaries
              if (bytesWritten + blockLen >= sz)
              {
#ifdef _WIN32
                sz = _setFileSize(sz * 2, &log, file, &mapFile);
#else
                sz = _setFileSize(sz * 2, &log, file, sz);
#endif
              }
              write(&log, &bytesWritten, subId, ENTRY_DISCARD,
                    (*entry)->_val);
            }
          }
        }
      }
      catch (StoreException& ex)
      {
#ifdef _WIN32
        UnmapViewOfFile(log);
        CloseHandle(mapFile);
        CloseHandle(file);
#else
        ::close(file);
        ::unlink(tmpFileName_.c_str());
#endif
        std::ostringstream os;
        os << "Exception during prune: " << ex.what();
        throw StoreException(os.str());
      }
#ifdef _WIN32
      BOOL success = FlushViewOfFile(_log, 0);
      success |= UnmapViewOfFile(_log);
      _log = NULL;
      success |= CloseHandle(_mapFile);
      success |= CloseHandle(_file);
      if (!success)
      {
        DWORD err = getErrorNo();
        std::ostringstream os;
        os << "Failed to flush, unmap, and close current file "
           << _fileName
           << " in prune in MMapBookmarkStore. ";
        error(os.str(), err);
      }
      _mapFile = INVALID_HANDLE_VALUE;
      _file = INVALID_HANDLE_VALUE;
      success = FlushViewOfFile(log, 0);
      success |= UnmapViewOfFile(log);
      log = NULL;
      success |= CloseHandle(mapFile);
      success |= CloseHandle(file);
      if (!success)
      {
        DWORD err = getErrorNo();
        std::ostringstream os;
        os << "Failed to flush, unmap and close completed temp file "
           << tmpFileName_
           << " in prune in MMapBookmarkStore. ";
        error(os.str(), err);
      }
      mapFile = INVALID_HANDLE_VALUE;
      file = INVALID_HANDLE_VALUE;
      // Replace current file with pruned file
      int retryCount = 3;
      while (!MoveFileExA(tmpFileName_.c_str(), _fileName.c_str(),
                          MOVEFILE_COPY_ALLOWED | MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
      {
        DWORD err = getErrorNo();
        if (--retryCount > 0)
        {
          continue;
        }
        // Try to set _file to the tmp file that won't move then throw
        std::string desiredFileName = _fileName;
        _fileName = tmpFileName_;
        init();
        std::ostringstream os;
        os << "Failed to move completed temp file " << tmpFileName_
           << " to " << desiredFileName
           << " in prune in MMapBookmarkStore. Continuing by using "
           << tmpFileName_ << " as the MMapBookmarkStore file.";
        error(os.str(), err);
      }
      // Call init to set up file again
      init();
#else
      munmap(_log, _fileSize);
      _log = NULL;
      ::close(_file);
      munmap(log, sz);
      ::close(file);
      if (-1 == ::unlink(_fileName.c_str()))
      {
        int err = getErrorNo();
        // Try to set _file to the tmp file that won't move then throw
        std::string desiredFileName = _fileName;
        _fileName = tmpFileName_;
        init();
        std::ostringstream os;
        os << "Failed to delete file " << desiredFileName
           << " after creating temporary file " << tmpFileName_
           << " in prune in MMapBookmarkStore. Continuing by using "
           << tmpFileName_ << " as the MMapBookmarkStore file.";
        error(os.str(), err);
      }
      if (-1 == ::rename(tmpFileName_.c_str(), _fileName.c_str()))
      {
        int err = getErrorNo();
        // Try to set _file to the tmp file that won't move then throw
        std::string desiredFileName = _fileName;
        _fileName = tmpFileName_;
        init();
        std::ostringstream os;
        os << "Failed to move completed temp file " << tmpFileName_
           << " to " << desiredFileName
           << " in prune in MMapBookmarkStore. Continuing by using "
           << tmpFileName_ << " as the MMapBookmarkStore file.";
        error(os.str(), err);
      }
      // Call init to set up file again
      init();
#endif
      _logOffset = bytesWritten;
    }

    virtual void _persisted(Subscription* subP_,
                            const Message::Field& bookmarkField_)
    {
      Lock<Mutex> l(_lock);
      write(subP_->id(), ENTRY_PERSISTED, bookmarkField_);
      MemoryBookmarkStore::_persisted(subP_, bookmarkField_);
    }

    virtual Message::Field _persisted(Subscription* subP_, size_t bookmark_)
    {
      Lock<Mutex> l(_lock);
      Subscription::Entry* entryPtr = subP_->getEntryByIndex(bookmark_);
      if (!entryPtr || entryPtr->_val.empty())
      {
        return Message::Field();
      }
      Message::Field bookmarkField = entryPtr->_val;
      write(subP_->id(), ENTRY_PERSISTED, bookmarkField);
      MemoryBookmarkStore::_persisted(subP_, bookmarkField);
      return bookmarkField;
    }

    // Returns true if file exists and is larger than 0 bytes and therefore
    // should be used for recovery.
    bool init(bool useLastModifiedTime_ = false)
    {
      bool retVal = true;
#ifdef _WIN32
      _file = CreateFileA(_fileName.c_str(), GENERIC_READ | GENERIC_WRITE, 0,
                          NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
      if ( _file == INVALID_HANDLE_VALUE )
      {
        DWORD err = getErrorNo();
        std::ostringstream os;
        os << "Failed to initialize file " << _fileName << " for MMapBookmarkStore";
        error(os.str(), err);
      }
      LARGE_INTEGER liFileSize;
      if (GetFileSizeEx(_file, &liFileSize) == 0)
      {
        DWORD err = getErrorNo();
        CloseHandle(_file);
        std::ostringstream os;
        os << "Failure getting initial file size for MMapBookmarkStore " << _fileName;
        error(os.str(), err);
        return false;
      }
#ifdef _WIN64
      size_t fileSize = liFileSize.QuadPart;
#else
      size_t fileSize = liFileSize.LowPart;
#endif
      if (useLastModifiedTime_ && fileSize > 0)
      {
        FILETIME ftModifiedTime;
        if (GetFileTime(_file, NULL, NULL, &ftModifiedTime) == 0)
        {
          DWORD err = getErrorNo();
          CloseHandle(_file);
          _recovering = false;
          error("Failure getting file time while trying to recover.", err);
          return false;
        }
        SYSTEMTIME st;
        if (FileTimeToSystemTime(&ftModifiedTime, &st) == 0)
        {
          DWORD err = getErrorNo();
          CloseHandle(_file);
          _recovering = false;
          error("Failure converting file time while trying to recover.", err);
          return false;
        }
        _fileTimestamp = new char[AMPS_TIMESTAMP_LEN];
        sprintf_s(_fileTimestamp, AMPS_TIMESTAMP_LEN,
                  "%04d%02d%02dT%02d%02d%02d", st.wYear, st.wMonth,
                  st.wDay, st.wHour, st.wMinute, st.wSecond);
        _fileTimestamp[AMPS_TIMESTAMP_LEN - 1] = 'Z';
      }
      retVal = (fileSize != 0);
      setFileSize( AMPS_INITIAL_LOG_SIZE > fileSize ?
                   AMPS_INITIAL_LOG_SIZE : fileSize);
#else
      _file = open(_fileName.c_str(), O_RDWR | O_CREAT, (mode_t)0644);
      if (_file == -1)
      {
        int err = getErrorNo();
        std::ostringstream os;
        os << "Failed to initialize log file " << _fileName << " for MMapBookmarkStore";
        error(os.str(), err);
      }
      struct stat statBuf;
      if (fstat(_file, &statBuf) == -1)
      {
        int err = getErrorNo();
        ::close(_file);
        std::ostringstream os;
        os << "Failed to stat log file " << _fileName << " for MMapBookmarkStore";
        error(os.str(), err);
        return false;
      }
      size_t fSize = (size_t)statBuf.st_size;
      if (fSize == 0)
      {
        retVal = false;
        if (::write(_file, "\0\0\0\0", 4) == -1)
        {
          int err = getErrorNo();
          ::close(_file);
          std::ostringstream os;
          os << "Failed to write header to log file " << _fileName
             << " for MMapBookmarkStore";
          error(os.str(), err);
          return false;
        }
      }
      else if (useLastModifiedTime_)
      {
        _fileTimestamp = new char[AMPS_TIMESTAMP_LEN];
        struct tm timeInfo;
        gmtime_r(&statBuf.st_mtime, &timeInfo);
        strftime(_fileTimestamp, AMPS_TIMESTAMP_LEN,
                 "%Y%m%dT%H%M%S", &timeInfo);
        _fileTimestamp[AMPS_TIMESTAMP_LEN - 1] = 'Z';
      }

      setFileSize((fSize > AMPS_INITIAL_LOG_SIZE) ? fSize - 1 : AMPS_INITIAL_LOG_SIZE);
#endif
      return retVal;
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
      os << "File: " << _fileName << ". " << message_ << " with error " << pMsg;
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
#if defined(sparc)
#define AMPS_WRITE8(p,v) { p[0] = (v>>56)&0xFF; p[1] = (v>>48)&0xFF; p[2] = (v>>40)&0xFF; p[3] = (v>>32)&0xFF; p[4] = (v>>24)&0xFF; p[5] = (v>>16)&0xFF; p[6] = (v>>8)&0xFF; p[7]=v&0xFF; }
#define AMPS_READ8(p, v) { memcpy(&v,p,8); }
#else
#define AMPS_WRITE8(p,v) { *(size_t*)p = (size_t)v; }
#define AMPS_READ8(p,v)  { v = *(const size_t*)p; }
#endif

    // This implementation will use this when logging a bookmark or a persisted
    void write(const Message::Field& subId_,
               char type_, const Message::Field& bookmark_)
    {
      Lock<Mutex> guard(_fileLock);
      write(&_log, &_logOffset, subId_, type_, bookmark_);
    }

    void write(char** logPtr, size_t* logOffsetPtr, const Message::Field& subId_,
               char type_, const Message::Field& bookmark_)
    {
      if (!_recovering && isWritableBookmark(bookmark_.len()))
      {
        size_t len = subId_.len();
        // Check we'll be in the current boundaries
        size_t blockLen = len + 2 * sizeof(size_t) + bookmark_.len() + 1;
        if (*logOffsetPtr + blockLen >= _fileSize)
        {
          setFileSize(_fileSize * 2);
        }
        char* offset = *logPtr + *logOffsetPtr;
        AMPS_WRITE8(offset, len);
        offset += sizeof(size_t);
        memcpy(offset, static_cast<const void*>(subId_.data()), len);
        offset += len;
        *offset++ = type_;
        len = bookmark_.len();
        AMPS_WRITE8(offset, len);
        offset += sizeof(size_t);
        memcpy(offset, static_cast<const void*>(bookmark_.data()), len);
        *logOffsetPtr += blockLen;
      }
    }

    // This implementation will only ever use this when discarding a bookmark
    // Could be used to add a feature where generated bookmarks are logged in
    // addition to the bookmark field.
    void write(const Message::Field& subId_, char type_, size_t bookmark_)
    {
      Lock<Mutex> guard(_fileLock);
      write(&_log, &_logOffset, subId_, type_, bookmark_);
    }

    void write(char** logPtr, size_t* logOffsetPtr, const Message::Field& subId_,
               char type_, size_t bookmark_)
    {
      if (!_recovering)
      {
        size_t len = subId_.len();
        size_t blockLen = len + 2 * sizeof(size_t) + 1;
        // Check we'll be in the current boundaries
        if (*logOffsetPtr + blockLen >= _fileSize)
        {
          setFileSize(_fileSize * 2);
        }
        char* offset = *logPtr + *logOffsetPtr;
        *(reinterpret_cast<size_t*>(offset)) = len;
        offset += sizeof(size_t);
        memcpy(offset, static_cast<const void*>(subId_.data()), len);
        offset += len;
        *offset++ = type_;
        *(reinterpret_cast<size_t*>(offset)) = bookmark_;
        *logOffsetPtr += blockLen;
      }
    }

    void setFileSize(size_t newSize_)
    {
      if (_log && newSize_ <= _fileSize) // Improper resize attempt
      {
        return;
      }
#ifdef _WIN32
      _fileSize = _setFileSize(newSize_, &_log, _file, &_mapFile);
#else
      _fileSize = _setFileSize(newSize_, &_log, _file, _fileSize);
#endif
    }

    // Returns new file size, 0 if there is a failure
    size_t _setFileSize(size_t newSize_, char** log_, FileType file_,
#ifdef WIN32
                        HANDLE* mapFile_
#else
                        size_t fileSize_
#endif
                       )
    {
      // Make sure we're using a multiple of page size
      size_t sz = newSize_ & (size_t)(~(getPageSize() - 1));
      if (sz < newSize_ || sz == 0)
      {
        sz += getPageSize();
      }
#ifdef _WIN32
      if (*mapFile_ && *mapFile_ != INVALID_HANDLE_VALUE)
      {
        if (*log_)
        {
          FlushViewOfFile(*log_, 0);
          UnmapViewOfFile(*log_);
        }
        CloseHandle(*mapFile_);
      }
#ifdef _WIN64
      *mapFile_ = CreateFileMapping( file_, NULL, PAGE_READWRITE, (DWORD)((sz >> 32) & 0xffffffff), (DWORD)sz, NULL);
#else
      *mapFile_ = CreateFileMapping( file_, NULL, PAGE_READWRITE, 0, (DWORD)sz, NULL);
#endif
      if (*mapFile_ == NULL || *mapFile_ == INVALID_HANDLE_VALUE)
      {
        DWORD errNo = getErrorNo();
        CloseHandle(file_);
        std::ostringstream os;
        os << "Failed to create map of MMapBookmarkStore file " << _fileName
           << " during resize.";
        error(os.str(), errNo);
        *log_ = 0;
        return 0;
      }
      else
      {
        *log_ = (char*)MapViewOfFile(*mapFile_, FILE_MAP_ALL_ACCESS, 0, 0, sz);
        if (*log_ == NULL)
        {
          DWORD errNo = getErrorNo();
          CloseHandle(*mapFile_);
          CloseHandle(file_);
          std::ostringstream os;
          os << "Failed to map MMapBookmarkStore file " << _fileName
             << " to memory during resize.";
          error(os.str(), errNo);
          *log_ = 0;
          return 0;
        }
      }
#else
      // Extend the underlying file
      if (lseek(file_, (off_t)sz, SEEK_SET) == -1)
      {
        int err = getErrorNo();
        ::close(file_);
        std::ostringstream os;
        os << "Failed to seek in MMapBookmarkStore file " << _fileName
           << " during resize.";
        error(os.str(), err);
      }
      if (::write(file_, "", 1) == -1)
      {
        int err = getErrorNo();
        ::close(file_);
        std::ostringstream os;
        os << "Failed to grow MMapBookmarkStore file " << _fileName
           << " during resize.";
        error(os.str(), err);
      }
      if (*log_)
      {
#if defined(linux)
        *log_ = static_cast<char*>(mremap(*log_, fileSize_, sz,
                                          MREMAP_MAYMOVE));
#else
        munmap(*log_, fileSize_);
        *log_ = static_cast<char*>(mmap(0, sz, PROT_READ | PROT_WRITE,
                                        MAP_SHARED, file_, 0));
#endif
      }
      else // New mapping
      {
        // New mapping, map the full file size for recovery or else it std size
        *log_ = static_cast<char*>(mmap(0, sz, PROT_READ | PROT_WRITE,
                                        MAP_SHARED, file_, 0));
      }

      if ((void*)(*log_) == MAP_FAILED)
      {
        int err = getErrorNo();
        ::close(file_);
        *log_ = 0;
        std::ostringstream os;
        os << "Failed to map MMapBookmarkStore file " << _fileName
           << " to memory during resize.";
        error(os.str(), err);
        return 0;
      }
#endif
      return sz;
    }

    void recover(bool useLastModifiedTime_ = false,
                 bool hasAdapter_ = false)
    {
      Message::Field sub;
      Message::Field bookmarkField;
      size_t bookmarkLen = 0;
      size_t lastGoodOffset = 0;
      bool inError = false;
      Lock<Mutex> guard(_lock);
      Lock<Mutex> fileGuard(_fileLock);
      _recovering = true;
      // Map of bookmark to sequence number
      typedef std::map<Message::Field, size_t, Message::Field::FieldHash> BookmarkMap;
      typedef std::map<Message::Field, size_t,
              Message::Field::FieldHash>::iterator BookmarkMapIter;
      // Map of subId to set of recovered bookmarks
      typedef std::map<Message::Field, BookmarkMap*,
              Message::Field::FieldHash> ReadMap;
      typedef std::map<Message::Field, BookmarkMap*,
              Message::Field::FieldHash>::iterator ReadMapIter;
      ReadMap recovered;
      size_t subLen = *(reinterpret_cast<size_t*>(_log));
      while (!inError && subLen > 0)
      {
        // If we recover something, remove anything adapter recovered
        if (_logOffset == 0 && hasAdapter_)
        {
          MemoryBookmarkStore::__purge();
        }
        _logOffset += sizeof(size_t);
        sub.assign(_log + _logOffset, subLen);
        _logOffset += subLen;
        switch (_log[_logOffset++])
        {
        case (char)-1:
          return;
        case ENTRY_BOOKMARK:
        {
          AMPS_READ8((_log + _logOffset), bookmarkLen);
          _logOffset += sizeof(size_t);
          bookmarkField.assign(_log + _logOffset, bookmarkLen);
          _logOffset += bookmarkLen;
          Subscription* subP = find(sub);
          BookmarkMap* bookmarks = NULL;
          ReadMapIter iter = recovered.find(sub);
          if (iter == recovered.end())
          {
            Message::Field subKey;
            subKey.deepCopy(sub);
            bookmarks = new BookmarkMap();
            recovered[subKey] = bookmarks;
          }
          else
          {
            bookmarks = iter->second;
          }
          if (bookmarks->find(bookmarkField) != bookmarks->end())
          {
            std::for_each(bookmarks->begin(), bookmarks->end(),
                          _clearBookmark);
            bookmarks->clear();
            subP->getMostRecent(true);
          }
          if (BookmarkRange::isRange(bookmarkField))
          {
            subP->log(bookmarkField);
          }
          else if (!subP->isDiscarded(bookmarkField))
          {
            size_t sequence = subP->log(bookmarkField);
            Message::Field copy;
            copy.deepCopy(bookmarkField);
            bookmarks->insert(std::make_pair(copy, sequence));
          }
          else
          {
            // We know it's discarded, but there may still be a
            // discard entry in the log, so avoid a search.
            Message::Field copy;
            copy.deepCopy(bookmarkField);
            bookmarks->insert(std::make_pair(copy, 0));
          }
        }
        break;
        case ENTRY_DISCARD:
        {
          AMPS_READ8((_log + _logOffset), bookmarkLen);
          _logOffset += sizeof(size_t);
          bookmarkField.assign(_log + _logOffset, bookmarkLen);
          _logOffset += bookmarkLen;
          size_t sequence = AMPS_UNSET_INDEX;
          ReadMapIter iter = recovered.find(sub);
          if (iter != recovered.end())
          {
            BookmarkMap* bookmarks = iter->second;
            BookmarkMapIter bookmarkIter = bookmarks->find(bookmarkField);
            if (bookmarkIter != bookmarks->end())
            {
              sequence = bookmarkIter->second;
              Message::Field bookmarkToClear(bookmarkIter->first);
              bookmarkToClear.clear();
              bookmarks->erase(bookmarkIter);
            }
          }
          if (!BookmarkRange::isRange(bookmarkField))
          {
            Subscription* subP = find(sub);
            if (sequence != AMPS_UNSET_INDEX)
            {
              // A sequence of 0 means it was already discarded
              if (sequence)
              {
                subP->discard(sequence);
              }
            }
            else // Shouldn't end up here, but just in case we'll search
            {
              subP->discard(bookmarkField);
            }
          }
        }
        break;
        case ENTRY_PERSISTED:
        {
          AMPS_READ8((_log + _logOffset), bookmarkLen);
          _logOffset += sizeof(size_t);
          bookmarkField.assign(_log + _logOffset, bookmarkLen);
          _logOffset += bookmarkLen;
          MemoryBookmarkStore::_persisted(find(sub), bookmarkField);
        }
        break;
        default:
          if (lastGoodOffset == 0)
          {
            error("Error while recovering MMapBookmarkStore file.", getErrorNo());
          }
          else
          {
            _logOffset = lastGoodOffset;
            inError = true;
          }
        }
        lastGoodOffset = _logOffset;
        if (!inError && _logOffset + 8 < _fileSize)
        {
          subLen = *(reinterpret_cast<size_t*>(_log + _logOffset));
        }
      }
      for (SubscriptionMap::iterator i = _subs.begin(); i != _subs.end(); ++i)
      {
        if (recovered.count(i->first) && !recovered[i->first]->empty())
        {
          if (i->second->getMostRecent(false).len() > 1)
          {
            i->second->justRecovered();
          }
          else
          {
            // Unlikely, but we may have recovered only undiscarded
            // bookmarks so just restart as a new subscription.
            delete i->second;
            _subs[i->first] = new Subscription(this, i->first);
          }
        }
        if (useLastModifiedTime_ && _fileTimestamp)
        {
          _subs[i->first]->setRecoveryTimestamp(_fileTimestamp);
        }
      }
      if (_fileTimestamp)
      {
        delete[] _fileTimestamp;
        _fileTimestamp = 0;
      }
      for (ReadMapIter i = recovered.begin(), e = recovered.end(); i != e; ++i)
      {
        std::for_each(i->second->begin(), i->second->end(), _clearBookmark);
        delete i->second;
        Message::Field f = i->first;
        f.clear();
      }
      _recovering = false;
    }

    Mutex _fileLock;
    std::string _fileName;
    size_t _fileSize;
    size_t _logOffset;
    char* _log;
    char* _fileTimestamp;
    FileType _file;
    // Each entry begins with a single byte indicating the type of entry:
    // a new bookmark, or a discard of a previous one.
    static size_t getPageSize()
    {
      static size_t pageSize;
      if (pageSize == 0)
      {
#ifdef _WIN32
        SYSTEM_INFO SYS_INFO;
        GetSystemInfo(&SYS_INFO);
        pageSize = SYS_INFO.dwPageSize;
#else
        pageSize = (size_t)sysconf(_SC_PAGESIZE);
#endif
      }
      return pageSize;
    }

  };

} // end namespace AMPS


#endif // _MMAPBOOKMARKSTORE_H_

