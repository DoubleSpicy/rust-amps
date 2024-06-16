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

#include <amps/ampsplusplus.hpp>

#ifndef _LOGGEDBOOKMARKSTORE_H_
#define _LOGGEDBOOKMARKSTORE_H_

#include <amps/MemoryBookmarkStore.hpp>
#include <amps/RecoveryPoint.hpp>
#include <amps/RecoveryPointAdapter.hpp>
#include <string>
#ifdef _WIN32
  #include <windows.h>
#else
  #include <sys/mman.h>
  #include <unistd.h>
  #include <sys/uio.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <map>
#include <set>

#if defined(sun)
  typedef char* amps_iovec_base_ptr;
#else
  typedef void* amps_iovec_base_ptr;
#endif

/// \file LoggedBookmarkStore.hpp
/// \brief Provides AMPS::LoggedBookmarkStore, a bookmark store that
/// uses a file to track received messages. This bookmark store provides
/// resumable subscriptions even when the application restarts.

namespace AMPS
{
///
/// A BookmarkStoreImpl implementation that logs all messages to a file. This
/// class provides resumable subscriptions even when the application restarts.
  class LoggedBookmarkStore : public MemoryBookmarkStore
  {
  private:
    static void _clearBookmark(std::pair<const Message::Field, size_t>& pair)
    {
      Message::Field f(pair.first);
      f.clear();
    }
#ifdef _WIN32
    typedef HANDLE FileType;
#else
    typedef int FileType;
#endif
  public:
    ///
    /// Creates a LoggedBookmarkStore using fileName_ as its file storage. If
    /// fileName_ already exists and has valid bookmarks in it, it will be
    /// recovered to determine the most recent bookmark and other messages
    /// received.
    /// \param fileName_ The name of the file to use.
    /// \param useLastModifiedTime_ If the file already exists, the last
    /// modification time of the file will be included in the bookmark
    /// list returned from `getMostRecent` for any subscription ID that exists
    /// in the file upon recovery.
    LoggedBookmarkStore(const char* fileName_,
                        bool useLastModifiedTime_ = false)
      : MemoryBookmarkStore()
#ifdef _WIN32
      , _file(INVALID_HANDLE_VALUE)
#else
      , _file(0)
#endif
      , _fileName(fileName_)
    {
      init();
      recover(useLastModifiedTime_, false);
    }

    ///
    /// Creates a LoggedBookmarkStore using a file name fileName_
    /// \param fileName_ The name of the file to use.
    /// \param useLastModifiedTime_ If the file already exists, the last
    /// modification time of the file will be included in the bookmark
    /// list returned from `getMostRecent` for any subscription ID that exists
    /// in the file upon recovery.
    LoggedBookmarkStore(const std::string& fileName_,
                        bool useLastModifiedTime_ = false)
      : MemoryBookmarkStore()
#ifdef _WIN32
      , _file(INVALID_HANDLE_VALUE)
#else
      , _file(0)
#endif
      , _fileName(fileName_)
    {
      init();
      recover(useLastModifiedTime_, false);
    }

    ///
    /// Creates a LoggedBookmarkStore using fileName_ as its file storage. If
    /// fileName_ already exists and has valid bookmarks in it, it will be
    /// recovered to determine the most recent bookmark and other messages
    /// received.
    /// \param adapter_ The {@link RecoveryPointAdapter} to notify
    /// about updates.
    /// \param fileName_ The name of the file to use.
    /// \param factory_ An optional factory function to use
    /// to create the {@link RecoveryPoint} objects sent to the recoveryPointAdapter_.
    /// \param useLastModifiedTime_ If the file already exists, the last
    /// modification time of the file will be included in the bookmark
    /// list returned from `getMostRecent` for any subscription ID that exists
    /// in the file upon recovery.
    LoggedBookmarkStore(const RecoveryPointAdapter& adapter_,
                        const char* fileName_,
                        RecoveryPointFactory factory_ = NULL,
                        bool useLastModifiedTime_ = false)
      : MemoryBookmarkStore(adapter_, factory_)
#ifdef _WIN32
      , _file(INVALID_HANDLE_VALUE)
#else
      , _file(0)
#endif
      , _fileName(fileName_)
    {
      init();
      recover(useLastModifiedTime_, true);
    }

    ///
    /// Creates a LoggedBookmarkStore using a file name fileName_
    /// \param adapter_ The {@link RecoveryPointAdapter} to notify
    /// about updates.
    /// \param fileName_ The name of the file to use.
    /// \param factory_ An optional factory function to use
    /// to create the {@link RecoveryPoint} objects sent to the recoveryPointAdapter_.
    /// \param useLastModifiedTime_ If the file already exists, the last
    /// modification time of the file will be included in the bookmark
    /// list returned from `getMostRecent` for any subscription ID that exists
    /// in the file upon recovery.
    LoggedBookmarkStore(const RecoveryPointAdapter& adapter_,
                        const std::string& fileName_,
                        RecoveryPointFactory factory_ = NULL,
                        bool useLastModifiedTime_ = false)
      : MemoryBookmarkStore(adapter_, factory_)
#ifdef _WIN32
      , _file(INVALID_HANDLE_VALUE)
#else
      , _file(0)
#endif
      , _fileName(fileName_)
    {
      init();
      recover(useLastModifiedTime_, true);
    }

    virtual ~LoggedBookmarkStore()
    {
      close();
      // In case _lock gets acquired by reader thread between end of this
      // destructor and start of base class destructor, prevent write()
      _recoveringFile = true;
    }

    void close()
    {
#ifdef _WIN32
      CloseHandle(_file);
#else
      ::close(_file);
#endif
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
      write(_file, sub->id(), ENTRY_BOOKMARK, bookmark);
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
      write(_file, subId, ENTRY_DISCARD, bookmark);
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
      Lock<Mutex> l(_lock);
      Subscription::Entry* entry = find(subId_)->getEntryByIndex(bookmarkSeqNo_);
      if (!entry || entry->_val.empty())
      {
        return;
      }
      write(_file, subId_, ENTRY_DISCARD, entry->_val);
      MemoryBookmarkStore::_discard(subId_, bookmarkSeqNo_);
    }

    ///
    /// Returns the most recent bookmark from the log that
    /// ought to be used for (re-)subscriptions.
    /// \param subId_ The id of the subscription to check.
    ///
    virtual Message::Field getMostRecent(const Message::Field& subId_)
    {
      Lock<Mutex> l(_lock);
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
        write(_file, subId, ENTRY_BOOKMARK, message_.getBookmark());
        write(_file, subId, ENTRY_DISCARD, message_.getBookmark());
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
#ifdef _WIN32
      if (_file != INVALID_HANDLE_VALUE)
      {
        CloseHandle(_file);
      }
      DeleteFileA(_fileName.c_str());
      _file = CreateFileA(_fileName.c_str(), GENERIC_READ | GENERIC_WRITE, 0,
                          NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
      if ( _file == INVALID_HANDLE_VALUE )
      {
        DWORD err = getErrorNo();
        std::ostringstream os;
        os << "Failed to recreate log file after purge for LoggedBookmarkStore" << _fileName << " for LoggedBookmarkStore";
        error(os.str(), err);
        return;
      }
#else
      ::close(_file);
      ::unlink(_fileName.c_str());
      _file = open(_fileName.c_str(), O_RDWR | O_CREAT, (mode_t)0644);
      if (_file == -1)
      {
        error("Failed to recreate log file after purge for LoggedBookmarkStore", getErrorNo());
        return;
      }
#endif
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
      MemoryBookmarkStore::_purge(subId_);
      std::string tmpFileName = _fileName + ".tmp";
      __prune(tmpFileName);
    }

    void setServerVersion(const VersionInfo& version_)
    {
      MemoryBookmarkStore::setServerVersion(version_);
    }

    void setServerVersion(size_t version_)
    {
      MemoryBookmarkStore::setServerVersion(version_);
    }

    // Yes, the argument is a non-const copy of what is passed in
    void _prune(const std::string& tmpFileName_)
    {
      Lock<Mutex> guard(_lock);
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

    void __prune(const std::string& tmpFileName_)
    {
#ifdef _WIN32
      HANDLE tmpFile;
      tmpFile = CreateFileA(tmpFileName_.c_str(), GENERIC_READ | GENERIC_WRITE, 0,
                            NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
      if (tmpFile == INVALID_HANDLE_VALUE )
      {
        DWORD err = getErrorNo();
        std::ostringstream os;
        os <<  "Failed to create temp log file " << tmpFileName_ <<
           " to prune LoggedBookmarkStore " << _fileName;
        error(os.str(), err);
        return;
      }
#else
      int tmpFile;
      tmpFile = open(tmpFileName_.c_str(), O_RDWR | O_CREAT, (mode_t)0644);
      if (tmpFile == -1)
      {
        int err = getErrorNo();
        std::ostringstream os;
        os <<  "Failed to create temp log file " << tmpFileName_ <<
           " to prune LoggedBookmarkStore " << _fileName;
        error(os.str(), err);
        return;
      }
#endif
      try
      {
        for (SubscriptionMap::iterator i = _subs.begin();
             i != _subs.end(); ++i)
        {
          Message::Field subId = i->first;
          assert(!subId.empty());
          Subscription* subPtr = i->second;
          const BookmarkRange& range = subPtr->getRange();
          if (range.isValid())
          {
            write(tmpFile, subId, ENTRY_BOOKMARK, range);
          }
          Message::Field recent = subPtr->getMostRecent(false);
          amps_uint64_t recentPub, recentSeq;
          Subscription::parseBookmark(recent, recentPub, recentSeq);
          Subscription::PublisherMap publishersDiscarded =
            subPtr->_publishers;
          MemoryBookmarkStore::EntryPtrList recovered;
          subPtr->getRecoveryEntries(recovered);
          subPtr->setPublishersToDiscarded(&recovered,
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
            write(tmpFile, subId, ENTRY_BOOKMARK, tmpBookmark);
            write(tmpFile, subId, ENTRY_DISCARD, tmpBookmark);
          }
          if (isWritableBookmark(recent.len()))
          {
            write(tmpFile, subId, ENTRY_BOOKMARK, recent);
            write(tmpFile, subId, ENTRY_DISCARD, recent);
          }
          else // set up _recentList
          {
            subPtr->getMostRecentList();
          }
          if (isWritableBookmark(subPtr->getLastPersisted().len()))
          {
            write(tmpFile, subId, ENTRY_PERSISTED,
                  subPtr->getLastPersisted());
          }
          subPtr->getActiveEntries(recovered);
          for (MemoryBookmarkStore::EntryPtrList::iterator entry =
                 recovered.begin();
               entry != recovered.end(); ++entry)
          {
            if ((*entry)->_val.empty() ||
                !isWritableBookmark((*entry)->_val.len()))
            {
              continue;
            }
            write(tmpFile, subId, ENTRY_BOOKMARK, (*entry)->_val);
            if (!(*entry)->_active)
            {
              write(tmpFile, subId, ENTRY_DISCARD, (*entry)->_val);
            }
          }
        }
      }
      catch (StoreException& ex)
      {
#ifdef _WIN32
        CloseHandle(tmpFile);
        DeleteFileA(tmpFileName_.c_str());
#else
        ::close(tmpFile);
        unlink(tmpFileName_.c_str());
#endif
        std::ostringstream os;
        os << "Exception during prune: " << ex.what();
        throw StoreException(os.str());
      }
#ifdef _WIN32
      CloseHandle(_file);
      CloseHandle(tmpFile);
      _file = INVALID_HANDLE_VALUE;
      tmpFile = INVALID_HANDLE_VALUE;
      // Replace file with pruned file
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
           << " in prune in LoggedBookmarkStore. Continuing by using "
           << tmpFileName_ << " as the LoggedBookmarkStore file.";
        error(os.str(), err);
        return;
      }
      init();
      SetFilePointer(_file, 0, NULL, FILE_END);
#else
      ::close(tmpFile);
      ::close(_file);
      if (-1 == ::unlink(_fileName.c_str()))
      {
        int err = getErrorNo();
        // Try to set _file to the tmp file then throw
        std::string desiredFileName = _fileName;
        _fileName = tmpFileName_;
        init();
        std::ostringstream os;
        os << "Failed to delete file " << desiredFileName
           << " after creating temporary file " << tmpFileName_
           << " in prune in LoggedBookmarkStore. Continuing by using "
           << tmpFileName_ << " as the LoggedBookmarkStore file.";
        error(os.str(), err);
        return;
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
           << " in prune in LoggedBookmarkStore. Continuing by using "
           << tmpFileName_ << " as the LoggedBookmarkStore file.";
        error(os.str(), err);
        return;
      }
      init();
      struct stat fst;
      if (-1 == ::fstat(_file, &fst))
      {
        int err = getErrorNo();
        std::ostringstream os;
        os << "Failed to get size of pruned file " << _fileName
           << " in prune in LoggedBookmarkStore. ";
        error(os.str(), err);
        return;
      }
      ::lseek(_file, (off_t)fst.st_size, SEEK_SET);
#endif
    }

  private:
    virtual void _persisted(Subscription* subP_,
                            const Message::Field& bookmark_)
    {
      Lock<Mutex> guard(_lock);
      write(_file, subP_->id(), ENTRY_PERSISTED, bookmark_);
      MemoryBookmarkStore::_persisted(subP_, bookmark_);
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
      write(_file, subP_->id(), ENTRY_PERSISTED, bookmarkField);
      MemoryBookmarkStore::_persisted(subP_, bookmarkField);
      return bookmarkField;
    }

#ifdef _WIN32
    typedef DWORD ERRTYPE ;
    ERRTYPE getErrorNo() const
    {
      return GetLastError();
    }

    void error(const std::string& message_, ERRTYPE err)
    {
      std::ostringstream os;
      static const DWORD msgSize = 2048;
      char pMsg[msgSize];
      DWORD sz = FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM |
                                FORMAT_MESSAGE_ARGUMENT_ARRAY,
                                NULL, err, LANG_NEUTRAL,
                                pMsg, msgSize, NULL);
      os << "File: " << _fileName << ". " << message_;
      if (err != 0)
      {
        os << " with error " << pMsg;
      }
      throw StoreException(os.str());
    }
#else
    typedef int ERRTYPE;
    ERRTYPE getErrorNo() const
    {
      return errno;
    }

    void error(const std::string& message_, ERRTYPE err)
    {
      std::ostringstream os;
      os << "File: " << _fileName << ". " << message_;
      if (err != 0)
      {
        os << " with error " << strerror(err);
      }
      close();
      throw StoreException(os.str());
    }
#endif

    void init()
    {
#ifdef _WIN32
      _file = CreateFileA(_fileName.c_str(), GENERIC_READ | GENERIC_WRITE, 0,
                          NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
      if ( _file == INVALID_HANDLE_VALUE )
      {
        DWORD err = getErrorNo();
        std::ostringstream os;
        os <<  "Failed to initialize log file " << _fileName << " for LoggedBookmarkStore";
        error(os.str(), err);
        return;
      }
#else
      _file = open(_fileName.c_str(), O_RDWR | O_CREAT, (mode_t)0644);
      if (_file == -1)
      {
        int err = getErrorNo();
        std::ostringstream os;
        os <<  "Failed to initialize log file " << _fileName << " for LoggedBookmarkStore";
        error(os.str(), err);
        return;
      }
#endif
    }

    // This implementation will only ever use this when logging a bookmark
    // Could be used to add a feature where discarded bookmark fields are logged in
    // addition to the generated bookmark.
    void write(FileType file_, const Message::Field& subId_, char type_,
               const Message::Field& bookmark_)
    {
      Lock<Mutex> guard(_fileLock);
      if (!_recoveringFile && isWritableBookmark(bookmark_.len()))
      {
#ifdef _WIN32
        DWORD written;
        size_t len = subId_.len();
        BOOL ok = WriteFile(file_, (LPVOID)&len, sizeof(size_t), &written, NULL);
        ok |= WriteFile(file_, (LPVOID)subId_.data(), (DWORD)len, &written, NULL);
        ok |= WriteFile(file_, (LPVOID)&type_, 1, &written, NULL);
        len = bookmark_.len();
        ok |= WriteFile(file_, (LPVOID)&len, sizeof(size_t), &written, NULL);
        ok |= WriteFile(file_, (LPVOID)bookmark_.data(), (DWORD)len,
                        &written, NULL);
        if (!ok)
        {
          error("Failed to write to bookmark log.", getErrorNo());
          return;
        }

#else
        if (file_ == -1)
        {
          file_ = open(_fileName.c_str(), O_RDWR | O_CREAT, (mode_t)0644);
          if (file_ == -1)
          {
            int err = getErrorNo();
            std::ostringstream os;
            os << "Failed to open file " << _fileName
               << " for write in LoggedBookmarkStore. ";
            error(os.str(), err);
            return;
          }
        }
        struct iovec data[5];
        size_t len = subId_.len();
        data[0].iov_base = (amps_iovec_base_ptr)(void*)&len;
        data[0].iov_len = sizeof(size_t);
        data[1].iov_base = (amps_iovec_base_ptr)(void*)subId_.data();
        data[1].iov_len = len;
        data[2].iov_base = (amps_iovec_base_ptr)(void*)&type_;
        data[2].iov_len = 1;
        size_t bookmarkLen = bookmark_.len();
        data[3].iov_base = (amps_iovec_base_ptr)(void*)&bookmarkLen;
        data[3].iov_len = sizeof(size_t);
        data[4].iov_base = (amps_iovec_base_ptr)(void*)bookmark_.data();
        data[4].iov_len = bookmarkLen;
        ssize_t written = ::writev(file_, data, 5);
        if (written == -1)
        {
          error("Failed to write to bookmark log.", getErrorNo());
          return;
        }
#endif
      }
    }

    // This implementation will only ever use this when discarding a bookmark
    // Could be used to add a feature where generated bookmarks are logged in
    // addition to the bookmark field.
    void write(FileType file_, const Message::Field& subId_,
               char type_, size_t bookmark_)
    {
      Lock<Mutex> guard(_fileLock);
      if (!_recoveringFile)
      {
#ifdef _WIN32
        DWORD written;
        size_t len = subId_.len();
        BOOL ok = WriteFile(file_, (LPVOID)&len, sizeof(size_t), &written, NULL);
        ok |= WriteFile(file_, (LPVOID)subId_.data(), (DWORD)len, &written, NULL);
        ok |= WriteFile(file_, (LPVOID)&type_, 1, &written, NULL);
        ok |= WriteFile(file_, (LPVOID)&bookmark_, sizeof(size_t),
                        &written, NULL);
        if (!ok)
        {
          error("Failed to write bookmark sequence to file.", getErrorNo());
          return;
        }

#else
        if (file_ == -1)
        {
          file_ = open(_fileName.c_str(), O_RDWR | O_CREAT, (mode_t)0644);
          if (file_ == -1)
          {
            int err = getErrorNo();
            std::ostringstream os;
            os << "Failed to open file " << _fileName
               << " to write bookmark sequence in LoggedBookmarkStore. ";
            error(os.str(), err);
            return;
          }
        }
        struct iovec data[4];
        size_t len = subId_.len();
        data[0].iov_base = (amps_iovec_base_ptr)(void*)&len;
        data[0].iov_len = sizeof(size_t);
        data[1].iov_base = (amps_iovec_base_ptr)(void*)subId_.data();
        data[1].iov_len = len;
        data[2].iov_base = (amps_iovec_base_ptr)(void*)&type_;
        data[2].iov_len = 1;
        data[3].iov_base = (amps_iovec_base_ptr)(void*)&bookmark_;
        data[3].iov_len = sizeof(size_t);
        ssize_t written = ::writev(file_, data, 4);
        if (written == -1)
        {
          error("Failed to write bookmark sequence to file.", getErrorNo());
          return;
        }
#endif
      }
    }

#ifdef _WIN32
#define VOID_P(buf) (LPVOID)buf
    bool readFileBytes(LPVOID buffer, size_t numBytes, DWORD* bytesRead)
    {
      return (ReadFile(_file, buffer, (DWORD)numBytes, bytesRead, NULL) == TRUE);
    }
#else
#define VOID_P(buf) (void*)buf
    bool readFileBytes(void* buffer, size_t numBytes, ssize_t* bytesRead)
    {
      *bytesRead = ::read(_file, buffer, numBytes);
      return (*bytesRead >= 0);
    }
#endif

    void recover(bool useLastModifiedTime_, bool hasAdapter_)
    {
      size_t bufferLen = 128;
      char* buffer = new char[bufferLen];
      size_t subIdBufferLen = 128;
      char* subIdBuffer = new char[bufferLen];
      Message::Field sub;
      size_t subLen = 0;
      Message::Field bookmarkField;
      size_t bookmarkLen = 0;
      Lock<Mutex> l(_lock);
      Lock<Mutex> guard(_fileLock);
      _recoveringFile = true;
      char* fileTimestamp = new char[AMPS_TIMESTAMP_LEN];
      fileTimestamp[0] = '\0';
#ifdef _WIN32
      LARGE_INTEGER lifileSize;
      if (GetFileSizeEx(_file, &lifileSize) == 0)
      {
        DWORD err = getErrorNo();
        delete[] buffer;
        delete[] subIdBuffer;
        _recoveringFile = false;
        error("Failure getting file size while trying to recover.", err);
        return;
      }
#ifdef _WIN64
      size_t fileSize = lifileSize.QuadPart;
#else
      size_t fileSize = lifileSize.LowPart;
#endif
      if (useLastModifiedTime_ && fileSize > 0)
      {
        FILETIME ftModifiedTime;
        if (GetFileTime(_file, NULL, NULL, &ftModifiedTime) == 0)
        {
          DWORD err = getErrorNo();
          delete[] buffer;
          delete[] subIdBuffer;
          _recoveringFile = false;
          error("Failure getting file time while trying to recover.", err);
          return;
        }
        SYSTEMTIME st;
        if (FileTimeToSystemTime(&ftModifiedTime, &st) == 0)
        {
          DWORD err = getErrorNo();
          delete[] buffer;
          delete[] subIdBuffer;
          _recoveringFile = false;
          error("Failure converting file time while trying to recover.", err);
          return;
        }
        sprintf_s(fileTimestamp, AMPS_TIMESTAMP_LEN,
                  "%04d%02d%02dT%02d%02d%02d", st.wYear, st.wMonth,
                  st.wDay, st.wHour, st.wMinute, st.wSecond);
        fileTimestamp[AMPS_TIMESTAMP_LEN - 1] = 'Z';
      }
      else if (fileSize == 0)
      {
        delete[] fileTimestamp;
        delete[] buffer;
        delete[] subIdBuffer;
        _recoveringFile = false;
        return;
      }
      DWORD readBytes = 0;
      OFF_T loc = 0;
      SetFilePointer(_file, 0, NULL, FILE_BEGIN);
#else
      struct stat fst;
      ::fstat(_file, &fst);
      ssize_t fileSize = fst.st_size;
      ssize_t readBytes = 0;
      if (useLastModifiedTime_ && fileSize > 0)
      {
        struct tm timeInfo;
        gmtime_r(&fst.st_mtime, &timeInfo);
        strftime(fileTimestamp, AMPS_TIMESTAMP_LEN,
                 "%Y%m%dT%H%M%S", &timeInfo);
        fileTimestamp[AMPS_TIMESTAMP_LEN - 1] = 'Z';
      }
      else if (fileSize == 0)
      {
        delete[] fileTimestamp;
        delete[] buffer;
        delete[] subIdBuffer;
        _recoveringFile = false;
        return;
      }
      OFF_T loc = 0;
      ::lseek(_file, loc, SEEK_SET);
#endif
      // We trust file recovery over Adapter recovery
      if (hasAdapter_)
      {
        MemoryBookmarkStore::__purge();
      }
      if (!readFileBytes(VOID_P(&subLen), sizeof(size_t), &readBytes)
          || subLen > getMaxSubIdLength())
      {
        delete[] fileTimestamp;
        delete[] buffer;
        delete[] subIdBuffer;
        _recoveringFile = false;
        error("Failure reading file while trying to recover.", getErrorNo());
        return;
      }
#ifdef _WIN32
      size_t totalBytes = readBytes;
#else
      ssize_t totalBytes = readBytes;
#endif
      ERRTYPE err = 0; // 0 no error, -1 corruption, positive is errno file error
      size_t tooManyBytes = 0;
      typedef std::map<Message::Field, size_t,
              Message::Field::FieldHash> BookmarkMap;
      typedef std::map<Message::Field, size_t,
              Message::Field::FieldHash>::iterator BookmarkMapIter;
      // Map of subId to set of recovered bookmarks
      typedef std::map<Message::Field, BookmarkMap*,
              Message::Field::FieldHash> ReadMap;
      typedef std::map<Message::Field, BookmarkMap*,
              Message::Field::FieldHash>::iterator ReadMapIter;
      ReadMap recovered;
      while (subLen > 0 && (size_t)readBytes == sizeof(size_t) &&
             (size_t)totalBytes <= (size_t)fileSize)
      {
        if (subLen >= ((size_t)fileSize - (size_t)totalBytes)
            || subLen > getMaxSubIdLength())
        {
          err = (ERRTYPE) - 1;
          tooManyBytes = subLen + 1;
          break;
        }
        else
        {
          if (subIdBufferLen < subLen)
          {
            delete [] subIdBuffer;
            subIdBufferLen = 2 * subLen;
            subIdBuffer = new char[subIdBufferLen];
          }
          if (!readFileBytes(VOID_P(subIdBuffer), subLen, &readBytes))
          {
            err = getErrorNo();
            tooManyBytes = subLen;
            break;
          }
          totalBytes += readBytes;
          sub.assign(subIdBuffer, subLen);
          if (!readFileBytes(VOID_P(buffer), 1, &readBytes))
          {
            err = getErrorNo();
            tooManyBytes = 1;
            break;
          }
          totalBytes += readBytes;
          switch (buffer[0])
          {
          case ENTRY_BOOKMARK:
          {
            if ((size_t)totalBytes + sizeof(size_t) >= (size_t)fileSize)
            {
              // Corrupt final record is ok
              err = (ERRTYPE) - 1;
              tooManyBytes = sizeof(size_t);
              break;
            }
            if (!readFileBytes(VOID_P(&bookmarkLen), sizeof(size_t), &readBytes))
            {
              err = getErrorNo();
              tooManyBytes = sizeof(size_t);
              break;
            }
            totalBytes += readBytes;
            if (bookmarkLen > (size_t)fileSize - (size_t)totalBytes)
            {
              // Corrupt final record is ok
              err = (ERRTYPE) - 1;
              tooManyBytes = bookmarkLen;
              break;
            }
            if (bufferLen < bookmarkLen)
            {
              delete [] buffer;
              bufferLen = 2 * bookmarkLen;
              buffer = new char[bufferLen];
            }
            if (!readFileBytes(VOID_P(buffer), bookmarkLen, &readBytes))
            {
              err = getErrorNo();
              tooManyBytes = bookmarkLen;
              break;
            }
            totalBytes += readBytes;
            bookmarkField.assign(buffer, bookmarkLen);
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
            if ((size_t)totalBytes + sizeof(size_t) >= (size_t)fileSize)
            {
              // Corrupt final record is ok
              err = (ERRTYPE) - 1;
              tooManyBytes = sizeof(size_t);
              break;
            }
            if (!readFileBytes(VOID_P(&bookmarkLen), sizeof(size_t), &readBytes))
            {
              err = getErrorNo();
              tooManyBytes = sizeof(size_t);
              break;
            }
            totalBytes += readBytes;
            if (bookmarkLen > (size_t)fileSize - (size_t)totalBytes)
            {
              // Corrupt final record is ok
              err = (ERRTYPE) - 1;
              tooManyBytes = bookmarkLen;
              break;
            }
            if (bufferLen < bookmarkLen)
            {
              delete [] buffer;
              bufferLen = 2 * bookmarkLen;
              buffer = new char[bufferLen];
            }
            if (!readFileBytes(VOID_P(buffer), bookmarkLen, &readBytes))
            {
              err = getErrorNo();
              tooManyBytes = bookmarkLen;
              break;
            }
            totalBytes += readBytes;
            bookmarkField.assign(buffer, bookmarkLen);
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
            Subscription* subP = find(sub);
            if (!BookmarkRange::isRange(bookmarkField))
            {
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
            if ((size_t)totalBytes + sizeof(size_t) >= (size_t)fileSize)
            {
              // Corrupt final record is ok
              err = (ERRTYPE) - 1;
              tooManyBytes = sizeof(size_t);
              break;
            }
            if (!readFileBytes(VOID_P(&bookmarkLen), sizeof(size_t), &readBytes))
            {
              err = getErrorNo();
              tooManyBytes = sizeof(size_t);
              break;
            }
            totalBytes += readBytes;
            if (bookmarkLen > (size_t)fileSize - (size_t)totalBytes)
            {
              // Corrupt final record is ok
              err = (ERRTYPE) - 1;
              tooManyBytes = bookmarkLen;
              break;
            }
            if (bufferLen < bookmarkLen)
            {
              delete [] buffer;
              bufferLen = 2 * bookmarkLen;
              buffer = new char[bufferLen];
            }
            if (!readFileBytes(VOID_P(buffer), bookmarkLen, &readBytes))
            {
              err = getErrorNo();
              tooManyBytes = bookmarkLen;
              break;
            }
            totalBytes += readBytes;
            bookmarkField.assign(buffer, bookmarkLen);
            Subscription* subP = find(sub);
            MemoryBookmarkStore::_persisted(subP, bookmarkField);
          }
          break;
          default:
          {
            // Corrupt final record is ok
            err = (ERRTYPE) - 1;
            tooManyBytes = (size_t)fileSize - (size_t)totalBytes;
          }
          break;
          }
        }
        loc = (OFF_T)totalBytes;
        if ((size_t)totalBytes > (size_t)fileSize)
        {
          loc = (OFF_T)fileSize;
          break;
        }
        if (!readFileBytes(VOID_P(&subLen), sizeof(size_t), &readBytes))
        {
          err = getErrorNo();
          tooManyBytes = sizeof(size_t);
          break;
        }
        totalBytes += readBytes;
      }
      delete[] buffer;
      delete[] subIdBuffer;
      if (err == 0)
      {
        for (SubscriptionMap::iterator i = _subs.begin(); i != _subs.end(); ++i)
        {
          if (recovered.count(i->first) && !recovered[i->first]->empty())
          {
            Subscription* subPtr = i->second;
            if (subPtr->getMostRecent(false).len() > 1)
            {
              subPtr->justRecovered();
            }
            else
            {
              // Unlikely, but we may have recovered only undiscarded bookmarks
              // so we should really just restart as a new subscription.
              delete subPtr;
              _subs[i->first] = new Subscription(this, i->first);
            }
          }
          if (useLastModifiedTime_ && fileTimestamp[0] != '\0')
          {
            _subs[i->first]->setRecoveryTimestamp(fileTimestamp);
          }
        }
      }
      for (ReadMapIter i = recovered.begin(), e = recovered.end(); i != e; ++i)
      {
        std::for_each(i->second->begin(), i->second->end(), _clearBookmark);
        delete i->second;
        Message::Field f = i->first;
        f.clear();
      }
      delete[] fileTimestamp;
      _recoveringFile = false;
      if (err != 0)
      {
        // Arbitrary guess if we're on the last record
        // We set err to -1 if we read a corrupt value or
        // to errno/last error if a read failed.
        if (err != (ERRTYPE) - 1 || loc == 0 || fileSize - loc > 128)
        {
          std::ostringstream os;
          os << "Error while recovering LoggedBookmarkStore from "
             << _fileName
             << ". Record starting at " << loc
             << " reading at " << totalBytes
             << " requested " << tooManyBytes
             << " and file size is " << fileSize;
          error(os.str(), (err != (ERRTYPE) - 1 ? err : 0));
        }
        else
        {
#ifdef _WIN32
#ifdef _WIN64
          LONG low = (LONG)loc;
          LONG high = (LONG)((loc >> 32) & 0xffffffff);
          SetFilePointer(_file, low, &high, FILE_BEGIN);
#else
          SetFilePointer(_file, loc, NULL, FILE_BEGIN);
#endif
#else
          ::lseek(_file, loc, SEEK_SET);
#endif
        }
      }
    }

  private:
    FileType _file;
    Mutex _fileLock;
    std::string _fileName;
    bool _recoveringFile;
  };

} // end namespace AMPS


#endif // _LOGGEDBOOKMARKSTORE_H_

