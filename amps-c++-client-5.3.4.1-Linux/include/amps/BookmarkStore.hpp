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
#ifndef _BOOKMARKSTORE_H_
#define _BOOKMARKSTORE_H_
#include <string>
#include "amps/Message.hpp"
#include "amps/util.hpp"

namespace AMPS
{

  /**
   @defgroup specialBookmarks Special Bookmark Values

   The AMPS C++ client includes definitions of special bookmark values. These
   are used with AMPS::Client::bookmarkSubscribe() as values provided to the
   bookmark parameter.

   @{
  */

/// Start the subscription at the first undiscarded message in the bookmark
/// store, or at the end of the bookmark store if all messages have been
/// discarded.
#define AMPS_BOOKMARK_RECENT "recent"

///
/// Start the subscription at the beginning of the journal.
#define AMPS_BOOKMARK_EPOCH "0"

///
/// Start the subscription at the point in time when AMPS processes the subscription.
#define AMPS_BOOKMARK_NOW "0|1|"

  /* @}  */

  class BookmarkStore;

///
/// Function type for BookmarkStore resize events
/// The store_ param is store which is resizing.
/// The subId_ param is the subscription id for which a resize is required.
/// The size_ is the number of bytes being requested for the new size.
/// The userData_ is the userData_ that was set when the handler was set on the store.
/// The return value should be true if resize should proceed and false if the
/// the size should be unchanged. A false value should only be returned if some
/// other action was taken to free up space within the store.
  typedef bool (*BookmarkStoreResizeHandler)(BookmarkStore store_,
                                             const Message::Field& subId_,
                                             size_t size_,
                                             void* userData_);

///
/// Abstract base class for storing received bookmarks for HA clients.
  class BookmarkStoreImpl : public RefBody
  {
  public:
    BookmarkStoreImpl()
      : _resizeHandler(NULL)
      , _resizeHandlerData(NULL)
      , _maxSubIdLength(AMPS_MAX_SUBID_LEN)
    {;}

    virtual ~BookmarkStoreImpl() {;}

    ///
    /// Log a bookmark to the persistent log.
    /// \param message_ The Message to log in the store.
    /// \return the corresponding bookmark sequence number
    ///         for this bookmark
    ///
    virtual size_t log(Message& message_) = 0;

    ///
    /// Log a discard-bookmark entry to the persistent log
    /// based on a bookmark sequence number.
    /// \param subId_ The id of the subscription to which the bookmark applies.
    /// \param bookmarkSeqNo_ The bookmark sequence number to discard.
    ///
    virtual void discard(const Message::Field& subId_,
                         size_t bookmarkSeqNo_) = 0;

    ///
    /// Log a discard-bookmark entry to the persistent log
    /// based on a bookmark sequence number.
    /// \param message_ The Message to discard from the store.
    ///
    virtual void discard(const Message& message_) = 0;

    ///
    /// Returns the most recent bookmark from the log that
    /// ought to be used for (re-)subscriptions.
    /// \param subId_ The id of the subscription to check.
    /// \return Most recent bookmark.
    ///
    virtual Message::Field getMostRecent(const Message::Field& subId_) = 0;

    ///
    /// Called for each arriving message to determine if
    /// the application has already seen this bookmark and should
    /// not be reprocessed.  Returns 'true' if the bookmark is
    /// in the log and should not be re-processed, false otherwise.
    /// \param message_ The Message to check.
    /// \return Whether or not the bookmark has been discarded.
    ///
    virtual bool isDiscarded(Message& message_) = 0;

    ///
    /// Called to purge the contents of this store.
    /// Removes any tracking history associated with publishers and received
    /// messages, and may delete or truncate on-disk representations as well.
    ///
    virtual void purge() = 0;

    ///
    /// Called to purge the contents of this store for particular subId.
    /// Removes any tracking history associated with publishers and received
    /// messages, and will remove the subId from the file as well.
    ///
    virtual void purge(const Message::Field& subId_) = 0;

    ///
    /// Called to find the oldest bookmark sequence in the store.
    /// \param subId_ The subscription ID on which to find the oldest bookmark.
    /// \return The bookmark sequence that is oldest in the store for subId_
    virtual size_t getOldestBookmarkSeq(const Message::Field& subId_) = 0;

    ///
    /// Set a handler on the bookmark store that will get called whenever
    /// a resize of the store is required due to the number of stored
    /// bookmarks exceeding the currently allocated storage to hold them.
    /// \param handler_ The handler to be called when resizing.
    /// \param userData_ User data passed to the handler when it is called.
    virtual void setResizeHandler(BookmarkStoreResizeHandler handler_, void* userData_)
    {
      _resizeHandler = handler_;
      _resizeHandlerData = userData_;
    }

    ///
    /// Mark the bookmark provided as replicated to all sync replication
    /// destinations for the given subscription.
    /// \param subId_ The subscription Id to which the bookmark applies.
    /// \param bookmark_ The most recent replicated bookmark.
    virtual void persisted(const Message::Field& subId_,
                           const Message::Field& bookmark_) = 0;

    ///
    /// Mark the bookmark provided as replicated to all sync replication
    /// destinations for the given subscription.
    /// \param subId_ The subscription Id to which the bookmark applies.
    /// \param bookmark_ The most recent bookmark's sequence number.
    /// \return The bookmark field that was just marked persisted.
    virtual Message::Field persisted(const Message::Field& subId_, size_t bookmark_) = 0;

    ///
    /// Internally used to set the server version so the store knows how to deal
    /// with persisted acks and calls to getMostRecent().
    /// \param version_ The version of the server being used.
    virtual void setServerVersion(size_t version_) = 0;

    ///
    /// Internally used to set the server version so the store knows how to deal
    /// with persisted acks and calls to getMostRecent().
    /// \param version_ The version of the server being used.
    virtual void setServerVersion(const VersionInfo& version_) = 0;

    bool callResizeHandler(const Message::Field& subId_, size_t newSize_);

    inline void prune(const std::string& tmpFileName_ = std::string())
    {
      _prune(tmpFileName_);
    }

    virtual void _prune(const std::string&)
    {
      return;
    }

    ///
    /// Gets the maximum allowed length for a sub id when recovering a bookmark
    /// store from persistent storage.
    /// \return The maximum length allowed.
    size_t getMaxSubIdLength() const
    {
      return _maxSubIdLength;
    }

    ///
    /// Sets the maximum allowed length for a sub id when recovering a bookmark
    /// store from persistent storage.
    /// \param maxSubIdLength_ The maximum length allowed.
    void setMaxSubIdLength(size_t maxSubIdLength_)
    {
      _maxSubIdLength = maxSubIdLength_;
    }

  private:
    BookmarkStoreResizeHandler  _resizeHandler;
    void*                       _resizeHandlerData;
    size_t                      _maxSubIdLength;
  };

///
/// Interface for BookmarkStoreImpl classes.
  class BookmarkStore
  {
    RefHandle<BookmarkStoreImpl> _body;
  public:
    ///
    /// Creates a BookmarkStore that does nothing
    BookmarkStore() {;}

    ///
    /// Creates a BookmarkStore based on the given implementation
    BookmarkStore(BookmarkStoreImpl* impl_) : _body(impl_) {;}

    BookmarkStore(const BookmarkStore& rhs) : _body(rhs._body) {;}

    BookmarkStore& operator=(const BookmarkStore& rhs)
    {
      _body = rhs._body;
      return *this;
    }

    ~BookmarkStore() {;}

    ///
    /// Sets the BookmarkStore to use the given implementation
    void setImplementation(BookmarkStoreImpl* impl_)
    {
      _body = impl_;
    }

    bool isValid() const
    {
      return _body.isValid();
    }

    ///
    /// Log a bookmark to the persistent log.
    /// \param message_ The Message to log.
    /// \return the corresponding bookmark sequence number
    ///         for this bookmark
    ///
    size_t log(Message& message_)
    {
      if (_body.isValid())
      {
        return _body.get().log(message_);
      }
      return Message::BOOKMARK_NONE;
    }

    ///
    /// Log a discard-bookmark entry to the persistent log
    /// based on a bookmark sequence number.
    /// \param subId_ The id of the subscription to which the bookmark applies.
    /// \param bookmarkSeqNo_ The bookmark sequence number to discard.
    ///
    void discard(const Message::Field& subId_, size_t bookmarkSeqNo_)
    {
      if (_body.isValid())
      {
        _body.get().discard(subId_, bookmarkSeqNo_);
      }
    }

    ///
    /// Log a discard-bookmark entry to the persistent log
    /// based on a Message.
    /// \param message_ The message to discard.
    ///
    void discard(const Message& message_)
    {
      if (_body.isValid())
      {
        _body.get().discard(message_);
      }
    }

    ///
    /// Returns the most recent bookmark from the log that
    /// ought to be used for (re-)subscriptions.
    /// \param subId_ The id of the subscription to check.
    /// \return Most recent bookmark.
    ///
    Message::Field getMostRecent(const Message::Field& subId_)
    {
      if (_body.isValid())
      {
        return _body.get().getMostRecent(subId_);
      }
      return Message::Field(AMPS_BOOKMARK_EPOCH);
    }

    ///
    /// Called for each arriving message to determine if
    /// the application has already seen this bookmark and should
    /// not be reprocessed.  Returns 'true' if the bookmark is
    /// in the log and should not be re-processed, false otherwise.
    /// \param message_ The Message to check.
    /// \return Whether or not the bookmark has been discarded.
    ///
    bool isDiscarded(Message& message_)
    {
      if (_body.isValid())
      {
        return _body.get().isDiscarded(message_);
      }
      return false;
    }

    ///
    /// Called to purge the contents of this store.
    /// Removes any tracking history associated with publishers and received
    /// messages, and may delete or truncate on-disk representations as well.
    ///
    void purge()
    {
      if (_body.isValid())
      {
        _body.get().purge();
      }
    }

    ///
    /// Called to purge the contents of this store for particular subId.
    /// Removes any tracking history associated with publishers and received
    /// messages, and will remove the subId from the file as well.
    ///
    void purge(const Message::Field& subId_)
    {
      if (_body.isValid())
      {
        _body.get().purge(subId_);
      }
    }

    ///
    /// Set a handler on the bookmark store that will get called whenever
    /// a resize of the store is required due to the number of stored
    /// bookmarks exceeding the currently allocated storage to hold them.
    /// \param handler_ The handler to be called when resizing.
    /// \param userData_ User data passed to the handler when it is called.
    void setResizeHandler(BookmarkStoreResizeHandler handler_, void* userData_)
    {
      if (_body.isValid())
      {
        _body.get().setResizeHandler(handler_, userData_);
      }
    }

    ///
    /// Called to find the oldest bookmark in the store.
    /// \param subId_ The subscription ID on which to find the oldest bookmark.
    /// \return The bookmark sequence that is oldest in the store for subId_
    size_t getOldestBookmarkSeq(const std::string& subId_)
    {
      if (_body.isValid())
        return _body.get().getOldestBookmarkSeq(Message::Field(subId_.c_str(),
                                                               subId_.length()));
      return AMPS_UNSET_INDEX;
    }

    ///
    /// Called to find the oldest bookmark sequence in the store.
    /// \param subId_ The subscription ID on which to find the oldest bookmark.
    /// \return The bookmark sequence that is oldest in the store for subId_
    size_t getOldestBookmarkSeq(const Message::Field& subId_)
    {
      if (_body.isValid())
      {
        return _body.get().getOldestBookmarkSeq(subId_);
      }
      return AMPS_UNSET_INDEX;
    }

    /// Called internally to indicate messages up to and including bookmark
    /// are replicated to all replication destinations.
    /// \param subId_ The subscription Id to which the bookmark applies.
    /// \param bookmark_ The most recent bookmark replicated everywhere.
    void persisted(const Message::Field& subId_, const Message::Field& bookmark_)
    {
      if (_body.isValid())
      {
        _body.get().persisted(subId_, bookmark_);
      }
    }

    /// Called internally to indicate messages up to and including bookmark
    /// are replicated to all replication destinations.
    /// \param subId_ The subscription Id to which the bookmark applies.
    /// \param bookmark_ The most recent bookmark replicated everywhere.
    void persisted(const Message::Field& subId_, size_t bookmark_)
    {
      if (_body.isValid())
      {
        _body.get().persisted(subId_, bookmark_);
      }
    }

    ///
    /// Internally used to set the server version so the store knows how to deal
    /// with persisted acks and calls to getMostRecent().
    /// \param version_ The version of the server being used.
    void setServerVersion(size_t version_)
    {
      if (_body.isValid())
      {
        _body.get().setServerVersion(version_);
      }
    }

    ///
    /// Internally used to set the server version so the store knows how to deal
    /// with persisted acks and calls to getMostRecent().
    /// \param version_ The version of the server being used.
    void setServerVersion(const VersionInfo& version_)
    {
      if (_body.isValid())
      {
        _body.get().setServerVersion(version_);
      }
    }

    ///
    /// Used to trim the size of a store's storage. Implemented for file-based
    /// stores to remove items no longer necessary to create the current state.
    /// \param tmpFileName_ The name to use for the temporary file created
    /// while pruning the bookmark store.
    void prune(const std::string& tmpFileName_ = "")
    {
      if (_body.isValid())
      {
        _body.get().prune(tmpFileName_);
      }
    }

    ///
    /// Used to get a pointer to the implementation.
    /// \return The BookmarkStoreImpl* for this store's implementation.
    BookmarkStoreImpl* get()
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

    ///
    /// Gets the maximum allowed length for a sub id when recovering a bookmark
    /// store from persistent storage.
    /// \return The maximum length allowed.
    size_t getMaxSubIdLength() const
    {
      if (_body.isValid())
      {
        return _body.get().getMaxSubIdLength();
      }
      else
      {
        return 0;
      }
    }

    ///
    /// Sets the maximum allowed length for a sub id when recovering a bookmark
    /// store from persistent storage.
    /// \param maxSubIdLength_ The maximum length allowed.
    void setMaxSubIdLength(size_t maxSubIdLength_)
    {
      if (_body.isValid())
      {
        _body.get().setMaxSubIdLength(maxSubIdLength_);
      }
    }

  };

  inline bool BookmarkStoreImpl::callResizeHandler(const Message::Field& subId_,
                                                   size_t newSize_)
  {
    if (_resizeHandler)
    {
      return _resizeHandler(BookmarkStore(this), subId_, newSize_, _resizeHandlerData);
    }
    return true;
  }

///
/// A BookmarkStoreResizeHandler that discards the oldest bookmark assuming that it
/// was used but not discarded when a resize request exceeds size in userData_.
/// WARNING: Using this handler could cause unseen message loss. This should be used
/// primarily as a sample and you should write your own version to at least
/// add some logging.
  inline bool ThrowawayBookmarkResizeHandler(BookmarkStore store_,
                                             const Message::Field& subId_,
                                             size_t newSize_, void* data_)
  {
    size_t* maxSizep = (size_t*)data_;
    if (newSize_ > *maxSizep)
    {
      size_t discardSeq = store_.getOldestBookmarkSeq(subId_);
      store_.discard(subId_, discardSeq);
      store_.persisted(subId_, discardSeq);
      return false;
    }
    return true;
  }

}

#endif //_BOOKMARKSTORE_H_


