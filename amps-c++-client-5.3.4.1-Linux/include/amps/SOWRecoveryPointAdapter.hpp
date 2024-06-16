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

#ifndef _SOWRECOVERYPOINTADAPTER_H_
#define _SOWRECOVERYPOINTADAPTER_H_

#include <amps/ampsplusplus.hpp>
#include <amps/Field.hpp>
#include <amps/RecoveryPoint.hpp>
#include <amps/RecoveryPointAdapter.hpp>
#include <assert.h>
#include <memory>
#include <string>

using std::string;

#define AMPS_SOW_STORE_DEFAULT_TOPIC "/ADMIN/bookmark_store"
#define AMPS_SOW_STORE_DEFAULT_CLIENT_FIELD "clientName"
#define AMPS_SOW_STORE_DEFAULT_SUB_FIELD "subId"
#define AMPS_SOW_STORE_DEFAULT_BOOKMARK_FIELD "bookmark"

#define SOW_RECOVERY_HANDLE_EXCEPTION(x) \
  catch (const AMPSException& ex_) \
  { \
    std::ostringstream os; \
    os << x << ": AMPSException " << ex_.what(); \
    StoreException ex(os.str()); \
    if (!_throwNotListen && _pExceptionListener) \
    { \
      _pExceptionListener->exceptionThrown(ex); \
    } \
    else throw ex; \
  } \
  catch (const std::exception& ex_) \
  { \
    std::ostringstream os; \
    os << x << ": std::exception " << ex_.what(); \
    StoreException ex(os.str()); \
    if (!_throwNotListen && _pExceptionListener) \
    { \
      _pExceptionListener->exceptionThrown(ex); \
    } \
    else throw ex; \
  } \
  catch (...) \
  { \
    std::ostringstream os; \
    os << x << ": Unknown exception"; \
    StoreException ex(os.str()); \
    if (!_throwNotListen && _pExceptionListener) \
    { \
      _pExceptionListener->exceptionThrown(ex); \
    } \
    else throw ex; \
  }

namespace AMPS
{

/// RecoveryPointAdapter virtual base class for implementing external
/// storage of subscription recovery points and recovery from storage.
  class SOWRecoveryPointAdapter : public RecoveryPointAdapterImpl
  {
  public:

    /// Create a SOWRecoveryPointAdapter for a BookmarkStore that writes
    /// updated RecoveryPoints to the server where storeClient_ is connected
    /// to the storeTopic_ using trackedClientName_ in the clientNameField_
    /// and the subId from the RecoveryPoint in the subIdField_ as the key
    /// fields and the bookmark from the RecoveryPoint in the bookmarkField_
    /// as the bookmark.
    /// \param storeClient_ A Client that is connected and logged on to a
    /// server containing the SOW for bookmark storage. This client must NOT
    /// be the tracked client.
    /// \param trackedClientName_ The name of the client for which this store
    /// is being used. It is an UsageException for trackedClientName_ to match
    /// the name of storeClient_.
    /// \param timeoutMillis_ The max time to wait for the sow query that
    /// returns bookmarks saved in the store to return the next record.
    /// \param useTimestamp_ If true, the recovered bookmarks will also
    /// include the update timestamp, which is useful if the bookmarks are no
    /// longer in the transaction log.
    /// \param closeClient_ If true, the storeClient_ will be disconnected when
    /// the adapter is closed or destructed. Default is true.
    /// \param updateFailureThrows_ If true, exceptions will be thrown out of
    /// this class to the BookmarkStore and eventually the tracked client's
    /// exception listener. If false, the default, exceptions are contained
    /// and only delivered to the exception listener set on this adapter.
    /// \param topic_ The name of the SOW topic in which to store
    /// the RecoveryPoints.
    /// \param clientNameField_ The name of key field in topic_ where the
    /// trackedClientName_ is saved for each RecoveryPoint.
    /// \param subIdField_ The name of the key field in topic_ where the subId
    /// for each saved RecoveryPoint is stored.
    /// \param bookmarkField_ The name of the field in topic_ where the
    /// bookmark from each RecoveryPoint is saved.
    SOWRecoveryPointAdapter(const Client& storeClient_,
                            const string& trackedClientName_,
                            unsigned timeoutMillis_ = 5000,
                            bool useTimestamp_ = false,
                            bool closeClient_ = true,
                            bool updateFailureThrows_ = false,
                            const string& topic_ = AMPS_SOW_STORE_DEFAULT_TOPIC,
                            const string& clientNameField_ = AMPS_SOW_STORE_DEFAULT_CLIENT_FIELD,
                            const string& subIdField_ = AMPS_SOW_STORE_DEFAULT_SUB_FIELD,
                            const string& bookmarkField_ = AMPS_SOW_STORE_DEFAULT_BOOKMARK_FIELD
                           )
      : RecoveryPointAdapterImpl()
      , _serializeLen(0)
      , _serializeBuffer(0)
      , _deserializeLen(0)
      , _deserializeBuffer(0)
      , _client(storeClient_)
      , _trackedName(trackedClientName_)
      , _topic(topic_)
      , _nameField(clientNameField_)
      , _subIdField(subIdField_)
      , _bookmarkField(bookmarkField_)
      , _timeoutMillis(timeoutMillis_)
      , _cmd("publish")
      , _closeClient(closeClient_)
      , _executed(false)
      , _throwNotListen(updateFailureThrows_)
      , _useTimestamp(useTimestamp_)
      , _closed(false)
    {
      _initSerialization();
      _cmd.setTopic(_topic);
    }

    virtual ~SOWRecoveryPointAdapter()
    {
      _close();
      delete[] _serializeBuffer;
      delete[] _deserializeBuffer;
    }

    /// Recovery is done by iteration over elements in storage. This function
    /// modifies the passed in argument to be the next stored RecoveryPoint
    /// or an empty RecoveryPoint to indicate completion.
    /// \param current_ The RecoveryPoint to set as the next recovery item.
    virtual bool next(RecoveryPoint& current_)
    {
      static Field emptyField;
      try
      {
        if (!_executed)
        {
          Command cmd("sow");
          cmd.setTopic(_topic)
          .setFilter("/" + _nameField + "='" + _trackedName + "'")
          .setTimeout(_timeoutMillis);
          if (_useTimestamp)
          {
            cmd.setOptions("select=[-/,+/" + _subIdField + ",+/"
                           + _bookmarkField + "],timestamp");
          }
          else
          {
            cmd.setOptions("select=[-/,+/" + _subIdField + ",+/"
                           + _bookmarkField + "]");
          }
          _stream = _client.execute(cmd).timeout(_timeoutMillis);
          _msIter = _stream.begin();
          _executed = true;
        }
        else
        {
          ++_msIter;
        }
        if (_msIter == MessageStream::iterator())
        {
          return false;
        }
        Message m = *_msIter;
        if (!m.isValid())
        {
          current_ = RecoveryPoint(NULL);
          return false;
        }
        if (m.getCommand() == "group_begin")
        {
          return next(current_);
        }
        else if (m.getCommand() == "sow")
        {
          if (_useTimestamp)
          {
            current_ = RecoveryPoint(deserialize(m.getData(),
                                                 m.getTimestamp()));
          }
          else
          {
            current_ = RecoveryPoint(deserialize(m.getData(),
                                                 emptyField));
          }
          return true;
        }
        else if (m.getCommand() == "group_end" || m.getCommand() == "ack")
        {
          current_ = RecoveryPoint(NULL);
          _msIter = MessageStream::iterator();
          _stream = MessageStream();
          return false;
        }
      }
      SOW_RECOVERY_HANDLE_EXCEPTION("SOWRecoveryPoint::next")
      return false;
    }

    /// Update the storage information with the given recovery point.
    /// \param recoveryPoint_ The new/updated RecoveryPoint to save.
    virtual void update(RecoveryPoint& recoveryPoint_)
    {
      try
      {
        Field data = serialize(recoveryPoint_);
        _cmd.setData(data.data(), data.len());
        _client.execute(_cmd);
      }
      SOW_RECOVERY_HANDLE_EXCEPTION("SOWRecoveryPoint::update")
    }

    /// Remove all data from the storage
    virtual void purge()
    {
      try
      {
        Message m = _client.sowDelete(_topic, "/" + _nameField
                                      + "='" + _trackedName + "'");
      }
      SOW_RECOVERY_HANDLE_EXCEPTION("SOWRecoveryPoint::purge")
    }

    /// Remove the specified subId_ from the storage
    /// \param subId_ The sub id to remove
    virtual void purge(const Field& subId_)
    {
      try
      {
        Message m = _client.sowDelete(_topic, "/" + _nameField + "='"
                                      + _trackedName + "' and /"
                                      + _subIdField + "='"
                                      + subId_ + "'");
      }
      SOW_RECOVERY_HANDLE_EXCEPTION("SOWRecoveryPoint::purge(subId)")
    }

    /// Take any necessary actions to close the associated storage.
    virtual void close()
    {
      _close();
    }

    /// Set an exception listener on this adapter that will be notified of
    /// all exceptions that occur rather than silently absorbing them. The
    /// exception listener will be ignored if the adapter is constructed with
    /// updateFailureThrows_ true; in that case exceptions will leave this
    /// adapter to be handled elsewhere.
    /// \param pListener_ A shared pointer to the ExceptionListener that
    /// should be notified.
    void setExceptionListener(const std::shared_ptr<const ExceptionListener>& pListener_)
    {
      _pExceptionListener = pListener_;
    }
  protected:
    void _close()
    {
      if (_closed || !_client.isValid())
      {
        return;
      }
      try
      {
        _client.publishFlush();
      }
      SOW_RECOVERY_HANDLE_EXCEPTION("SOWRecoveryPoint::close publishFlush")
      try
      {
        if (_closeClient)
        {
          _closed = true;
          _client.disconnect();
          _client = Client();
        }
      }
      SOW_RECOVERY_HANDLE_EXCEPTION("SOWRecoveryPoint::close disconnect")
    }

    void _initSerialization()
    {
      try
      {
        // Set up json serialization
        if (_serializeLen == 0)
        {
          _serializeLen = (size_t) (_nameField.length()
                                    + _trackedName.length()
                                    + _subIdField.length()
                                    + _bookmarkField.length()
                                    + (AMPS_MAX_BOOKMARK_LEN * 4UL)
                                    + SUBID_LEN + JSON_EXTRA);
          _serializeLen += (128 - (_serializeLen % 128));
        }
        _serializeBuffer = new char[_serializeLen];
        AMPS_snprintf(_serializeBuffer, _serializeLen,
                      "{\"%s\":\"%s\",\"%s\":\"", _nameField.c_str()
                      , _trackedName.c_str()
                      , _subIdField.c_str());
        _serializeStart = JSON_START + _nameField.length()
                          + _trackedName.length() + _subIdField.length();
      }
      SOW_RECOVERY_HANDLE_EXCEPTION("SOWRecoveryPoint::initSerialization")
    }

    // Subclasses can override this to set up for something other than json
    // serialization if not using json.
    virtual void initSerialization()
    {
      _initSerialization();
    }

    // Subclasses can override this function if not using json data type.
    // It needs to return an allocated RecoveryPointImpl based on the data
    // field from a sow message that contains only 2 fields: _subIdField and
    // _bookmarkField. If you'd like more, override begin()
    virtual RecoveryPointImpl* deserialize(const Field& data_,
                                           const Field& timestamp_)
    {
      Field subId;
      Field bookmark;
      try
      {
        // We have 2 fields subId and bookmark and we only need the
        // values. Find : then start ", then end ".
        const char* start = (const char*)memchr((const void*)data_.data(),
                                                (int)':', data_.len());
        if (!start)
        {
          throw StoreException("Failure parsing json RecoveryPoint subId, no :");
        }
        size_t remain = data_.len() - (size_t)(start - data_.data());
        start = (const char*)memchr((const void*)start, (int)'"', remain);
        if (!start)
        {
          throw StoreException("Failure parsing json RecoveryPoint subId, no start \"");
        }
        ++start;
        remain = data_.len() - (size_t)(start - data_.data());
        const char* end = (const char*)memchr((const void*)start,
                                              (int)'"', remain);
        if (!end)
        {
          throw StoreException("Failure parsing json RecoveryPoint subId, no end \"");
        }
        size_t len = (size_t)(end - start);
        subId = Field(start, len);
        start = (const char*)memchr((const void*)start, (int)':', data_.len());
        if (!start)
        {
          throw StoreException("Failure parsing json RecoveryPoint bookmark, no :");
        }
        remain = data_.len() - (size_t)(start - data_.data());
        start = (const char*)memchr((const void*)start, (int)'"', remain);
        if (!start)
        {
          throw StoreException("Failure parsing json RecoveryPoint bookmark, no start \"");
        }
        ++start;
        remain = data_.len() - (size_t)(start - data_.data());
        end = (const char*)memchr((const void*)start, (int)'"', remain);
        if (!end)
        {
          throw StoreException("Failure parsing json RecoveryPoint bookmark, no end \"");
        }
        len = (size_t)(end - start);
        if (_useTimestamp && !timestamp_.empty())
        {
          if (_deserializeLen < len + timestamp_.len())
          {
            delete[] _deserializeBuffer;
            _deserializeBuffer = 0;
          }
          if (!_deserializeBuffer)
          {
            _deserializeLen = len + timestamp_.len() + 1;
            _deserializeBuffer = new char[_deserializeLen];
          }
          memcpy((void*)_deserializeBuffer, (const void*)start, len);
          _deserializeBuffer[len] = ',';
          memcpy((void*)(_deserializeBuffer + len + 1),
                 (const void*)timestamp_.data(), timestamp_.len());
          bookmark = Field(_deserializeBuffer, _deserializeLen);
        }
        else
        {
          bookmark = Field(start, len);
        }
      }
      SOW_RECOVERY_HANDLE_EXCEPTION("SOWRecoveryPoint::deserialize")
      // Return a recovery point that will copy current field values and
      // clear them when destructed.
      return new FixedRecoveryPoint(subId, bookmark, true);
    }

    virtual Field& serialize(const RecoveryPoint& recoveryPoint_)
    {
      try
      {
        Field subId = recoveryPoint_.getSubId();
        Field bookmark = recoveryPoint_.getBookmark();
        size_t fullLen = _serializeStart + subId.len()
                         + _bookmarkField.length() + bookmark.len() + JSON_END;
        if (fullLen >= _serializeLen)
        {
          _serializeLen = fullLen + (128 - (fullLen % 128));
          delete[] _serializeBuffer;
          // This will reallocate the buffer and fill with predicate
          initSerialization();
        }
        AMPS_snprintf(_serializeBuffer + _serializeStart,
                      _serializeLen - _serializeStart,
                      "%.*s\",\"%s\":\"%.*s\"}", (int)subId.len()
                      , subId.data()
                      , _bookmarkField.c_str()
                      , (int)bookmark.len()
                      , bookmark.data());
        _serializeField.assign(_serializeBuffer, fullLen);
      }
      SOW_RECOVERY_HANDLE_EXCEPTION("SOWRecoveryPoint::serialize")
      return _serializeField;
    }

    enum Constants : size_t
    {
      JSON_START = 11, // '{', 7 '"', 2 ':', 1 ','
      JSON_END   =  8, // '}', 5 '"', 1 ':', 1 ','
      JSON_EXTRA = 19, // '{', '}', 3 ':', 12 '"', 2 ','
      SUBID_LEN  = 64  // rough guess on typical max len
    };

  private:
    size_t                                      _serializeLen;
    size_t                                      _serializeStart;
    Field                                       _serializeField;
    char*                                       _serializeBuffer;
    size_t                                      _deserializeLen;
    char*                                       _deserializeBuffer;
    Client                                      _client;
    std::string                                 _trackedName;
    std::string                                 _topic;
    std::string                                 _nameField;
    std::string                                 _subIdField;
    std::string                                 _bookmarkField;
    unsigned                                    _timeoutMillis;
    Command                                     _cmd;
    MessageStream                               _stream;
    MessageStream::iterator                     _msIter;
    std::shared_ptr<const ExceptionListener>    _pExceptionListener;
    bool                                        _closeClient;
    bool                                        _executed;
    bool                                        _throwNotListen;
    bool                                        _useTimestamp;
    bool                                        _closed;
  };
} // namespace AMPS
#endif //_SOWRECOVERYPOINTADAPTER_H_

