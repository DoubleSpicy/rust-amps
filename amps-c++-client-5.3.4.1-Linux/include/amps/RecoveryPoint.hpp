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

#ifndef _RECOVERYPOINT_H_
#define _RECOVERYPOINT_H_

#include <amps/BookmarkStore.hpp>
#include <amps/Field.hpp>
#include <amps/util.hpp>

/// \file RecoveryPoint.hpp
/// \brief Provides AMPS::RecoveryPoint, AMPS::RecoveryPointFactory,
/// AMPS::FixedRecoveryPoint, and AMPS::DynamicRecoveryPoint.
namespace AMPS
{

///
/// \class RecoveryPoint
/// \brief Provides access to the subId and bookmark needed to restart
/// a subscription.
///
  class RecoveryPoint;

///
/// RecoveryPointImpl virtual base class provides access to the subId and
/// bookmark needed to restart a subscription.
  class RecoveryPointImpl : public RefBody
  {
  public:
    virtual ~RecoveryPointImpl() { }
    /// Get the sub id for this recovery point.
    /// \return The sub id.
    virtual const Field& getSubId() const = 0;
    /// Get the bookmark for this recovery point.
    /// \return The bookmark.
    virtual const Field& getBookmark() const = 0;
    /// Return a deep copy of self
    virtual RecoveryPointImpl* deepCopy() = 0;
    /// Make self a deep copy of original_
    virtual RecoveryPointImpl* deepCopy(const RecoveryPointImpl& original_) = 0;
    /// Clear the internal state, possibly reclaiming memory
    virtual void clear() = 0;
  };

  class RecoveryPoint
  {
  public:
    RecoveryPoint() : _body() { }

    RecoveryPoint(RecoveryPointImpl* body_)
      : _body(body_)
    { }

    RecoveryPoint(const RecoveryPoint& rhs_)
      : _body(rhs_._body)
    { }

    ~RecoveryPoint() { }

    /// Get the sub id for this recovery point.
    /// \return The sub id.
    const Field& getSubId() const
    {
      return _body.get().getSubId();
    }

    /// Get the bookmark for this recovery point.
    /// \return The bookmark.
    const Field& getBookmark() const
    {
      return _body.get().getBookmark();
    }

    /// Return a deep copy of self
    RecoveryPoint deepCopy()
    {
      return _body.get().deepCopy();
    }

    /// Make self a deep copy of original_
    RecoveryPoint deepCopy(const RecoveryPoint& original_)
    {
      return _body.get().deepCopy(original_._body.get());
    }

    /// Clear the internal state, possibly reclaiming memory
    void clear()
    {
      return _body.get().clear();
    }

    RecoveryPoint& operator=(const RecoveryPoint& rhs_)
    {
      _body = rhs_._body;
      return *this;
    }
  private:
    RefHandle<RecoveryPointImpl> _body;
  };

///
/// RecoveryPointFactory is a function type for producing a RecoveryPoint that
/// is sent to a {@link RecoveryPointAdapter}.
  typedef RecoveryPoint (*RecoveryPointFactory)(const Field& subId_,
                                                const Field& bookmark_);

///
/// FixedRecoveryPoint is a RecoveryPoint implementation where subId and
/// bookmark are set explicitly. This is normally used by RecoveryPointAdapter
/// implementations during recover.
  class FixedRecoveryPoint : public RecoveryPointImpl
  {
  public:
    /// Use this function in BookmarkStore::setRecoveryPointFactory(
    /// std::bind(&FixedRecoveryPoint::create, std::placeholder::_1,
    /// std::placeholder::_2))
    static RecoveryPoint create(const Field& subId_, const Field& bookmark_)
    {
      return RecoveryPoint(new FixedRecoveryPoint(subId_, bookmark_));
    }

    FixedRecoveryPoint() : RecoveryPointImpl(), _owner(false) { }

    FixedRecoveryPoint(const Field& subId_, const Field& bookmark_)
      : _subId(subId_), _bookmark(bookmark_), _owner(false)
    {
    }

    FixedRecoveryPoint(const Field& subId_, const Field& bookmark_,
                       bool deepCopy_)
      : _owner(deepCopy_)
    {
      if (_owner)
      {
        _subId.deepCopy(subId_);
        _bookmark.deepCopy(bookmark_);
      }
      else
      {
        _subId = subId_;
        _bookmark = bookmark_;
      }
    }

    virtual ~FixedRecoveryPoint()
    {
      if (_owner)
      {
        _subId.clear();
        _bookmark.clear();
      }
    }

    virtual const Field& getSubId() const
    {
      return _subId;
    }

    virtual const Field& getBookmark() const
    {
      return _bookmark;
    }

    virtual RecoveryPointImpl* deepCopy()
    {
      return new FixedRecoveryPoint(_subId, _bookmark, true);
    }

    virtual RecoveryPointImpl* deepCopy(const RecoveryPointImpl& original_)
    {
      if (!_owner)
      {
        // deepCopy calls clear() so need to avoid that
        _subId = Field();
        _bookmark = Field();
      }
      _owner = true;
      _subId.deepCopy(original_.getSubId());
      _bookmark.deepCopy(original_.getBookmark());
      return this;
    }

    /// Clear the internal state, possibly reclaiming memory
    virtual void clear()
    {
      if (_owner)
      {
        _subId.clear();
        _bookmark.clear();
        _owner = false;
      }
      else
      {
        _subId = Field();
        _bookmark = Field();
      }
    }

  private:
    Field _subId;
    Field _bookmark;
    bool  _owner;
  };

///
/// DynamicRecoveryPoint is a RecoveryPoint implementation where subId is set
/// explicitly but bookmark is retrieved from the BookmarkStore as its
/// most recent at the time of access. This can be used instead of the default
/// FixedRecoveryPoint by using the create() method as a
/// RecoveryPointFactory when setting an adapter on the underlying
/// BookmarkStore
  class DynamicRecoveryPoint : public RecoveryPointImpl
  {
  public:
    /// Use this function in BookmarkStore::setRecoveryPointFactory(
    /// std::bind(&DynamicRecoveryPoint::create, std::placeholder::_1,
    /// std::placeholder::_2, std::ref(bookmarkStore)))
    static RecoveryPoint create(const Field& subId_, const Field&,
                                const BookmarkStore& store_)
    {
      return RecoveryPoint(new DynamicRecoveryPoint(subId_, store_));
    }

    DynamicRecoveryPoint(const Field& subId_,
                         const BookmarkStore& store_,
                         bool owner_ = false)
      : RecoveryPointImpl(), _subId(subId_), _store(store_), _owner(owner_)
    {
      if (_owner)
      {
        _subId.deepCopy(subId_);
      }
    }

    virtual ~DynamicRecoveryPoint()
    {
      if (_owner)
      {
        _subId.clear();
      }
    }

    virtual const Field& getSubId() const
    {
      return _subId;
    }

    virtual const Field& getBookmark() const
    {
      _bookmark = _store.getMostRecent(_subId);
      return _bookmark;
    }

    virtual RecoveryPointImpl* deepCopy()
    {
      return new DynamicRecoveryPoint(_subId, _store, true);
    }

    virtual RecoveryPointImpl* deepCopy(const RecoveryPointImpl& original_)
    {
      if (!_owner)
      {
        // deepCopy calls clear() so need to avoid that
        _subId = Field();
      }
      _owner = true;
      _subId.deepCopy(original_.getSubId());
      if (typeid(*this) == typeid(original_))
      {
        _store = ((DynamicRecoveryPoint*)&original_)->_store;
      }
      return this;
    }

    /// Clear the internal state, possibly reclaiming memory
    virtual void clear()
    {
      if (_owner)
      {
        _subId.clear();
        _owner = false;
      }
      else
      {
        _subId = Field();
      }
    }

  private:
    Field _subId;
    mutable Field _bookmark;
    mutable BookmarkStore _store;
    bool  _owner;

    DynamicRecoveryPoint();
  };

}

#endif //_RECOVERYPOINT_H_

