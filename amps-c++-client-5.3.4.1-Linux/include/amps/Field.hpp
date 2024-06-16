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
#ifndef __AMPS_FIELD_HPP__
#define __AMPS_FIELD_HPP__

#define _AMPS_SKIP_AMPSPLUSPLUS
#include <amps/amps.h>
#undef _AMPS_SKIP_AMPSPLUSPLUS
#include <algorithm>
#include <string>
#include <string.h>
#include <vector>

#define AMPS_UNSET_INDEX    (size_t)-1
#define AMPS_TIMESTAMP_LEN 16
#define AMPS_TIMESTAMP_LEN_LONG 23
#define AMPS_MAX_BOOKMARK_LEN 42
// Used in bookmark store recovery to look for corruption, NOT FIXED IN AMPS
#define AMPS_MAX_SUBID_LEN 1048576 // 1MB, NOT FIXED IN AMPS
#ifdef _WIN32
  #if (_MSC_VER >= 1400) // VS2005 or higher
    #define AMPS_snprintf(buf_, sz_, ...) _snprintf_s(buf_, sz_, _TRUNCATE, __VA_ARGS__)
    #define AMPS_snprintf_amps_uint64_t(buf,sz,val) sprintf_s(buf,sz,"%I64u",val)
    #ifdef _WIN64
      #define AMPS_snprintf_sizet(buf,sz,val) sprintf_s(buf,sz,"%lu",val)
    #else
      #define AMPS_snprintf_sizet(buf,sz,val) sprintf_s(buf,sz,"%u",val)
    #endif
  #else // VS2003 or older
    #define AMPS_snprintf _snprintf
    #ifdef _WIN64
      #define AMPS_snprintf_sizet(buf,sz,val) _sprintf(buf,sz,"%lu",val)
    #else
      #define AMPS_snprintf_sizet(buf,sz,val) _sprintf(buf,sz,"%u",val)
    #endif
  #endif
#else
  #define AMPS_snprintf snprintf
  #if defined(__x86_64__) || defined(__aarch64__)
    #define AMPS_snprintf_amps_uint64_t(buf,sz,val) snprintf(buf,sz,"%lu", (unsigned long)val)
    #define AMPS_snprintf_sizet(buf,sz,val) snprintf(buf,sz,"%lu",val)
  #else
    #define AMPS_snprintf_amps_uint64_t(buf,sz,val) snprintf(buf,sz,"%llu",val)
    #define AMPS_snprintf_sizet(buf,sz,val) snprintf(buf,sz,"%u",val)
  #endif
#endif

namespace AMPS
{

  using std::string;

/// \file  Field.hpp
/// \brief Defines the AMPS::Field class, which represents the value of a
///        field in a message.

///
/// Field represents the value of a single field in a Message.
/// Field has limited string functionality, but doesn't require a copy of
/// the data to be made until you want to copy it out to your own
/// std::string.
///

  class Field
  {
  protected:
    const char* _data;
    size_t _len;
  public:
    Field() : _data(NULL), _len(0) {;}
    Field(const char* data_)
    {
      _data = data_;
      _len = ::strlen(data_);
    }
    Field(const char* data_, size_t len_)
    {
      _data = data_;
      _len = len_;
    }
    Field(const Field& rhs)
    {
      _data = rhs._data;
      _len = rhs._len;
    }
    Field& operator=(const Field& rhs)
    {
      _data = rhs._data;
      _len = rhs._len;
      return *this;
    }
    Field(const std::string& string_)
    {
      _data = string_.c_str();
      _len  = string_.length();
    }

    bool contains(const char* searchString, size_t len) const
    {
      const char* dataEnd = _data + _len;
      return std::search(_data, dataEnd, searchString, searchString + len) != dataEnd;
    }

    ///
    /// Returns 'true' if empty, 'false' otherwise.
    bool empty () const
    {
      return _len == 0;
    }

    ///
    /// Conversion operator to std::string.
    /// Makes a copy of the data in self.
    operator std::string () const
    {
      return _len ? std::string(_data, _len) : std::string();
    }

    ///
    /// Comparison operator
    /// Returns `true' if self and rhs are equivalent, `false' otherwise
    /// \param rhs_  Field to compare against.
    ///
    bool operator==(const Field& rhs_) const
    {
      if ( _len == rhs_._len )
      {
        return ::memcmp(_data, rhs_._data, _len) == 0;
      }
      return false;
    }

    ///
    /// String comparison operator
    /// Returns `true' if self and rhs are equivalent, `false' otherwise
    /// \param rhs_  Null-terminated string to compare against.
    ///
    bool operator==(const char* rhs_) const
    {
      if (!_data || !rhs_)
      {
        return (!_data && !rhs_);
      }
      return ::strncmp(_data, rhs_, _len) == 0 && rhs_[_len] == '\0';
    }

    bool operator<(const Field& rhs) const;

    ///
    /// Comparison operator
    /// Returns `true` if self and rhs are not equivalent.
    /// \param rhs_  Field to compare against.
    ///
    bool operator!=(const Field& rhs_) const
    {
      if ( _len == rhs_._len )
      {
        return ::memcmp(_data, rhs_._data, _len) != 0;
      }
      return true;
    }

    ///
    /// String comparison operator
    /// Returns `true` if self and rhs are not equivalent.
    /// \param rhs_  Null-terminated string to compare against.
    ///
    bool operator!=(const char* rhs_) const
    {
      return strncmp(_data, rhs_, _len) != 0 || rhs_[_len] != '\0';
    }

    ///
    /// String comparison operator
    /// Returns `true' if self and rhs are not equivalent.
    /// \param rhs_ std::string to compare against.
    ///
    bool operator!=(const std::string& rhs_) const
    {
      return rhs_.compare(0, rhs_.length(), _data, _len) != 0;
    }

    ///
    /// String comparison operator
    /// Returns `true' if self and rhs are equivalent.
    /// \param rhs_ std::string to compare against.
    ///
    bool operator==(const std::string& rhs_) const
    {
      return rhs_.compare(0, rhs_.length(), _data, _len) == 0;
    }

    ///
    /// Makes self a deep copy of the original field
    /// \param orig_ Field to copy
    void deepCopy(const Field& orig_)
    {
      delete[] _data;
      if (orig_._len > 0)
      {
        _data = new char[orig_._len];
        ::memcpy(static_cast<void*>(const_cast<char*>(_data)),
                 orig_._data, orig_._len);
        _len = orig_._len;
      }
      else
      {
        _data = NULL;
        _len = 0;
      }
    }

    ///
    /// Makes a deep copy of self, returns it.
    Field deepCopy() const
    {
      Field newField;
      newField.deepCopy(*this);
      return newField;
    }

    ///
    /// Deletes the data associated with this Field, should only be
    /// used on Fields that were created as deepCopy of other Fields.
    void clear()
    {
      if (!_data || !_len)
      {
        return;
      }
      delete[] _data;
      _len = 0;
      _data = NULL;
    }

    ///
    /// Returns the (non-null-terminated) data underlying this field.
    const char* data() const
    {
      return _data;
    }

    ///
    /// Returns the length of the data underlying this field.
    size_t len() const
    {
      return _len;
    }

    // assign a new range into this Message::Field
    void assign(const char* ptr, size_t len)
    {
      _data = ptr;
      _len = len;
    }

    // compute a hash value
    inline size_t hash_function() const
    {
      size_t n_ = _len;
      const char* p_ = _data;
      size_t h = 0, c;
      while (n_ != 0)
      {
        c = (unsigned long) * p_;
        h += (h << 5) + c;
        ++p_, --n_;
      }
      return h;
    }

    struct FieldHash
    {
      size_t operator()(const Field& f) const
      {
        return f.hash_function();
      }

      bool operator()(const Field& f1, const Field& f2) const
      {
        if (f1.len() < f2.len())
        {
          return true;
        }
        if (f1.len() > f2.len())
        {
          return false;
        }
        // Length is the same, don't compare empty
        if (f1.len() == 0)
        {
          return true;
        }
        return ::memcmp(f1.data(), f2.data(), f2.len()) < 0;
      }
    };

    // Determine if the Field represents a timestamp
    static bool isTimestamp(const Field& field_)
    {
      return (field_.len() >= AMPS_TIMESTAMP_LEN
              && field_.len() <= AMPS_TIMESTAMP_LEN_LONG
              && field_.data()[8] == 'T');
    }

    static std::vector<Field> parseBookmarkList(const Field& field_)
    {
      std::vector<Field> list;
      const char* start = field_.data();
      size_t remain = field_.len();
      const char* comma = (const char*)memchr((const void*)start,
                                              (int)',', remain);
      while (comma)
      {
        size_t len = (size_t)(comma - start);
        if (len != 0)
        {
#ifdef AMPS_USE_EMPLACE
          list.emplace_back(Field(start, len));
#else
          list.push_back(Field(start, len));
#endif
        }
        start = ++comma;
        remain = field_.len() - (size_t)(start - field_.data());
        comma = (const char*)memchr((const void*)start,
                                    (int)',', remain);
      }
      if (remain != 0)
      {
#ifdef AMPS_USE_EMPLACE
        list.emplace_back(Field(start, remain));
#else
        list.push_back(Field(start, remain));
#endif
      }
      return list;
    }

    // Get sequence number from a Field that is a bookmark
    static void parseBookmark(const Field& field_,
                              amps_uint64_t& publisherId_,
                              amps_uint64_t& sequenceNumber_)
    {
      publisherId_ = sequenceNumber_ = (amps_uint64_t)0;
      const char* data = field_.data();
      size_t len = field_.len();
      // Can't parse a timestamp
      if (isTimestamp(field_))
      {
        return;
      }
      size_t i = 0;
      for ( ; i < len; ++i)
      {
        if (!isdigit(data[i]))
        {
          break;
        }
        publisherId_ *= 10;
        publisherId_ += (amps_uint64_t)(data[i] - '0');
      }
      // Make sure it's just the | separator
      if (i < len && data[i] != '|')
      {
        publisherId_ = sequenceNumber_ = (amps_uint64_t)0;
        return;
      }
      for (i = i + 1; i < len; ++i)
      {
        if (!isdigit(data[i]))
        {
          break;
        }
        sequenceNumber_ *= 10;
        sequenceNumber_ += (amps_uint64_t)(data[i] - '0');
      }
    }

  };

  class BookmarkRange : public Field
  {
  public:
    static bool isRange(const Field& bookmark_)
    {
      return memchr(bookmark_.data(), ':', bookmark_.len()) != NULL;
    }

    BookmarkRange()
      : Field(), _start(), _end(), _open(AMPS_UNSET_INDEX)
      , _capacity(AMPS_UNSET_INDEX)
    {
    }

    // Parse it for a range, set everything empty if not a valid range
    BookmarkRange(const Field& field_)
      : Field(), _start(), _end(), _open(AMPS_UNSET_INDEX)
      , _capacity(field_.len())
    {
      set(field_);
    }

    void set(const Field& field_)
    {
      // Are we already the same
      if (_data == field_.data() || operator==(field_))
      {
        return;
      }
      // Reset self
      notValid();
      // Make self a copy
      deepCopy(field_);
      _capacity = _len;
      bool foundSeparator = false;
      bool foundClose = false;
      for (size_t i = 0; i < _len; ++i)
      {
        switch (_data[i])
        {
        case '(':
        case '[':
        {
          // Is this within the range?
          if (foundClose || _open != AMPS_UNSET_INDEX)
          {
            notValid();
            return;
          }
          _open = i;
        }
        break;
        // Valid bookmark characters [0-9|,TZ]
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
        case '|':
        case ',':
        case 'T':
        case 'Z':
        {
          // Is this within the range?
          if (foundClose || _open == AMPS_UNSET_INDEX)
          {
            notValid();
            return;
          }
          else if (foundSeparator) // Part of end?
          {
            if (!_end.data()) // Start of end?
            {
              _end.assign(_data + i, 0);
            }
          }
          else if (!_start.data()) // Start of start?
          {
            _start.assign(_data + i, 0);
          }
        }
        break;
        case ':':
        {
          // Is this within the range and do we have a start?
          if (foundSeparator || foundClose || _open == AMPS_UNSET_INDEX
              || !_start.data())
          {
            notValid();
            return;
          }
          foundSeparator = true;
          // Do we need to set start length?
          if (_start.len() == 0)
          {
            // Length is here, minus beginning of start - 1
            _start.assign(_start.data(), i - (size_t)(_start.data() - _data));
          }
        }
        break;
        case ']':
        case ')':
        {
          // Is this within the range and do we have an end?
          if (foundClose || _open == AMPS_UNSET_INDEX || !_end.data())
          {
            notValid();
            return;
          }
          foundClose = true;
          _len = i + 1;
          // Do we need to set end length?
          if (_end.len() == 0)
          {
            // Length is here, minus beginning of end - 1
            _end.assign(_end.data(), i - (size_t)(_end.data() - _data));
          }
        }
        break;
        case ' ':
        {
          // Do we need to set end length?
          if (_end.data() && _end.len() == 0)
          {
            // Length is here, minus beginning of end - 1
            _end.assign(_end.data(), i - (size_t)(_end.data() - _data));
          }
          // Else do we need to set start length?
          else if (_start.data() && _start.len() == 0)
          {
            // Length is here, minus beginning of start - 1
            _start.assign(_start.data(), i - (size_t)(_start.data() - _data));
          }
        }
        break;
        default:
        {
          notValid();
        }
        break;
        }
      }
      // If we didn't find everything clear self
      if (_start.empty() || _end.empty())
      {
        notValid();
      }
    }

    // Centralized place to clear self
    void notValid()
    {
      if (!_data || !_len)
      {
        return;
      }
      delete[] _data;
      _data = 0;
      _len = 0;
      _start.assign(0, 0);
      _end.assign(0, 0);
      _open = AMPS_UNSET_INDEX;
      _capacity = 0;
    }

    /// If this range was created with a valid range, return true.
    /// \return If this is a valid range.
    bool isValid() const
    {
      return !empty();
    }

    /// Does the range start inclusive of the first bookmark.
    /// \return If the range start inclusive of the first bookmark.
    bool isStartInclusive() const
    {
      return _data[_open] == '[';
    }

    /// Does the range start inclusive of the first bookmark.
    /// \return If the range start inclusive of the first bookmark.
    void makeStartExclusive()
    {
      const_cast<char*>(_data)[_open] = '(';
    }

    /// Does the range end inclusive of the last bookmark.
    /// \return If the range end inclusive of the last bookmark.
    bool isEndInclusive() const
    {
      return _data[_len - 1] == ']';
    }

    /// Does the range end inclusive of the last bookmark.
    /// \return If the range end inclusive of the last bookmark.
    void makeEndExclusive()
    {
      const_cast<char*>(_data)[_len - 1] = ')';
    }

    /// The start bookmark
    /// \return The starting bookmark in the range.
    const Field& getStart() const
    {
      return _start;
    }

    const Field& getEnd() const
    {
      return _end;
    }

    /// This copies start_ to replace the beginning of the range.
    /// \param start_ The bookmark field to copy as this range's start.
    void replaceStart(const Field& start_, bool makeExclusive_ = false)
    {
      // Best case, it fits in our existing self. Since we may do this more
      // than once, just add start+end+open+close+separator
      if (_capacity >= (start_.len() + _end.len() + 3))
      {
        char* data = const_cast<char*>(_data);
        if (makeExclusive_)
        {
          data[_open] = '(';
        }
        if (_open) // Move open to beginning if not there
        {
          data[0] = _data[_open];
          _open = 0;
        }
        if ((size_t)(_end.data() - _data - 2) < start_.len())
        {
          size_t newLen = start_.len() + _end.len() + 3;
          // Need to move end to make room for new start
          // This copies _end and close
          // Last char of _start will be at _start.len() because of open
          for (size_t dest = newLen - 1, src = _len - 1;
               src > _start.len(); --src, --dest)
          {
            data[dest] = _data[src];
            // Find separator, we're done
            if (data[src] == ':')
            {
              _end.assign(data + dest + 1, _end.len());
              break;
            }
          }
          _len = newLen;
        }
        // Copy in new start after _open
        ::memcpy(static_cast<void*>(data + 1), start_.data(), start_.len());
        _start.assign(data + 1, start_.len());
        // Possibly move end left
        if ((size_t)(_end.data() - _start.data()) > _start.len() + 1UL)
        {
          // Copy to just after start starting with ':'
          for (size_t dest = _start.len() + 1, src = (size_t)(_end.data() - data - 1);
               src < _len; ++dest, ++src)
          {
            data[dest] = data[src];
            if (data[src] == ']' || data[src] == ')')
            {
              _end.assign(data + _start.len() + 2, _end.len());
              break;
            }
          }
          _len = _start.len() + _end.len() + 3;
        }
      }
      else // We need to resize and copy everything over
      {
        // Let's set min resize at 4 bookmarks + 3 commas + 3
        // Some MSVC versions have issues with max so use ?:
        _capacity = (4UL * AMPS_MAX_BOOKMARK_LEN + 6UL
                     >= start_.len() + _end.len() + 3UL)
                    ? (4UL * AMPS_MAX_BOOKMARK_LEN + 6UL)
                    : (start_.len() + _end.len() + 3UL);
        char* data = new char[_capacity];
        if (makeExclusive_)
        {
          data[0] = '(';
        }
        else
        {
          data[0] = _data[_open];
        }
        _open = 0;
        ::memcpy(static_cast<void*>(data + 1), start_.data(), start_.len());
        _start.assign(data + 1, start_.len());
        data[start_.len() + 1] = ':';
        ::memcpy(static_cast<void*>(data + start_.len() + 2), _end.data(),
                 _end.len());
        _end.assign(data + start_.len() + 2, _end.len());
        size_t len = start_.len() + 3 + _end.len();
        data[len - 1] = _data[_len - 1];
        clear();
        assign(data, len);
      }
    }

  private:
    Field _start;
    Field _end;
    size_t _open;
    size_t _capacity;
  };

}

#endif

