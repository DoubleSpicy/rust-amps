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

#ifndef _AMPS_COMPOSITEMESSAGEBUILDER_HPP_
#define _AMPS_COMPOSITEMESSAGEBUILDER_HPP_

#include <string>

namespace AMPS
{
  /// Used to create payloads for AMPS composite messages, which are messages with
  /// a number of parts where each part is a complete message of a specific
  /// message type.
  /// For example, a composite message type of "composite-json-binary" may be
  /// declared on the server that combines a set of JSON headers with
  /// an opaque binary payload. CompositeMessageBuilder makes it easy to assemble
  /// this payload.
  class CompositeMessageBuilder
  {
  public:
    static const size_t DEFAULT_INITIAL_CAPACITY = 16 * 1024;
    static const size_t PART_HEADER_LENGTH = 4;
    ///
    /// Create a new, empty CompositeMessageBuilder.
    /// \param initialCapacity_ The initial capacity (bytes) for this builder.
    ///
    CompositeMessageBuilder(size_t initialCapacity_ =
                              DEFAULT_INITIAL_CAPACITY);

    ~CompositeMessageBuilder(void);
    ///
    /// Appends a message part to this object.
    /// \param data_ The data to append.
    /// \returns this object.
    ///
    CompositeMessageBuilder& append(const std::string& data_);
    ///
    /// Appends a message part to this object.
    /// \param data_ The data to append.
    /// \param length_ The length of the data to append.
    /// \returns this object.
    ///
    CompositeMessageBuilder& append(const char* data_, size_t length_);

    ///
    /// Clears this object. Does not resize or free internal buffer.
    /// \returns this object.
    ///
    CompositeMessageBuilder& clear(void);

    ///
    /// Returns the composite message's data.
    /// \returns a pointer to the beginning of the composite message data.
    ///
    const char* data(void) const;
    ///
    /// Returns the length of the composite message's data.
    /// \returns the length in bytes of the data comprising this message.
    ///
    size_t length(void) const;

  protected:
    // Not implemented.
    CompositeMessageBuilder(const CompositeMessageBuilder&);
    CompositeMessageBuilder& operator=(const CompositeMessageBuilder&);
  private:
    void _resize(size_t required_);

    char*       _data;
    size_t      _position;
    size_t      _capacity;
  };

  inline CompositeMessageBuilder::CompositeMessageBuilder(size_t initialCapacity_)
    : _data(new char[initialCapacity_]),
      _position(0),
      _capacity(initialCapacity_)
  {;}

  inline CompositeMessageBuilder::~CompositeMessageBuilder(void)
  {
    delete [] _data;
    _data = NULL;
  }

  inline CompositeMessageBuilder&
  CompositeMessageBuilder::append(const std::string& data_)
  {
    size_t length = data_.length();
    size_t required = _position + length + PART_HEADER_LENGTH;
    if (_capacity < required)
    {
      _resize(required);
    }
    char* p = _data + _position;
    *p++ = (char)((length & 0xFF000000) >> 24);
    *p++ = (char)((length & 0x00FF0000) >> 16);
    *p++ = (char)((length & 0x0000FF00) >>  8);
    *p++ = (char)((length & 0x000000FF)      );
    memcpy(p, data_.c_str(), length);
    _position += length + PART_HEADER_LENGTH;
    return *this;
  }
  inline CompositeMessageBuilder&
  CompositeMessageBuilder::append(const char* data_, size_t length_)
  {
    size_t required = _position + length_ + PART_HEADER_LENGTH;
    if (_capacity < required)
    {
      _resize(required);
    }
    char* p = _data + _position;
    *p++ = (char)((length_ & 0xFF000000) >> 24);
    *p++ = (char)((length_ & 0x00FF0000) >> 16);
    *p++ = (char)((length_ & 0x0000FF00) >>  8);
    *p++ = (char)((length_ & 0x000000FF)      );
    memcpy(p, data_, length_);
    _position += length_ + PART_HEADER_LENGTH;
    return *this;
  }
  inline const char*
  CompositeMessageBuilder::data(void) const
  {
    return _data;
  }
  inline size_t
  CompositeMessageBuilder::length(void) const
  {
    return _position;
  }
  inline CompositeMessageBuilder&
  CompositeMessageBuilder::clear(void)
  {
    _position = 0;
    return *this;
  }
  inline void
  CompositeMessageBuilder::_resize(size_t required_)
  {
    if (required_ <= _capacity)
    {
      return;
    }
    char* newData = new char[required_];
    memcpy(newData, _data, _position);
    delete[] _data;
    _data = newData;
    _capacity = required_;
  }
}

#endif
