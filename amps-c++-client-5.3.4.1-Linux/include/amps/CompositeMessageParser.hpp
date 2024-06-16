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

#ifndef _AMPS_COMPOSITEMESSAGEPARSER_HPP_
#define _AMPS_COMPOSITEMESSAGEPARSER_HPP_
#include <amps/Field.hpp>
#include <vector>

namespace AMPS
{
  ///
  /// Used to retrieve individual message parts from AMPS composite messages,
  /// which are messages where the data contains a number of parts and each part
  /// is a complete message of a specific message type.
  /// For example, a composite message type of "composite-json-binary" may be
  /// declared on the server that combines a set of JSON headers with
  /// an opaque binary payload. CompositeMessageParser makes it easy to retrieve
  /// the parts of a composite message from the composite message payload.
  /// CompositeMessageParser does not make a copy of the underlying data
  /// parsed: your application must ensure that the underlying data is
  /// valid while using the CompositeMessageParser.
  ///
  class CompositeMessageParser
  {
  public:
    /// Creates a new CompositeMessageParser with 0 valid parts.
    CompositeMessageParser(void);

    ///
    /// Parses a composite message, first clearing any existing data in
    /// the parser. This function is designed to accept an AMPS::Message
    /// whose data is a composite message. It will also work for any other
    /// class that implements a getDataRaw(char**, size_t*) that sets the
    /// provided char* to the start of the payload and the provided
    /// size_t to the length of the data.
    /// \param message_ The AMPS::Message whose data is a composite message.
    /// \returns The number of valid composite message parts found in message_.
    ///
    template <class T>
    size_t parse(const T& message_);

    ///
    /// Parses a composite message body, first clearing any existing data in
    /// the parser.
    /// \param data_ A pointer to the composite message data.
    /// \param length_ The length, in bytes, of the composite message data at data_.
    /// \returns The number of valid composite message parts found in message_.
    ///
    size_t parse(const char* data_, size_t length_);

    ///
    /// Returns the number of valid message parts in the message that was last parsed.
    ///
    size_t size(void) const;

    ///
    /// Returns the data of a message part.
    /// \param index_ The index of the part to retrieve.
    /// \returns A Message::Field that points to the requested message part's data.
    ///          If index_ is invalid, a 0-length Message::Field pointing to NULL.
    ///
    AMPS::Field getPart(size_t index_) const;
  private:
    typedef std::pair<const char*, size_t> PartLocator;
    typedef std::vector<PartLocator> PartVector;
    PartVector    _parts;
  };

  inline CompositeMessageParser::CompositeMessageParser(void)
  {;}

  template <class T>
  inline size_t
  CompositeMessageParser::parse(const T& message_)
  {
    const char* data;
    size_t length;
    message_.getRawData(&data, &length);
    return parse(data, length);
  }

  inline size_t
  CompositeMessageParser::parse(const char* data_, size_t length_)
  {
    _parts.clear();
    const unsigned char* end = (const unsigned char*) data_ + length_;
    const unsigned char* p = (const unsigned char*) data_;
    while ((p + 4) <= end)
    {
      size_t partLength = p[0];
      partLength = partLength << 8 | p[1];
      partLength = partLength << 8 | p[2];
      partLength = partLength << 8 | p[3];

      p += 4;
      if (p + partLength <= end)
      {
#ifdef AMPS_USE_EMPLACE
        _parts.emplace_back(PartLocator((const char*)(p), partLength));
#else
        _parts.push_back(PartLocator((const char*)(p), partLength));
#endif
      }
      p += partLength;
    }
    return _parts.size();
  }
  inline size_t
  CompositeMessageParser::size(void) const
  {
    return _parts.size();
  }
  inline AMPS::Field
  CompositeMessageParser::getPart(size_t index_) const
  {
    if (index_ < _parts.size())
    {
      const PartLocator& part = _parts[index_];
      return AMPS::Field(part.first, part.second);
    }
    else
    {
      return AMPS::Field();
    }
  }
}
#endif


