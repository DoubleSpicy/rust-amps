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

#ifndef _BUFFER_H_
#define _BUFFER_H_

#include <amps/ampsplusplus.hpp>

/// \file Buffer.hpp
/// \brief Provides AMPS::Buffer, an abstract base class used by the
/// store implementations in the AMPS client.

namespace AMPS
{

/// Abstract base class for implementing a buffer to be used by a StoreImpl
/// for storage of publish messages for possible replay to AMPS.
  class Buffer
  {
  public:
    struct ByteArray
    {
      char*  _data;
      size_t _len;
      // If true, the _data buffer is owned and must be deleted
      bool   _owned;

      ByteArray() : _data(0), _len(0), _owned(false) {}

      ByteArray(char* data_, size_t len_, bool owned_)
        : _data(data_), _len(len_), _owned(owned_) {}

      ByteArray(const ByteArray& rhs_) : _data(rhs_._data), _len(rhs_._len)
        , _owned(rhs_._owned)
      {
        const_cast<ByteArray&>(rhs_)._owned = false;
      }

      ~ByteArray()
      {
        if (_owned)
        {
          delete[] _data;
        }
      }

      ByteArray& operator=(const ByteArray& rhs_)
      {
        _data = rhs_._data;
        _len = rhs_._len;
        _owned = rhs_._owned;
        const_cast<ByteArray&>(rhs_)._owned = false;
        return *this;
      }
    };

    virtual ~Buffer() {}

    ///
    /// Get the current size of the Buffer in bytes.
    /// \return The size in bytes.
    virtual size_t        getSize() const = 0;

    ///
    /// Set the size for the buffer.
    /// \param newSize_ The new size in bytes for the buffer.
    virtual void          setSize(size_t newSize_) = 0;

    ///
    /// Get the current position in the buffer.
    /// \return The position in the buffer.
    virtual size_t        getPosition() const = 0;

    ///
    /// Set the buffer postion to a location.
    /// \param position_ The new buffer position.
    virtual void          setPosition(size_t position_) = 0;

    ///
    /// Put a byte into the buffer at the current position and advance.
    /// \param byte_ The byte to put in the buffer.
    virtual void          putByte(char byte_) = 0;

    ///
    /// Get the next byte from the buffer and advance.
    /// \return The byte at the current position.
    virtual char          getByte() = 0;

    ///
    /// Put an unsigned 32-bit int value into the buffer at the current
    /// position and advance past the end of it.
    /// \param i_ The unsigned 32-bit int value to write to the buffer.
    virtual void          putUint32(amps_uint32_t i_) = 0;

    ///
    /// Get the unsigned 32-bit int value at the current buffer position and
    /// advance past it.
    /// \return The unsigned 32-bit int value.
    virtual amps_uint32_t getUint32() = 0;

    ///
    /// Put a 32-bit int value into the buffer and advance past it.
    /// \param i_ The 32-bit int value.
    virtual void          putInt32(amps_int32_t i_) = 0;

    ///
    /// Get the 32-bit int value at the current buffer position and advance
    /// past it.
    /// \return The 32-bit int value at the current position.
    virtual amps_int32_t  getInt32() = 0;

    ///
    /// Put a size_t value into the buffer at the current position and advance
    /// past it.
    /// \param s_ The size_t value.
    virtual void          putSizet(size_t s_) = 0;

    ///
    /// Get a size_t value at the current buffer position and advance past it.
    /// \return The size_t value.
    virtual size_t        getSizet() = 0;

    ///
    /// Put an amps_uint64_t value into the buffer at the current position and
    /// advance past it.
    /// \param ui_ The amps_uint64_t value.
    virtual void          putUint64(amps_uint64_t ui_) = 0;

    ///
    /// Get an unsigned 64-bit int value at the current buffer position and
    /// advance past it.
    /// \return The amps_uint64_t value.
    virtual amps_uint64_t getUint64() = 0;

    ///
    /// Put an amps_int64_t value into the buffer at the current position and
    /// advance past it.
    /// \param i_ The amps_int64_t value.
    virtual void          putInt64(amps_int64_t i_) = 0;

    ///
    /// Get a 64-bit int value at the current buffer position and
    /// advance past it.
    /// \return The amps_int64_t value.
    virtual amps_int64_t  getInt64() = 0;

    ///
    /// Put the given length of bytes in data into the buffer at the current
    /// position and advance past them.
    /// \param data_ A pointer to the beginning of the bytes to write.
    /// \param dataLength_ The number of bytes from data_ to write.
    virtual void          putBytes(const char* data_, size_t dataLength_) = 0;

    ///
    /// Put the given bytes into the buffer at the current position and advance
    /// past them.
    /// \param bytes_ The bytes to write into the buffer.
    virtual void          putBytes(const ByteArray& bytes_) = 0;

    ///
    /// Get the given number of bytes from the buffer.
    /// \param numBytes_ The number of bytes to get from the buffer.
    /// \return The bytes from the buffer.
    virtual ByteArray  getBytes(size_t numBytes_) = 0;

    ///
    /// Copy the given number of bytes from this buffer to the given buffer.
    /// \param buffer_ The destination for the copied bytes.
    /// \param numBytes_ The number of bytes to get from the buffer.
    virtual void copyBytes(char* buffer_, size_t numBytes_) = 0;

    ///
    /// Set the given number of bytes in the buffer to zero starting at the
    /// given position.
    /// \param offset_ The position at which to start setting bytes to 0.
    /// \param length_ The number of bytes to set to zero.
    virtual void       zero(size_t offset_, size_t length_) = 0;

    ///
    /// Move the given number of bytes at the given location to the new location
    /// Buffer should do this in the most optimal fashion.
    /// \param destination_ The destination offset to copy the bytes to
    /// \param source_ The source offset in the buffer to copy from
    /// \param number_ The number of bytes to move
    virtual void        copyBytes(size_t destination_, size_t source_,
                                  size_t number_) = 0;
  };

} // end namespace AMPS

#endif //_BUFFER_H_

