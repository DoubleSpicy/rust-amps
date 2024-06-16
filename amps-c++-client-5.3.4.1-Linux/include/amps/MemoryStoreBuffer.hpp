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

#ifndef _MEMORYSTOREBUFFER_H_
#define _MEMORYSTOREBUFFER_H_

#include <amps/ampsplusplus.hpp>
#include <amps/Buffer.hpp>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifdef _WIN32
  #include <windows.h>
#else
  #include <sys/mman.h>
#endif

/// \file MemoryStoreBuffer.hpp
/// \brief Provides AMPS::MemoryStoreBuffer, used by an AMPS::HAClient to
/// store messages in memory.


namespace AMPS
{
///
/// A Buffer implementation that uses memory for storage.
  class MemoryStoreBuffer : public Buffer
  {
  public:
    MemoryStoreBuffer() : _buffer(new char[AMPS_MEMORYBUFFER_DEFAULT_LENGTH])
      , _bufferLen(AMPS_MEMORYBUFFER_DEFAULT_LENGTH), _bufferPos(0)
    {}

    ~MemoryStoreBuffer()
    {
      delete[] _buffer;
    }

    virtual size_t getSize() const
    {
      return _buffer == NULL ? 0 : _bufferLen;
    }

    virtual void setSize(size_t newSize_)
    {
      if (_buffer != NULL && newSize_ > _bufferLen)
      {
        char* _oldBuffer = _buffer;
        _buffer = new char[newSize_];
        memcpy(_buffer, _oldBuffer, _bufferLen);
        delete[] _oldBuffer;
      }
      else if (_buffer == NULL)
      {
        _buffer = new char[newSize_];
      }
      _bufferLen = newSize_;
    }

    virtual size_t getPosition() const
    {
      return _bufferPos;
    }

    virtual void setPosition(size_t position_)
    {
      while (position_ > _bufferLen)
      {
        setSize(_bufferLen * 2);
      }
      _bufferPos = position_;

    }

    virtual void putByte(char byte_)
    {
      _buffer[_bufferPos++] = byte_;
    }

    virtual char getByte()
    {
      return _buffer[_bufferPos++];
    }

    virtual void putLong(long l_)
    {
      *((long*)(_buffer + _bufferPos)) = l_;
      _bufferPos += sizeof(long);
    }

    virtual long getLong()
    {
      _bufferPos += sizeof(long);
      return *((long*)(_buffer + _bufferPos - sizeof(long)));
    }

    virtual void putUnsignedLong(unsigned long l_)
    {
      *((unsigned long*)(_buffer + _bufferPos)) = l_;
      _bufferPos += sizeof(unsigned long);
    }

    virtual unsigned long getUnsignedLong()
    {
      _bufferPos += sizeof(unsigned long);
      return *((unsigned long*)(_buffer + _bufferPos - sizeof(unsigned long)));
    }

    virtual void putSizet(size_t s_)
    {
      *((size_t*)(_buffer + _bufferPos)) = s_;
      _bufferPos += sizeof(size_t);
    }

    virtual size_t getSizet()
    {
      _bufferPos += sizeof(size_t);
      return *((size_t*)(_buffer + _bufferPos - sizeof(size_t)));
    }

    virtual void putInt32(amps_int32_t s_)
    {
      *((amps_int32_t*)(_buffer + _bufferPos)) = s_;
      _bufferPos += sizeof(amps_int32_t);
    }

    virtual amps_int32_t getInt32()
    {
      _bufferPos += sizeof(amps_int32_t);
      return *((amps_int32_t*)(_buffer + _bufferPos - sizeof(amps_int32_t)));
    }

    virtual void putUint32(amps_uint32_t s_)
    {
      *((amps_uint32_t*)(_buffer + _bufferPos)) = s_;
      _bufferPos += sizeof(amps_uint32_t);
    }

    virtual amps_uint32_t getUint32()
    {
      _bufferPos += sizeof(amps_uint32_t);
      return *((amps_uint32_t*)(_buffer + _bufferPos - sizeof(amps_uint32_t)));
    }

    virtual void putInt64(amps_int64_t s_)
    {
      *((amps_int64_t*)(_buffer + _bufferPos)) = s_;
      _bufferPos += sizeof(amps_int64_t);
    }

    virtual amps_int64_t getInt64()
    {
      _bufferPos += sizeof(amps_int64_t);
      return *((amps_int64_t*)(_buffer + _bufferPos - sizeof(amps_int64_t)));
    }

    virtual void putUint64(amps_uint64_t s_)
    {
      *((amps_uint64_t*)(_buffer + _bufferPos)) = s_;
      _bufferPos += sizeof(amps_uint64_t);
    }

    virtual amps_uint64_t getUint64()
    {
      _bufferPos += sizeof(amps_uint64_t);
      return *((amps_uint64_t*)(_buffer + _bufferPos - sizeof(amps_uint64_t)));
    }

    virtual void putBytes(const char* data_, size_t dataLength_)
    {
      memcpy(_buffer + _bufferPos, data_, dataLength_);
      _bufferPos += dataLength_;
    }

    virtual void putBytes(const Buffer::ByteArray& bytes_)
    {
      memcpy(_buffer + _bufferPos, bytes_._data, bytes_._len);
      _bufferPos += bytes_._len;
    }

    virtual Buffer::ByteArray getBytes(size_t numBytes_)
    {
      Buffer::ByteArray retVal(_buffer + _bufferPos, numBytes_, false);
      _bufferPos += numBytes_;
      return retVal;
    }

    // Copy bytes from here into buffer_
    virtual void copyBytes(char* buffer_, size_t numBytes_)
    {
      memcpy(buffer_, _buffer + _bufferPos, numBytes_);
      _bufferPos += numBytes_;
    }

    virtual void zero(size_t offset_, size_t length_)
    {
      memset(_buffer + offset_, 0, length_);
    }

    // Copy bytes from source_ location in the buffer to destination_ in buffer
    virtual void copyBytes(size_t destination_, size_t source_, size_t number_)
    {
      // Could be overlap, should be infrequent, use memmove
      memmove(_buffer + destination_, _buffer + source_, number_);
      _bufferPos += number_;
    }

  protected:
    enum CtorFlag { EMPTY };
    MemoryStoreBuffer(CtorFlag) : _buffer(0), _bufferLen(0), _bufferPos(0)
    {}

    char*  _buffer;
    size_t _bufferLen;
    size_t _bufferPos;
  };

} // end namespace AMPS

#endif //_MEMORYSTOREBUFFER_H_

