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

#ifndef _MMAPSTOREBUFFER_H_
#define _MMAPSTOREBUFFER_H_

#include <amps/ampsplusplus.hpp>
#include <amps/MemoryStoreBuffer.hpp>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifdef _WIN32
  #include <windows.h>
#else
  #include <sys/mman.h>
#endif

/// \file MMapStoreBuffer.hpp
/// \brief Provides AMPS::MMapStoreBuffer, an AMPS::Buffer implementation used by the AMPS::MMapBookmarkStore.


namespace AMPS
{
///
/// A Buffer implementation that uses a memory mapped file as its storage.
  class MMapStoreBuffer : public MemoryStoreBuffer
  {
  public:
    ///
    /// Create an MMapStoreBuffer using fileName_ as the name of the memory
    /// mapped file used for storage. If the file exists and contains valid
    /// messages, they will be available for replay.
    /// \param fileName_ The name of the file mapped to memory.
    MMapStoreBuffer(const std::string& fileName_)
      : MemoryStoreBuffer(MemoryStoreBuffer::EMPTY)
#ifdef _WIN32
      , _mapFile(INVALID_HANDLE_VALUE), _file(INVALID_HANDLE_VALUE)
    {
      _file = CreateFileA(fileName_.c_str(), GENERIC_READ | GENERIC_WRITE, 0,
                          NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
      if ( _file == INVALID_HANDLE_VALUE )
      {
        std::ostringstream os;
        os << "Failed to create file " << fileName_ << " for MMapStoreBuffer";
        error(os.str());
      }
      LARGE_INTEGER liFileSize;
      if (GetFileSizeEx(_file, &liFileSize) == 0)
      {
        std::ostringstream os;
        os << "Failure getting file size of " << fileName_ << " for MMapStoreBuffer";
        error(os.str());
      }
#ifdef _WIN64
      size_t fileSize = liFileSize.QuadPart;
#else
      size_t fileSize = liFileSize.LowPart;
#endif
      _setSize( AMPS_MEMORYBUFFER_DEFAULT_LENGTH > fileSize ?
                AMPS_MEMORYBUFFER_DEFAULT_LENGTH : fileSize);
#else
    {
      _fd = ::open(fileName_.c_str(), O_CREAT | O_RDWR, (mode_t)0644);
      struct stat statBuf;
      memset(&statBuf, 0, sizeof(statBuf));
      if (fstat(_fd, &statBuf) == -1)
      {
        std::ostringstream os;
        os << "Failed to stat file " << fileName_ << " for MMapStoreBuffer";
        error(os.str());
      }
      _bufferLen = (size_t)statBuf.st_size;
      _setSize(statBuf.st_size > (ssize_t)AMPS_MEMORYBUFFER_DEFAULT_LENGTH
               ? (size_t)statBuf.st_size : AMPS_MEMORYBUFFER_DEFAULT_LENGTH);
#endif
    }

    ~MMapStoreBuffer()
    {
      if (_buffer)
      {
        close();
      }
    }

    void close()
    {
      sync();
#ifdef _WIN32
      FlushFileBuffers(_file);
      UnmapViewOfFile(_buffer);
      CloseHandle(_mapFile);
      CloseHandle(_file);
      _mapFile = INVALID_HANDLE_VALUE;
      _file = INVALID_HANDLE_VALUE;
#else
      munmap(_buffer, _bufferLen);
      ::close(_fd);
      _fd = 0;
#endif
      _buffer = NULL;
      _bufferLen = 0;
    }

    void sync()
    {
      if (_buffer != NULL)
      {
#ifdef _WIN32
        if (!FlushViewOfFile(_buffer, _bufferPos))
#else
        if (msync(_buffer, _bufferPos, MS_ASYNC) != 0)
#endif
        {
          std::ostringstream os;
          os << "Failed to sync mapped memory; buffer: " << (size_t)_buffer
             << " pos: " << _bufferPos;
          error(os.str());
        }
      }
    }

    virtual void setSize(size_t newSize_)
    {
      _setSize(newSize_);
    }

    void _setSize(size_t newSize_)
    {
      if (_bufferLen > 0)
      {
        sync();
      }
      // Make sure we're using a multiple of page size
      size_t sz = newSize_ & (size_t)(~(getPageSize() - 1));
      if (sz < newSize_)
      {
        sz += getPageSize();
      }
#ifdef _WIN32
      if (_mapFile != INVALID_HANDLE_VALUE && _mapFile != NULL)
      {
        FlushFileBuffers(_file);
        UnmapViewOfFile(_buffer);
        CloseHandle(_mapFile);
      }
#ifdef _WIN64
      _mapFile = CreateFileMapping( _file, NULL, PAGE_READWRITE, (DWORD)((sz >> 32) & 0xffffffff), (DWORD)sz, NULL);
#else
      _mapFile = CreateFileMapping( _file, NULL, PAGE_READWRITE, 0, (DWORD)sz, NULL);
#endif
      if (_mapFile == INVALID_HANDLE_VALUE || _mapFile == NULL)
      {
        _buffer = 0;
        _bufferLen = 0;
        error("Failed to create map of log file");
      }
      else
      {
        _buffer = (char*)MapViewOfFile(_mapFile, FILE_MAP_ALL_ACCESS, 0, 0, sz);
        if (_buffer == NULL)
        {
          std::ostringstream os;
          os << "Failed to map log file to memory; buffer: " << (size_t)_buffer << " length: " << sz << " previous size: " << _bufferLen;
          _buffer = 0;
          _bufferLen = 0;
          error(os.str());
        }
      }
#else
      // If not at current size, extend the underlying file
      if (sz > _bufferLen)
      {
        if (lseek(_fd, (off_t)sz - 1, SEEK_SET) == -1)
        {
          std::ostringstream os;
          os << "Seek failed for buffer extension; buffer: " << (size_t)_buffer
             << " length: " << _bufferLen << " pos: " << _bufferPos
             << " requested new size: " << newSize_;
          error(os.str());
        }
        if (::write(_fd, "", 1) == -1)
        {
          std::ostringstream os;
          os << "Failed to grow buffer; buffer: " << (size_t)_buffer << " length: "
             << _bufferLen << " pos: " << _bufferPos << " requested new size: "
             << newSize_;
          error(os.str());
        }
      }

      void* result = NULL;
      if (_buffer == NULL)
      {
        result = mmap(0, sz, PROT_WRITE | PROT_READ, MAP_SHARED, _fd, 0);
      }
      else if (_bufferLen < sz)
      {
#if defined(linux)
        result = mremap(_buffer, _bufferLen, sz, MREMAP_MAYMOVE);
#else
        munmap(_buffer, _bufferLen);
        result = mmap(0, sz, PROT_WRITE | PROT_READ, MAP_SHARED, _fd, 0);
#endif
      }
      if (result == MAP_FAILED || result == NULL)
      {
        std::ostringstream os;
        os << "Failed to map log file to memory; buffer: "
           << (size_t)_buffer << " length: " << sz
           << " previous size: " << _bufferLen;
        _buffer = 0;
        _bufferLen = 0;
        error(os.str());
      }
      _buffer = (char*)result;
#endif
      if (_buffer)
      {
        _bufferLen = sz;
      }
    }

  private:
    void error(const std::string & message)
    {
      std::ostringstream os;
#ifdef _WIN32
      const size_t errorBufferSize = 1024;
      char errorBuffer[errorBufferSize];
      memset(errorBuffer, 0, errorBufferSize);
      strerror_s(errorBuffer, errorBufferSize, errno);
      os << message << ". Error is " << errorBuffer;
#else
      os << message << ". Error is " << strerror(errno);
#endif
      throw StoreException(os.str());
    }

#ifdef _WIN32
    HANDLE _mapFile;
    HANDLE _file;
#else
    int    _fd;
#endif
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

#endif //_MMAPSTOREBUFFER_H_

