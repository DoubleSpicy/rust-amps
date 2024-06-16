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

#ifndef _PUBLISHSTORE_H_
#define _PUBLISHSTORE_H_

#include <amps/ampsplusplus.hpp>
#include <amps/BlockPublishStore.hpp>
#include <amps/MMapStoreBuffer.hpp>
#ifndef _WIN32
  #include <unistd.h>
#endif

/// \file PublishStore.hpp
/// \brief Provides AMPS::PublishStore, a publish store that uses memory-mapped
/// files to provide a publish store that persists across application
/// restarts.

namespace AMPS
{
///
/// A StoreImpl implementation that uses a memory-mapped file to provide
/// a publish store that persists across application restarts.
  class PublishStore : public BlockPublishStore
  {
  public:
    ///
    /// Create a PublishStore that uses fileName_ for the storage. If the
    /// file exists and has valid messages, they will be recovered.
    /// \param fileName_ The name of the file to use for mmap storage.
    /// \param errorOnPublishGap_ If true, PublishStoreGapException can be
    /// thrown by the store if the client logs onto a server that appears
    /// to be missing messages no longer held in the store.
    PublishStore(const std::string& fileName_, bool errorOnPublishGap_ = false)
      : BlockPublishStore(new MMapStoreBuffer(fileName_), 1000,
                          true, errorOnPublishGap_)
      , _fileName(fileName_)
      , _initialBlocks(1000)
      , _truncateOnClose(false)
    {
      recover();
    }

    ///
    /// Create a PublishStore that uses fileName_ for the storage. If the
    /// file exists and has valid messages, they will be recovered.
    /// \param fileName_ The name of the file to use for mmap storage.
    /// \param blocksPerRealloc_ The number of new blocks to create each resize.
    /// \param errorOnPublishGap_ If true, PublishStoreGapException can be
    /// thrown by the store if the client logs onto a server that appears
    /// to be missing messages no longer held in the store.
    PublishStore(const std::string& fileName_, size_t blocksPerRealloc_,
                 bool errorOnPublishGap_ = false)
      : BlockPublishStore(new MMapStoreBuffer(fileName_),
                          (amps_uint32_t)blocksPerRealloc_,
                          true, errorOnPublishGap_)
      , _fileName(fileName_)
      , _initialBlocks((int)blocksPerRealloc_)
      , _truncateOnClose(false)
    {
      recover();
    }

    ~PublishStore()
    {
      close();
    }

    ///
    /// Tell the PublishStore if it should return the file to its
    /// initial capacity when the store is closed if there are no
    /// messages stored in it.
    /// \param truncate_ If true, the file will be truncated.
    void truncateOnClose(bool truncate_)
    {
      _truncateOnClose = truncate_;
    }

    ///
    /// Close the PublishStore and associated file.
    void close()
    {
      if (!_blockStore.getBuffer())
      {
        return;
      }
      amps_uint64_t unpersisted = unpersistedCount();
      BufferLock guard(_blockStore);
      reinterpret_cast<MMapStoreBuffer*>(_blockStore.getBuffer())->close();
      if (_truncateOnClose && unpersisted == 0)
      {
#ifdef _WIN32
        size_t retries = 0;
        HANDLE file = INVALID_HANDLE_VALUE;
        while ( file == INVALID_HANDLE_VALUE && retries++ < 5)
        {
          file = CreateFileA(_fileName.c_str(),
                             GENERIC_READ | GENERIC_WRITE, 0, NULL,
                             OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
        }
        // Ignore failure, since this could be in destructor
        if ( file != INVALID_HANDLE_VALUE )
        {
          SetFilePointer(file, (LONG)(_initialBlocks * _blockStore.getBlockSize()),
                         NULL, FILE_BEGIN);
          SetEndOfFile(file);
          CloseHandle(file);
        }
#else
        if (truncate(_fileName.c_str(),
                     (off_t)_initialBlocks * (off_t)_blockStore.getBlockSize()) == -1)
        {
          // Ignore failure, since this could be in destructor
          ;
        }
#endif
      }
    }

    ///
    /// Force the PublishStore to sync to disk.
    void sync()
    {
      BufferLock guard(_blockStore);
      reinterpret_cast<MMapStoreBuffer*>(_blockStore.getBuffer())->sync();
    }
  private:
    std::string _fileName;
    int _initialBlocks;
    bool _truncateOnClose;
  };

} //namespace AMPS

#endif //_PUBLISHSTORE_H_

