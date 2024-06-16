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

#ifndef _BLOCKSTORE_H_
#define _BLOCKSTORE_H_
#include <amps/ampsplusplus.hpp>
#include <amps/Buffer.hpp>
#include <sstream>
#include <string>
#include <map>
#include <amps/ampscrc.hpp>
#if __cplusplus >= 201103L || _MSC_VER >= 1900
#include <atomic>
#endif

#ifdef _WIN32
  #include <intrin.h>
  #include <sys/timeb.h>
#else
  #include <sys/time.h>
  #if AMPS_SSE_42
    #include <cpuid.h>
  #endif
#endif
#include <iostream>

/// \file  BlockStore.hpp
/// \brief Provides AMPS::BlockStore, a class for storing Blocks of a
/// fixed size into a Buffer implementation. This is used as a base class
/// for bookmark and publish stores in the AMPS C++ client.

namespace AMPS
{
///
/// Used as a base class for other stores in the AMPS C++ client, this
/// is an implementation that breaks a provided Buffer into
/// uniform blocks for storing messages and tracks used and unused blocks.
///
  class BlockStore
  {
  public:
    ///
    /// Default constant values for BlockStore
    enum Constants : amps_uint32_t
    {
      DEFAULT_BLOCK_HEADER_SIZE  =   32,
      DEFAULT_BLOCKS_PER_REALLOC = 1000,
      DEFAULT_BLOCK_SIZE         = 2048
    };

    ///
    /// Used as metadata for each block in a Buffer.
    class Block
    {
    public:
      // The offset of the Block's data in the buffer.
      size_t          _offset;
      // The sequence number associated with the Block.
      amps_uint64_t   _sequence;
      // The next Block in the chain when data is in multiple Blocks.
      Block*          _nextInChain;
      // The next Block in list of available or free Blocks.
      Block*          _nextInList;

      // Create Block with given offset
      Block(size_t offset_) : _offset(offset_), _sequence(0)
        , _nextInChain(0), _nextInList(0)
      { ; }

      // Create Block with _nextInList at an address one Block farther
      // than self. Convenient for creating arrays of Blocks.
      Block() : _offset(0), _sequence(0)
        , _nextInChain(0), _nextInList((Block*)(this + 1))
      { ; }

      // Init Block to an offset at index_ * blockSize_
      Block* init(size_t index_, amps_uint32_t blockSize_)
      {
        _offset = index_ * blockSize_;
        return this;
      }

      // Set Block to given offset and return pointer to self
      Block* setOffset(size_t offset_)
      {
        _offset = offset_;
        return this;
      }

    };

  private:
    // Typedefs
    typedef Lock<Mutex> BufferLock;
    typedef Unlock<Mutex> BufferUnlock;
    typedef bool (*ResizeHandler)(size_t, void*);
    typedef std::vector<Block*> BlockList;

  public:
    /// Create a BlockStore using buffer_ and default block size, that grows by
    /// blocksPerRealloc_ blocks when it must grow.
    /// The initialization or recovery from the buffer_ is handled outside of
    /// this class by either calling init() to start new, or by recovering
    /// the Blocks and using setFreeList(), setUsedList(), setEndOfUsedList()
    /// and addBlocks().
    /// \param buffer_ Pointer to an allocated Buffer implementation that
    /// will be used for storage. The store will delete buffer_ when it is
    /// destructed.
    /// \param blocksPerRealloc_ Number of blocks to add to when growing
    /// the size of the Buffer.
    /// \param blockSize_ The size to use for each block in the store.
    /// \param blockHeaderSize_ The size of the header section of each block,
    /// which is also the only memory that needs to be cleared in a block
    /// for it to skipped during recovery and be reusable.
    BlockStore(Buffer* buffer_,
               amps_uint32_t blocksPerRealloc_ = DEFAULT_BLOCKS_PER_REALLOC,
               amps_uint32_t blockHeaderSize_ = DEFAULT_BLOCK_HEADER_SIZE,
               amps_uint32_t blockSize_ = DEFAULT_BLOCK_SIZE)
      : _buffer(buffer_), _freeList(0), _usedList(0)
      , _endOfUsedList(0), _blocksPerRealloc(blocksPerRealloc_)
      , _blockSize(blockSize_), _blockHeaderSize(blockHeaderSize_)
      , _blocksAvailable(0), _resizeHandler(0), _resizeUserData(0)
      , _resizing(false)
    {
    }

    ///
    /// Destructor that cleans up the buffer and other associated memory
    ~BlockStore()
    {
      for (BlockList::iterator i = _blockList.begin();
           i != _blockList.end(); ++i)
      {
        delete[] *i;
      }
      delete _buffer;
    }

    ///
    /// Get the size of each Block, as set in the constructor.
    amps_uint32_t getBlockSize() const
    {
      return _blockSize;
    }

    ///
    /// Get the size of a header within each Block, as set in the constructor.
    amps_uint32_t getBlockHeaderSize() const
    {
      return _blockHeaderSize;
    }

    ///
    /// Acquire the lock for this object. Used by RAII templates. The lock
    /// should be acquired before calling any other functions below and held
    /// while using the Buffer* returned by getBuffer().
    void acquireRead() const
    {
      _lock.acquireRead();
    }

    ///
    /// Release the lock for this object. Used by RAII templates.
    void releaseRead() const
    {
      _lock.releaseRead();
    }

    ///
    /// Signal lock waiters.
    void signalAll()
    {
      _lock.signalAll();
    }

    ///
    /// Wait for a signal.
    void wait()
    {
      _lock.wait();
    }

    ///
    /// Wait timeout_ ms for a signal.
    /// \param timeout_ The maximum time to wait in milliseconds.
    /// \return true for signal received, false for timeout.
    bool wait(long timeout_)
    {
      return _lock.wait(timeout_);
    }

    ///
    /// Set a resize handler that is called with the new total size of the
    /// Buffer. The resize handler can return true to allow the resize or
    /// false to prevent it. If it returns false, some action should have been
    /// taken to create room or it will be called repeatedly in a loop.
    /// \param resizeHandler_ A ResizeHandler function to be invoked for every
    /// resize attempt.
    /// \param userData_ A pointer to any data that should be passed to the
    /// ResizeHandler when invoked.
    void setResizeHandler(ResizeHandler resizeHandler_, void* userData_)
    {
      _resizeHandler = resizeHandler_;
      _resizeUserData = userData_;
    }

    /// Get the first used block in the store.
    /// \return A pointer to the first used block, which may be null if the
    /// store is currently empty.
    // Lock should already be acquired
    Block* front() const
    {
      return _usedList;
    }

    /// Get the last used block in the store.
    /// \return A pointer to the last used block, which may be null if the
    /// store is empty, or the same as front() if there is only one block
    /// in use in the store.
    // Lock should already be acquired
    Block* back() const
    {
      return _endOfUsedList;
    }

    /// Allow containing classes to initialize the free list in recovery.
    /// \param block_ A pointer the first Block of the free list.
    /// \param freeCount_ The number of free blocks in the chain.
    // Lock should already be acquired
    void setFreeList(Block* block_, amps_uint32_t freeCount_)
    {
      _freeList = block_;
      _blocksAvailable = freeCount_;
    }

    /// Allow containing classes to initialize the used list in recovery.
    /// \param block_ A pointer the first Block of the used list.
    // Lock should already be acquired
    void setUsedList(Block* block_)
    {
      _usedList = block_;
    }

    /// Allow containing classes to initialize the used list in recovery.
    /// \param block_ A pointer to the last Block of the used list.
    // Lock should already be acquired
    void setEndOfUsedList(Block* block_)
    {
      _endOfUsedList = block_;
    }

    /// Allow users to create Block arrays during recovery that are
    /// tracked for cleanup here with all other Block arrays.
    /// \param blockArray_ A pointer the beginning of the allocated array.
    // Lock should already be acquired
    void addBlocks(Block* blockArray_)
    {
      _blockList.push_back(blockArray_);
    }

    /// Get the requested number of Blocks as a chain from the free list.
    /// \param numBlocksInChain_ The number of blocks needed.
    /// \return A pointer to the first Block in the chain. Remaining Blocks
    /// are found at nextInChain.
    // Lock should already be acquired
    Block* get(amps_uint32_t numBlocksInChain_)
    {
      // Check that we have enough blocks
      // Do this in a loop since resize can possibly return without resizing
      // and may still leave us needing more space.
      while (_blocksAvailable < numBlocksInChain_)
      {
        // Resize by required multiple of blockPerRealloc
        unsigned int blocksNeeded = numBlocksInChain_ - _blocksAvailable;
        amps_uint32_t addedBlocks = (blocksNeeded / _blocksPerRealloc + 1)
                                    * _blocksPerRealloc;
        size_t size = _buffer->getSize() + (addedBlocks * _blockSize);
        resize(size);
      }
      // Return first free block with others as _nextInChain
      Block* first = 0;
      Block* last = 0;
      Block* next = 0;
      for (unsigned int i = 0; i < numBlocksInChain_; ++i)
      {
        // Take from free list and advance
        next = _freeList;
        _freeList = _freeList->_nextInList;
        next->_nextInList = 0;
        if (!first)
        {
          // First, set it up
          first = next;
          last = next;
        }
        else
        {
          // Not first, add it to chain
          last->_nextInChain = next;
          last = next;
        }
      }
      assert(first);
      // Set _usedList or add it to the end of the used list
      if (!_usedList)
      {
        _usedList = first;
      }
      else
      {
        _endOfUsedList->_nextInList = first;
      }
      _endOfUsedList = first;
      _blocksAvailable -= numBlocksInChain_;
      return first;
    }

    /// Return the given chain of Blocks to the free list for reuse.
    /// Used when returning a Block out of order.
    /// \param block_ The first Block in the chain.
    // Lock should already be acquired
    void put(Block* block_)
    {
      assert(_usedList);
      assert(_endOfUsedList);
      // Remove from used list
      if (_usedList == block_)
      {
        // Easy
        _usedList = _usedList->_nextInList;
        if (!_usedList)
        {
          _endOfUsedList = 0;
        }
      }
      else
      {
        // Search and remove the block
        Block* used = _usedList;
        while (used)
        {
          if (used->_nextInList == block_)
          {
            used->_nextInList = block_->_nextInList;
            break;
          }
          used = used->_nextInList;
          if (!_usedList) // -V1051
          {
            _endOfUsedList = 0;
          }
        }
      }
      // Add to free list
      _flattenToFreeList(block_);
    }

    /// Return all Blocks with sequence <= sequence_ for reuse.
    /// \param sequence_ The highest sequence number to free.
    /// \return The number of items removed.
    // Lock should already be acquired
    AMPS_ATOMIC_BASE_TYPE put(amps_uint64_t sequence_)
    {
      assert(_usedList);
      assert(_endOfUsedList);
      Block* used = _usedList;
      AMPS_ATOMIC_BASE_TYPE removalCount = 0;
      while (used && used->_sequence <= sequence_)
      {
        Block* next = used->_nextInList;
        // Add to free list
        _flattenToFreeList(used);
        used = next;
        ++removalCount;
      }
      _usedList = used;
      if (!used)
      {
        _endOfUsedList = 0;
      }
      return removalCount;
    }

    /// Return all Blocks starting with the given Block to the free list.
    /// Used if corruption was detected or to remove the end of what's stored.
    /// \param block_ The beginning of list of Blocks to return.
    // Lock should already be acquired
    void putAll(Block* block_)
    {
      // Remove from used list
      Block* newEndOfUsedList = 0;
      for (Block* used = _usedList; used; used = used->_nextInList)
      {
        if (used == block_)
        {
          if (newEndOfUsedList)
          {
            newEndOfUsedList->_nextInList = 0;
          }
          else
          {
            _usedList = 0;
          }
          _endOfUsedList = newEndOfUsedList;
        }
        newEndOfUsedList = used;
      }
      // Add all remaining to free list
      Block* next = 0;
      for (Block* block = block_; block; block = next)
      {
        next = block->_nextInList;
        _flattenToFreeList(block);
      }
    }

    /// Initialize, assuming that _buffer has no existing information. Divide
    /// the _buffer in Blocks and put them all into the free list.
    // Lock should already be held
    void init()
    {
      size_t startSize = _buffer->getSize();
      if (!startSize)
      {
        resize(getDefaultResizeSize());
        startSize = _buffer->getSize();
      }
      // How many blocks are we resizing
      amps_uint32_t numBlocks = (amps_uint32_t)(startSize) / getBlockSize();
      _freeList = new Block[numBlocks];
      _blockList.push_back(_freeList);
      for (size_t i = 0; i < numBlocks; ++i)
      {
        _freeList[i].init(i, getBlockSize());
      }
      _freeList[numBlocks - 1]._nextInList = 0;
      _blocksAvailable += numBlocks;
      assert(_freeList);
      assert(_blocksAvailable);
    }

    /// Return the default number of bytes for each resize.
    /// \return The number of bytes by which the store resizes itself.
    size_t getDefaultResizeSize() const
    {
      return _blocksPerRealloc * _blockSize;
    }

    /// Return the default number of blocks for each resize.
    /// \return The number of blocks by which the store resizes itself.
    amps_uint32_t getDefaultResizeBlocks() const
    {
      return _blocksPerRealloc;
    }

    /// Resize the buffer to the requested size, returning all new space.
    /// This is used during recovery when additional space is needed that
    /// isn't added immediately to the internal accounting. It's also used
    /// by the normal resize function, which takes care of the accounting.
    /// \param size_ The new size for the _buffer.
    /// \param pNewBlocks_ A pointer where the number of new blocks created
    /// will be stored for the calling function.
    // Lock should already be held
    Block* resizeBuffer(size_t size_, amps_uint32_t* pNewBlocks_)
    {
      Block* freeList = 0;
      while (_resizing)
      {
        if (_buffer->getSize() >= size_)
        {
          return freeList;
        }
        if (!_lock.wait(1000))
        {
          amps_invoke_waiting_function();
        }
      }
      FlagFlip flip(&_resizing);
      bool okToResize = false;
      if (true)
      {
        BufferUnlock u(_lock);
        // Don't do anything if resizeHandler says no
        okToResize = _canResize(size_);
      }
      if (!okToResize)
      {
        return freeList;
      }
      try
      {
        _lock.signalAll();
        size_t oldSize = _buffer->getSize();
        amps_uint32_t oldBlocks = (amps_uint32_t)(oldSize / getBlockSize());
        if (oldSize >= size_)
        {
          *pNewBlocks_ = 0;
          return freeList;
        }
        _buffer->setSize(size_);
        _buffer->zero(oldSize, size_ - oldSize);
        // How many blocks are we resizing
        *pNewBlocks_ = (amps_uint32_t)((size_ - oldSize) / getBlockSize());
        freeList = new Block[*pNewBlocks_];
        for (size_t i = 0; i < *pNewBlocks_; ++i)
        {
          freeList[i].init(oldBlocks + i, getBlockSize());
        }
        freeList[*pNewBlocks_ - 1]._nextInList = 0;
      }
#ifdef _WIN32
      catch (const std::bad_alloc&)
#else
      catch (const std::bad_alloc& e)
#endif
      {
        std::ostringstream os;
        os << "BlockStore failed to allocate " << size_
           << " bytes for resize of store from " << _buffer->getSize()
           << " bytes.";
        throw StoreException(os.str());
      }
      return freeList;
    }

    /// Resize the buffer to the requested size, adding all new space as
    /// unused Blocks for the free list.
    /// \param size_ The new size for the _buffer.
    // Lock should already be held
    void resize(size_t size_)
    {
      amps_uint32_t newBlocks = 0;
      Block* addedBlockList = resizeBuffer(size_, &newBlocks);
      if (!addedBlockList || !newBlocks)
      {
        // Maybe we didn't have to allocate in this thread
        return;
      }
      _blockList.push_back(addedBlockList);
      addedBlockList[newBlocks - 1]._nextInList = _freeList;
      _freeList = addedBlockList;
      _blocksAvailable += newBlocks;
      assert(_blocksAvailable);
    }

    /// Set the size to use for all Blocks. This should only be called before
    /// any Blocks have been created. Returns old Block size.
    // Lock should be held, no blocks should be used or allocated
    amps_uint32_t setBlockSize(amps_uint32_t blockSize_)
    {
      if (_usedList || _freeList)
      {
        return 0;
      }
      amps_uint32_t oldSize = _blockSize;
      _blockSize = blockSize_;
      return oldSize;
    }

    /// Set the size to use for the header for all Blocks. This should only be
    /// called before any Blocks have been created. Returns the old Block
    /// header size.
    // Lock should be held, no blocks should be used or allocated
    amps_uint32_t setBlockHeaderSize(amps_uint32_t blockHeaderSize_)
    {
      if (_usedList || _freeList)
      {
        return 0;
      }
      amps_uint32_t oldSize = _blockHeaderSize;
      _blockHeaderSize = blockHeaderSize_;
      return oldSize;
    }

    /// Return the buffer underlying the store for direct write/read.
    /// \return The wrapped buffer.
    // Lock should already be held
    Buffer* getBuffer()
    {
      return _buffer;
    }

  private:
    /// Checks if resize is allowed.
    bool _canResize(size_t requestedSize_)
    {
      if (_resizeHandler)
      {
        return _resizeHandler(requestedSize_, _resizeUserData);
      }
      else
      {
        return true;
      }
    }

    // Lock should already be acquired
    void _flattenToFreeList(Block* block_)
    {
      // Flatten chain to front of free list
      Block* current = block_;
      while (current)
      {
        Block* chain = current->_nextInChain;
        // Clear the header
        _buffer->zero(current->_offset, _blockHeaderSize);
        // Prepend to the free list and clear other values
        current->_nextInList = _freeList;
        _freeList = current;
        ++_blocksAvailable;
        current->_sequence = (amps_uint64_t)0;
        current->_nextInChain = 0;
        current = chain;
      }
      assert(_freeList);
      assert(_blocksAvailable);
    }

    // Member variables
    // Buffer to use for storage
    Buffer*         _buffer;

    // The Block accounting
    Block*              _freeList;
    Block*              _usedList;
    Block*              _endOfUsedList;
    // How much to resize buffer when needed
    amps_uint32_t       _blocksPerRealloc;
    // How big is each Block, and what part is header
    amps_uint32_t       _blockSize;
    amps_uint32_t       _blockHeaderSize;
    // How many blocks are free
    amps_uint32_t       _blocksAvailable;
    // ResizeHandler to call before resizing
    ResizeHandler       _resizeHandler;
    // ResizeHandler data
    void*               _resizeUserData;
    // List of every allocated slab of Blocks
    BlockList           _blockList;
    // Flag to control resizing
#if __cplusplus >= 201103L || _MSC_VER >= 1900
    std::atomic<bool>   _resizing;
#else
    volatile bool       _resizing;
#endif

    // Lock for _buffer
    mutable Mutex   _lock;

  };

}

#endif

