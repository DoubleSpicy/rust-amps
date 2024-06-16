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

#ifndef _BLOCKPUBLISHSTORE_H_
#define _BLOCKPUBLISHSTORE_H_
#include <amps/ampsplusplus.hpp>
#include <amps/BlockStore.hpp>
#include <amps/Buffer.hpp>
#include <sstream>
#include <stack>
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

/// \file  BlockPublishStore.hpp
/// \brief Provides AMPS::BlockPublishStore, a concrete implementation of a
/// store that breaks the allocated storage into blocks. Used as a base class
/// for other stores in the AMPS C++ client.

namespace AMPS
{
///
/// Used as a base class for other stores in the AMPS C++ client, this
/// is an implementation of StoreImpl that breaks a provided Buffer into
/// uniform blocks for storing messages and tracks used and unused blocks.
///
  class BlockPublishStore : public StoreImpl
  {
  public:
    typedef BlockStore::Block Block;
    typedef Lock<BlockStore> BufferLock;
    typedef Unlock<BlockStore> BufferUnlock;

    typedef enum
    {
      SOW_DELETE_DATA = 0x01,
      SOW_DELETE_FILTER = 0x02,
      SOW_DELETE_KEYS = 0x04,
      SOW_DELETE_BOOKMARK = 0x08,
      SOW_DELETE_BOOKMARK_CANCEL = 0x10,
      SOW_DELETE_UNKNOWN = 0x80
    } SowDeleteType;

    ///
    /// Default constant values for BlockPublishStore
    enum Constants : amps_uint32_t
    {
      DEFAULT_BLOCK_HEADER_SIZE       =   32,
      DEFAULT_BLOCK_CHAIN_HEADER_SIZE =   64,
      DEFAULT_BLOCKS_PER_REALLOC      = 1000,
      DEFAULT_BLOCK_SIZE              = 2048
    };

    /**********************************************************************
     * Storage format
     *************************************************************************
     * Field Description                                | Type     | # Bytes
     *************************************************************************
     * HEADER as detailed below                         |          | 32 TOTAL
     *                                                  |          |
     * Total number of blocks used by the record        | uint32_t | 4
     * Total length of the saved record                 | uint32_t | 4
     * HA Message sequence                              | uint64_t | 8
     * CRC Value - only set in first Block              | uint64_t | 8
     * next in chain offset                             | uint64_t | 8
     *************************************************************************
     * CHAIN HEADER as detailed below                   |          | 64 TOTAL
     *                                                  |          |
     * operation                                        | uint32_t | 4
     * command id length                                | uint32_t | 4
     * correltation id length                           | uint32_t | 4
     * expiration length                                | uint32_t | 4
     * sow key length                                   | uint32_t | 4
     * topic length                                     | uint32_t | 4
     * sow delete flag                                  | int32_t  | 4
     * ack types                                        | uint32_t | 4
     * unused [8]                                       | uint32_t | 4*8 = 32
     *************************************************************************
     * DATA SECTION - can be spread across multiple blocks
     *
     * command id                                       | char[]
     * correlation id                                   | char[]
     * expiration                                       | char[]
     * sow key                                          | char[]
     * topic                                            | char[]
     * data                                             | char[]
     *************************************************************************/

    struct BlockHeader
    {
      amps_uint32_t _blocksToWrite;
      amps_uint32_t _totalRemaining;
      amps_uint64_t _seq;
      amps_uint64_t _crcVal;
      amps_uint64_t _nextInChain;
    };

    struct BlockChainHeader
    {
      amps_uint32_t _operation;
      amps_uint32_t _commandIdLen;
      amps_uint32_t _correlationIdLen;
      amps_uint32_t _expirationLen;
      amps_uint32_t _sowKeyLen;
      amps_uint32_t _topicLen;
      amps_int32_t  _flag;
      amps_uint32_t _ackTypes;
      amps_uint32_t _unused[8];
      BlockChainHeader() // -V730
        : _operation(0), _commandIdLen(0), _correlationIdLen(0)
        , _expirationLen(0), _sowKeyLen(0), _topicLen(0), _flag(-1)
        , _ackTypes(0)
      { ; }
    };

    ///
    /// Block header is number of blocks, total length, sequence number,
    /// crc, next in chain offset
    /// \return The size of Block header
    static inline amps_uint32_t getBlockHeaderSize()
    {
      return DEFAULT_BLOCK_HEADER_SIZE;
    }

    ///
    /// Block chain header is operation, command id length, correlation id
    /// length, expiration length, sow key length, topic length, sow delete
    /// flag, ack types
    /// \return The size of Block chain header
    static inline amps_uint32_t getBlockChainHeaderSize()
    {
      return DEFAULT_BLOCK_CHAIN_HEADER_SIZE;
    }

    ///
    /// Return the size left in a block for data when it has a header in it
    /// \return The amount of room left in a block for data
    inline amps_uint32_t getBlockSize()
    {
      return _blockStore.getBlockSize();
    }

    ///
    /// Return the size left in a block for data when it has a header in it
    /// \return The amount of room left in a block for data
    inline amps_uint32_t getBlockDataSize()
    {
      return _blockStore.getBlockSize() - getBlockHeaderSize();
    }

    /// Create a BlockPublishStore using buffer_, that grows by
    /// blocksPerRealloc_ blocks when it must grow. If isFile_ is true,
    /// it will use CRCs for all storage and retrieval.
    /// \param buffer_ Pointer to an allocated Buffer implementation that
    /// will be used for storage. The store will delete buffer_ when it is
    /// destructed.
    /// \param blocksPerRealloc_ Number of blocks to add to when growing
    /// the size of the Buffer.
    /// \param isFile_ Whether or not the buffer is a recoverable file.
    /// If false, the buffer is intialized as if empty, otherwise recovery
    /// remains possible and a CRC will be used for all stored items.
    /// \param errorOnPublishGap_ If true, PublishStoreGapException can be
    /// thrown by the store if the client logs onto a server that appears
    /// to be missing messages no longer held in the store.
    /// \param blockSize_ Size in bytes for each Block
    BlockPublishStore(Buffer* buffer_,
                      amps_uint32_t blocksPerRealloc_ = 1000,
                      bool isFile_ = false,
                      bool errorOnPublishGap_ = false,
                      amps_uint32_t blockSize_ = DEFAULT_BLOCK_SIZE)
      : StoreImpl(errorOnPublishGap_)
      , _blockStore(buffer_, blocksPerRealloc_,
                    DEFAULT_BLOCK_HEADER_SIZE,
                    (blockSize_ > DEFAULT_BLOCK_HEADER_SIZE * 2
                     ? blockSize_ : DEFAULT_BLOCK_HEADER_SIZE * 2))
      , _metadataBlock(0)
      , _maxDiscarded((amps_uint64_t)0), _lastSequence((amps_uint64_t)1)
      , _stored(0)
    {
      _blockStore.setResizeHandler(&BlockPublishStore::canResize, (void*)this);
      chooseCRC(isFile_);
      if (!isFile_)
      {
        // This gets set in recover in file-based stores
        BufferLock bufferGuard(_blockStore);
        _blockStore.init();
        _metadataBlock = _blockStore.get(1);
        // Remove metadata block from used list
        _blockStore.setUsedList(0);
        _blockStore.setEndOfUsedList(0);
        // Metadata block holds block size, block header size,
        // last discarded sequence, client version
        _metadataBlock->_sequence = (amps_uint64_t)0;
        Buffer* pBuffer = _blockStore.getBuffer();
        pBuffer->setPosition(_metadataBlock->_offset);
        pBuffer->putUint32((amps_uint32_t)getBlockSize());
        pBuffer->putUint32((amps_uint32_t)getBlockHeaderSize());
        pBuffer->putUint64((amps_uint64_t)0);
        // Metadata blocks puts client version in CRC position
        pBuffer->putUint64((amps_uint64_t)VersionInfo::parseVersion(AMPS_CLIENT_VERSION));
        // No next in chain
        pBuffer->putUint64((amps_uint64_t)0);
      }
    }

    ///
    /// Destructor that cleans up the buffer and other associated memory
    virtual ~BlockPublishStore()
    {
    }

    ///
    /// Store a given message that will be delivered to AMPS.
    /// This method will also assign a sequence number to the Message.
    /// This method is primarily used internally by the Client to store
    /// outgoing messages.
    /// \param message_ The Message to put into the store.
    virtual amps_uint64_t store(const Message& message_)
    {
      return store(message_, true);
    }

    ///
    /// Store a given message that will be delivered to AMPS.
    /// This method will also assign a sequence number to the Message if
    /// assignSequence_ is true.
    /// HybridPublishStore uses this method with false when moving messages
    /// from memory to file.
    /// \param message_ The Message to put into the store.
    /// \param assignSequence_ If true, message_ will be updated with the
    /// next sequence number from the store.
    /// \return The sequence number of the message_.
    amps_uint64_t store(const Message& message_, bool assignSequence_)
    {
      const char* commandId, *correlationId, *expiration, *sowKey,
            *topic, *data;
      size_t dataLen = 0;
      BlockHeader blockHeader;
      BlockChainHeader chainHeader;
      message_.getRawCommandId(&commandId, &dataLen);
      chainHeader._commandIdLen = (amps_uint32_t)dataLen;
      message_.getRawCorrelationId(&correlationId, &dataLen);
      chainHeader._correlationIdLen = (amps_uint32_t)dataLen;
      message_.getRawExpiration(&expiration, &dataLen);
      chainHeader._expirationLen = (amps_uint32_t)dataLen;
      message_.getRawSowKey(&sowKey, &dataLen);
      chainHeader._sowKeyLen = (amps_uint32_t)dataLen;
      message_.getRawTopic(&topic, &dataLen);
      chainHeader._topicLen = (amps_uint32_t)dataLen;
      message_.getRawData(&data, &dataLen);
      chainHeader._flag = -1;
      Message::Command::Type operation = message_.getCommandEnum();
      chainHeader._operation = (amps_uint32_t)operation;
      if (operation == Message::Command::SOWDelete)
      {
        if (dataLen > 0)
        {
          chainHeader._flag = SOW_DELETE_DATA;
        }
        else
        {
          message_.getRawFilter(&data, &dataLen);
          if (dataLen > 0)
          {
            chainHeader._flag = SOW_DELETE_FILTER;
          }
          else
          {
            message_.getRawSowKeys(&data, &dataLen);
            if (dataLen > 0)
            {
              chainHeader._flag = SOW_DELETE_KEYS;
            }
            else
            {
              message_.getRawBookmark(&data, &dataLen);
              chainHeader._flag = SOW_DELETE_BOOKMARK;
              // Check options for cancel
              Message::Field options = message_.getOptions();
              size_t remaining = options.len();
              const void* next = NULL;
              const void* start = (const void*)(options.data());
              // Not necessarily null-terminated so no strstr
              while (remaining >= 6 &&
                     (next = memchr(start, (int)'c', remaining)) != NULL)
              {
                remaining = (size_t)next - (size_t)start;
                if (remaining >= 6 && strncmp((const char*)start,
                                              "cancel", 6) == 0)
                {
                  chainHeader._flag = SOW_DELETE_BOOKMARK_CANCEL;
                  break;
                }
              }
            }
          }
        }
      }
      blockHeader._totalRemaining = (
                                      (chainHeader._operation == Message::Command::Unknown)
                                      ? 0
                                      : (getBlockChainHeaderSize()
                                         + chainHeader._commandIdLen
                                         + chainHeader._correlationIdLen
                                         + chainHeader._expirationLen
                                         + chainHeader._sowKeyLen
                                         + chainHeader._topicLen
                                         + (amps_uint32_t)dataLen));
      size_t lastBlockLength = ((operation == Message::Command::Unknown) ? 0 :
                                (blockHeader._totalRemaining % getBlockDataSize()));
      blockHeader._blocksToWrite = ((operation == Message::Command::Unknown)
                                    ? 1
                                    : ((amps_uint32_t)(blockHeader._totalRemaining
                                                       / getBlockDataSize())
                                       + ((lastBlockLength > 0) ? 1 : 0)));
      blockHeader._crcVal = (amps_uint64_t)0ULL;
      blockHeader._crcVal = _crc(commandId,
                                 chainHeader._commandIdLen,
                                 blockHeader._crcVal);
      blockHeader._crcVal = _crc(correlationId,
                                 chainHeader._correlationIdLen,
                                 blockHeader._crcVal);
      blockHeader._crcVal = _crc(expiration,
                                 chainHeader._expirationLen,
                                 blockHeader._crcVal);
      blockHeader._crcVal = _crc(sowKey,
                                 chainHeader._sowKeyLen,
                                 blockHeader._crcVal);
      blockHeader._crcVal = _crc(topic,
                                 chainHeader._topicLen,
                                 blockHeader._crcVal);
      blockHeader._crcVal = _crc(data, dataLen, blockHeader._crcVal);

      // Reserve slots for storage, growing if necessary
      BufferLock bufferGuard(_blockStore);
      Block* first = _blockStore.get(blockHeader._blocksToWrite);
      if (assignSequence_)
      {
        if (_lastSequence <= 2)
        {
          _getLastPersisted();
        }
        blockHeader._seq = ++_lastSequence;
      }
      else
      {
        blockHeader._seq = amps_message_get_field_uint64(
                             message_.getMessage(),
                             AMPS_Sequence);
        if (!_maxDiscarded)
        {
          _maxDiscarded = blockHeader._seq - 1;
        }
        if (blockHeader._seq >= _lastSequence)
        {
          _lastSequence = blockHeader._seq;
        }
      }

      try
      {
        size_t topicWritten = 0UL;
        size_t dataWritten = 0UL;
        size_t commandWritten = 0UL;
        size_t correlationWritten = 0UL;
        size_t expirationWritten = 0UL;
        size_t sowKeyWritten = 0UL;
        Buffer* pBuffer = _blockStore.getBuffer();
        for (Block* next = first; next; next = next->_nextInChain)
        {
          next->_sequence = blockHeader._seq;
          if (next->_nextInChain)
          {
            blockHeader._nextInChain = next->_nextInChain->_offset;
          }
          else
          {
            blockHeader._nextInChain = (amps_uint64_t)0;
          }
          // Set buffer to start of Block and write the header
          pBuffer->setPosition(next->_offset);
          pBuffer->putBytes((const char*)&blockHeader, sizeof(BlockHeader));
          // Clear crcVal, as it's only written in the first Block
          blockHeader._crcVal = (amps_uint64_t)0;
          size_t bytesRemaining = getBlockDataSize();
          if (next == first)
          {
            // Write Block chain header
            chainHeader._ackTypes = (amps_uint32_t)message_.getAckTypeEnum();
            pBuffer->putBytes((const char*)&chainHeader,
                              sizeof(BlockChainHeader));
            pBuffer->setPosition(next->_offset + getBlockHeaderSize() + getBlockChainHeaderSize());
            bytesRemaining -= getBlockChainHeaderSize();
          }
          else
          {
            pBuffer->setPosition(next->_offset + getBlockHeaderSize());
          }

          if (commandWritten < chainHeader._commandIdLen)
          {
            size_t commandWrite = (chainHeader._commandIdLen - commandWritten < bytesRemaining) ? chainHeader._commandIdLen - commandWritten : bytesRemaining;
            pBuffer->putBytes(commandId + commandWritten,
                              commandWrite);
            bytesRemaining -= commandWrite;
            commandWritten += commandWrite;
          }
          if (correlationWritten < chainHeader._correlationIdLen)
          {
            size_t correlationWrite = (chainHeader._correlationIdLen - correlationWritten < bytesRemaining) ? chainHeader._correlationIdLen - correlationWritten : bytesRemaining;
            pBuffer->putBytes(correlationId + correlationWritten,
                              correlationWrite);
            bytesRemaining -= correlationWrite;
            correlationWritten += correlationWrite;
          }
          if (bytesRemaining > 0 && expirationWritten < chainHeader._expirationLen)
          {
            size_t expWrite = (chainHeader._expirationLen - expirationWritten < bytesRemaining) ?  chainHeader._expirationLen - expirationWritten : bytesRemaining;
            pBuffer->putBytes(expiration + expirationWritten, expWrite);
            bytesRemaining -= expWrite;
            expirationWritten += expWrite;
          }
          if (bytesRemaining > 0 && sowKeyWritten < chainHeader._sowKeyLen)
          {
            size_t sowKeyWrite = (chainHeader._sowKeyLen - sowKeyWritten < bytesRemaining) ? chainHeader._sowKeyLen - sowKeyWritten : bytesRemaining;
            pBuffer->putBytes(sowKey + sowKeyWritten, sowKeyWrite);
            bytesRemaining -= sowKeyWrite;
            sowKeyWritten += sowKeyWrite;
          }
          if (bytesRemaining > 0 && topicWritten < chainHeader._topicLen)
          {
            size_t topicWrite = (chainHeader._topicLen - topicWritten
                                 < bytesRemaining)
                                ? chainHeader._topicLen - topicWritten
                                : bytesRemaining;
            pBuffer->putBytes(topic + topicWritten, topicWrite);
            bytesRemaining -= topicWrite;
            topicWritten += topicWrite;
          }
          if (bytesRemaining > 0 && dataWritten < dataLen)
          {
            size_t dataWrite = (dataLen - dataWritten < bytesRemaining) ?
                               dataLen - dataWritten : bytesRemaining;
            pBuffer->putBytes(data + dataWritten, dataWrite);
            bytesRemaining -= dataWrite;
            dataWritten += dataWrite;
          }
        }
      }
      catch (const AMPSException&)
      {
        _blockStore.put(first);
        throw;
      }
      AMPS_FETCH_ADD(&_stored, 1);
      return blockHeader._seq;
    }

    /// Remove all messages with an index up to and including index_.
    /// This method is used internally by the Client to remove messages once
    /// they have been acknowledged by AMPS as stored on the server side.
    /// \param index_ The highest index to remove.
    /// \throw PublishStoreGapException If index_ < getLastPersisted() which could
    /// leave a gap on the server of missing messages from this Client.
    virtual void discardUpTo(amps_uint64_t index_)
    {
      // Get the lock
      BufferLock bufferGuard(_blockStore);
      Buffer* pBuffer = _blockStore.getBuffer();
      // Don't use _getLastPersisted() here, don't want to set it
      // to something other than index_ if it's not already set
      amps_uint64_t lastPersisted = _metadataBlock->_sequence;
      // Make sure it's a real index and we have messages to discard
      if (index_ == (amps_uint64_t)0 || !_blockStore.front() || index_ <= _maxDiscarded)
      {
        // During logon it's very possible we don't have a last persisted
        // but that the Client is calling discardUpTo with the ack value.
        if (lastPersisted < index_)
        {
          pBuffer->setPosition(_metadataBlock->_offset + 8);
          pBuffer->putUint64(index_);
          _metadataBlock->_sequence = index_;
          if (_maxDiscarded < index_)
          {
            _maxDiscarded = index_;
          }
          if (_lastSequence <= index_)
          {
            _lastSequence = index_;
          }
        }
        else if (!index_) // Fresh logon, no sequence history
        {
          _getLastPersisted();
        }
        else if (getErrorOnPublishGap() && index_ < lastPersisted) //Message gap
        {
          std::ostringstream os;
          os << "Server last saw " << index_ << " from Client but Store "
             << "has already discarded up to " << lastPersisted
             << " which would leave a gap of unreceived messages.";
          throw PublishStoreGapException(os.str());
        }
        _blockStore.signalAll();
        return;
      }

      _maxDiscarded = index_;
      AMPS_FETCH_SUB(&_stored, _blockStore.put(index_));
      _blockStore.signalAll();
      if (lastPersisted >= index_)
      {
        return;
      }
      pBuffer->setPosition(_metadataBlock->_offset + 8);
      pBuffer->putUint64(index_);
      _metadataBlock->_sequence = index_;
      if (_lastSequence < index_)
      {
        _lastSequence = index_;
      }
    }

    /// Replay all messages in the Store onto the given StoreReplayer.
    /// This is used internally by the Client to replay any messages to the
    /// server after a successful connection or reconnection to AMPS.
    /// \param replayer_ The StoreReplayer that will transmit the message.
    void replay(StoreReplayer& replayer_)
    {
      // Get the lock
      BufferLock bufferGuard(_blockStore);
      // If we don't have anything yet, return
      if (!_blockStore.front())
      {
        return;
      }
      Block* next = _blockStore.front();
      try
      {
        for (Block* block = _blockStore.front(); block; block = next)
        {
          // Replay the message
          replayOnto(block, replayer_);
          next = block->_nextInList;
        }
      }
      catch (const StoreException&)
      {
        _blockStore.putAll(next);
        throw;
      }
    }

    /// Replay one message in the Store onto the given StoreReplayer.
    /// This is used internally by replay to replay each message.
    /// \param replayer_ The StoreReplayer that will transmit the message.
    /// \param index_ The index of the message to replay.
    /// \return Returns true for success, false for failure such as an
    /// invalid index.
    bool replaySingle(StoreReplayer& replayer_, amps_uint64_t index_)
    {
      BufferLock bufferGuard(_blockStore);
      // If we don't have anything yet, return
      if (!_blockStore.front())
      {
        return false;
      }
      // Get the end point
      amps_uint64_t lastIdx = _blockStore.back()->_sequence;
      // Get the start point
      amps_uint64_t leastIdx = _blockStore.front()->_sequence;
      if (index_ >= leastIdx && index_ <= lastIdx)
      {
        Block* block = _blockStore.front();
        while (block && block->_sequence != index_)
        {
          block = block->_nextInList;
        }
        if (!block)
        {
          return false;
        }
        // If total bytes is 0, it's a queue ack and gets skipped.
        Buffer* pBuffer = _blockStore.getBuffer();
        pBuffer->setPosition(block->_offset +
                             sizeof(amps_uint32_t));
        if (pBuffer->getUint32() == 0)
        {
          return false;
        }
        replayOnto(block, replayer_);
        return true;
      }
      else // Get Store and Client back in sync
      {
        _message.reset();
        leastIdx -= 1;
        _message.setSequence(leastIdx);
        replayer_.execute(_message);
        return false;
      }
    }

    /// Method to return the count of messages that currently in the Store
    /// because they have not been discarded, presumably because AMPS has not
    /// yet acknowledged them. This assumes that there are no gaps in sequence
    /// numbers for better performance.
    /// \return The count of messages in the store.
    size_t unpersistedCount() const
    {
      size_t count = (size_t)_stored;
      return count;
    }

    ///
    /// Method to wait for the Store to discard everything that has been
    /// stored up to the point in time when flush is called. It will get
    /// the current max and wait up to timeout for that message to be discarded
    /// \param timeout_ The number of milliseconds to wait.
    /// \throw DisconnectedException The Client is no longer connected to a server.
    /// \throw ConnectionException An error occurred while sending the message.
    /// \throw TimedOutException The publish command was not acked in the allowed time.
    virtual void flush(long timeout_)
    {
      BufferLock bufferGuard(_blockStore);
      amps_uint64_t waitFor = _getHighestUnpersisted();
      // Check that we aren't already empty
      if (waitFor == getUnsetSequence())
      {
        return;
      }
      if (timeout_ > 0)
      {
        bool timedOut = false;
        AMPS_START_TIMER(timeout_);
        // While timeout hasn't expired and we haven't had everything acked
        while (!timedOut && _stored != 0
               && waitFor >= _getLowestUnpersisted())
        {
          if (!_blockStore.wait(timeout_))
          {
            // May have woken up early, check real time
            AMPS_RESET_TIMER(timedOut, timeout_);
          }
        }
        // If we timed out and still haven't caught up with the acks
        if (timedOut && _stored != 0
            && waitFor >= _getLowestUnpersisted())
        {
          throw TimedOutException("Timed out waiting to flush publish store.");
        }
      }
      else
      {
        while (_stored != 0 && waitFor >= _getLowestUnpersisted())
        {
          // Still wake up every 1s so python can interrupt
          _blockStore.wait(1000);
          // Don't hold lock if possibly grabbing GIL
          BufferUnlock unlck(_blockStore);
          amps_invoke_waiting_function();
        }
      }
    }

    amps_uint64_t getLowestUnpersisted() const
    {
      BufferLock bufferGuard(_blockStore);
      return _getLowestUnpersisted();
    }

    amps_uint64_t getHighestUnpersisted() const
    {
      BufferLock bufferGuard(_blockStore);
      return _getHighestUnpersisted();
    }

    amps_uint64_t getLastPersisted(void)
    {
      BufferLock bufferGuard(_blockStore);
      return _getLastPersisted();
    }

  protected:
    static bool canResize(size_t requestedSize_, void* vpThis_)
    {
      return ((BlockPublishStore*)vpThis_)->callResizeHandler(requestedSize_);
    }

    amps_uint64_t _getLowestUnpersisted() const
    {
      // Assume the lock is held
      // If we don't have anything, return MAX
      if (!_blockStore.front())
      {
        return getUnsetSequence();
      }
      return _blockStore.front()->_sequence;
    }

    amps_uint64_t _getHighestUnpersisted() const
    {
      // Assume the lock is held
      // If we don't have anything, return MAX
      if (!_blockStore.back())
      {
        return getUnsetSequence();
      }
      return _blockStore.back()->_sequence;
    }

    amps_uint64_t _getLastPersisted(void)
    {
      // Assume the lock is held
      amps_uint64_t lastPersisted = (amps_uint64_t)0;
      Buffer* pBuffer = _blockStore.getBuffer();
      pBuffer->setPosition(_metadataBlock->_offset + 8);
      lastPersisted = pBuffer->getUint64();
      if (lastPersisted)
      {
        if (_lastSequence < lastPersisted)
        {
          _lastSequence = lastPersisted;
        }
        return lastPersisted;
      }
      if (_maxDiscarded)
      {
        lastPersisted = _maxDiscarded;
      }
      else
      {
#ifdef _WIN32
        struct _timeb t;
        _ftime_s(&t);
        lastPersisted = (t.time * 1000 + t.millitm) * (amps_uint64_t)1000000;
#else // not _WIN32
        struct timeval tv;
        gettimeofday(&tv, NULL);
        lastPersisted = (amps_uint64_t)((tv.tv_sec * 1000) + (tv.tv_usec / 1000))
                        * (amps_uint64_t)1000000;
#endif
      }
      if (_lastSequence > 2)
      {
        amps_uint64_t low = _getLowestUnpersisted();
        amps_uint64_t high = _getHighestUnpersisted();
        if (low != getUnsetSequence())
        {
          lastPersisted = low - 1;
        }
        if (high != getUnsetSequence() && _lastSequence <= high)
        {
          _lastSequence = high;
        }
        if (_lastSequence < lastPersisted)
        {
          lastPersisted = _lastSequence - 1;
        }
      }
      else
      {
        _lastSequence = lastPersisted;
      }
      pBuffer->setPosition(_metadataBlock->_offset
                           + sizeof(amps_uint32_t)  // blocks used
                           + sizeof(amps_uint32_t)); // record length
      pBuffer->putUint64(lastPersisted);
      _metadataBlock->_sequence = lastPersisted;
      return lastPersisted;
    }

    void recover(void)
    {
      BufferLock bufferGuard(_blockStore);
      // Make sure the size isn't 0 and is a multiple of block size
      Buffer* pBuffer = _blockStore.getBuffer();
      size_t size = pBuffer->getSize();
      amps_uint32_t blockSize = getBlockSize();
      if (size == 0)
      {
        _blockStore.init();
        _metadataBlock = _blockStore.get(1);
        _metadataBlock->_sequence = (amps_uint64_t)0;
        pBuffer->setPosition(_metadataBlock->_offset);
        // Metadata block holds block size, block header size,
        // last discarded sequence, client version
        pBuffer->putUint32((amps_uint32_t)blockSize);
        pBuffer->putUint32((amps_uint32_t)getBlockHeaderSize());
        pBuffer->putUint64((amps_uint64_t)0);
        // Metadata blocks puts client version in CRC position
        pBuffer->putUint64((amps_uint64_t)VersionInfo::parseVersion(AMPS_CLIENT_VERSION));
        // No next in chain
        pBuffer->putUint64((amps_uint64_t)0);
        return;
      }
      size_t numBlocks = size / blockSize;
      if (size % blockSize > 0)
      {
        // We shouldn't ever be in here, since it requires starting with a
        // file that is not an even multiple of block size and we always
        // fix the size.
        numBlocks = size / blockSize;
        ++numBlocks;
        amps_uint32_t blockCount = 0;
        // We allocate all the Blocks at once below so delete allocated Block[]
        delete[] _blockStore.resizeBuffer(numBlocks * blockSize, &blockCount);
        // Resize can fail if resizeHandler is set and refuses the request
        // Since this is recovery, we need to simply fail in that case
        if (size > pBuffer->getSize() || numBlocks != (size_t)blockCount)
        {
          throw StoreException("Publish Store could not resize correctly during recovery, possibly due to resizeHandler refusing the request.");
        }
        size = pBuffer->getSize();
      }

      amps_uint64_t maxIdx = 0;
      amps_uint64_t minIdx = 0;
      size_t location = 0;
      BlockHeader blockHeader;
      // The blocks we create here all get their offset set in below loop
      Block* blocks = new Block[numBlocks];
      blocks[numBlocks - 1]._nextInList = 0;
      size_t blockNum = 0;
      _blockStore.addBlocks(blocks);
      _metadataBlock = blocks; // The first Block is metadata
      _metadataBlock->_nextInList = 0;
      pBuffer->setPosition(0);
      pBuffer->copyBytes((char*)&blockHeader, sizeof(BlockHeader));
      /* Metadata Block header fields
       * amps_uint32_t _blocksToWrite = BlockSize
       * amps_uint32_t _totalRemaining = BlockHeaderSize
       * amps_uint64_t _seq = last persisted sequence number
       * amps_uint64_t _crcVal = unused
       * amps_uint64_t _nextInChain = unused
      */
      if (blockHeader._blocksToWrite == 1) // Old store format?
      {
        /* Old format metadata block header fields
         * amps_uint32_t _blocksToWrite = 1
         * amps_uint32_t _totalRemaining = client version
         * amps_uint64_t _seq = last persisted sequence number
         * amps_uint64_t _crcVal = unused
         * amps_uint64_t _nextInChain = unused
        */
        // Readable old format starts with version 5.0.0.0
        if (blockHeader._totalRemaining >= 5000000)
        {
          // All recovery needs to be based on old format
          // so go do that instead.
          recoverOldFormat(blocks);
          return;
        }
        // Unreadable format, fail
        throw StoreException("Unrecognized format for Store. Can't recover.");
      }
      if (blockHeader._blocksToWrite == 0)
      {
        pBuffer->setPosition(0);
        pBuffer->putUint32((amps_uint32_t)blockSize);
      }
      else
      {
        blockSize = blockHeader._blocksToWrite;
        _blockStore.setBlockSize(blockSize);
      }
      if (blockHeader._totalRemaining == 0)
      {
        pBuffer->setPosition(sizeof(amps_uint32_t));
        pBuffer->putUint32((amps_uint32_t)getBlockHeaderSize());
      }
      else
      {
        _blockStore.setBlockHeaderSize(blockHeader._totalRemaining);
      }
      _metadataBlock->_sequence = blockHeader._seq;
      if (_metadataBlock->_sequence
          && _metadataBlock->_sequence < (amps_uint64_t)1000000)
      {
        pBuffer->setPosition(_metadataBlock->_offset
                             + sizeof(amps_uint32_t)   // BlockSize
                             + sizeof(amps_uint32_t)); // BlockHeaderSize
        pBuffer->putUint64((amps_uint64_t)0);
        _metadataBlock->_sequence = 0;
      }
      else
      {
        // Set _maxDiscarded and _lastSequence
        _maxDiscarded = _metadataBlock->_sequence;
        _lastSequence = _maxDiscarded;
      }
      // This would be where to check the client version string
      // No checks currently
      location += blockSize;
      amps_uint32_t freeCount = 0;
      Block* firstFree = NULL;
      Block* endOfFreeList = NULL;
      // Used to create used list in order after recovery
      typedef std::map<amps_uint64_t, Block*> RecoverMap;
      RecoverMap recoveredBlocks;
      while (location < size)
      {
        // Get index and check if non-zero
        pBuffer->setPosition(location);
        pBuffer->copyBytes((char*)&blockHeader, sizeof(BlockHeader));
        if ((blockHeader._seq > 0 && blockHeader._totalRemaining < size) &&
            (!blockHeader._crcVal || recoveredBlocks.count(blockHeader._seq)))
        {
          // Block is part of a chain
          location += blockSize;
          continue;
        }
        Block* block = blocks[++blockNum].setOffset(location);
        bool recovered = false;
        if (blockHeader._seq > 0 && blockHeader._totalRemaining < size)
        {
          blockHeader._totalRemaining -= (amps_uint32_t)getBlockChainHeaderSize();
          block->_sequence = blockHeader._seq;
          // Track min and max
          if (maxIdx < blockHeader._seq)
          {
            maxIdx = blockHeader._seq;
          }
          if (minIdx > blockHeader._seq)
          {
            minIdx = blockHeader._seq;
          }
          // Save it in recovered blocks
          recoveredBlocks[blockHeader._seq] = block;
          // Set up the chain
          while (blockHeader._nextInChain != (amps_uint64_t)0)
          {
            Block* chain = blocks[++blockNum]
                           .setOffset((size_t)blockHeader._nextInChain);
            chain->_nextInList = 0;
            pBuffer->setPosition((size_t)blockHeader._nextInChain
                                 + sizeof(amps_uint32_t)   // blocks used
                                 + sizeof(amps_uint32_t)   // record length
                                 + sizeof(amps_uint64_t)   // seq
                                 + sizeof(amps_uint64_t)); // crc
            blockHeader._nextInChain = pBuffer->getUint64();
            block->_nextInChain = chain;
            block = chain;
            block->_sequence = blockHeader._seq;
          }
          recovered = true;
        }
        if (!recovered)
        {
          // Put this Block on the free list
          if (endOfFreeList)
          {
            endOfFreeList->_nextInList = block;
          }
          else
          {
            firstFree = block;
          }
          endOfFreeList = block;
          ++freeCount;
        }
        location += blockSize;
      }
      if (endOfFreeList)
      {
        endOfFreeList->_nextInList = 0;
      }
      _blockStore.setFreeList(firstFree, freeCount);
      if (maxIdx > _lastSequence)
      {
        _lastSequence = maxIdx;
      }
      if (minIdx > _maxDiscarded + 1)
      {
        _maxDiscarded = minIdx - 1;
      }
      if (_maxDiscarded > _metadataBlock->_sequence)
      {
        _metadataBlock->_sequence = _maxDiscarded;
        pBuffer->setPosition(_metadataBlock->_offset + 8);
        pBuffer->putUint64(_maxDiscarded);
      }
      Block* end = NULL;
      AMPS_FETCH_ADD(&_stored, (long)(recoveredBlocks.size()));
      for (RecoverMap::iterator i = recoveredBlocks.begin();
           i != recoveredBlocks.end(); ++i)
      {
        if (end)
        {
          end->_nextInList = i->second;
        }
        else
        {
          _blockStore.setUsedList(i->second);
        }
        end = i->second;
      }
      if (end)
      {
        end->_nextInList = 0;
      }
      _blockStore.setEndOfUsedList(end);
    }

  private:
    // Lock should already be held
    void replayOnto(Block* block_, StoreReplayer& replayer_)
    {
      // Read the header
      size_t start = block_->_offset;
      size_t position = start;
      Buffer* pBuffer = _blockStore.getBuffer();
      pBuffer->setPosition(position);
      BlockHeader blockHeader;
      pBuffer->copyBytes((char*)&blockHeader, sizeof(BlockHeader));
      if (blockHeader._totalRemaining == 0)
      {
        // Queue acking sow_delete
        return;
      }
      position += getBlockHeaderSize();
      BlockChainHeader blockChainHeader;
      pBuffer->copyBytes((char*)&blockChainHeader, sizeof(blockChainHeader));
      if (blockChainHeader._operation == Message::Command::Unknown)
      {
        // Queue acking sow_delete
        return;
      }
      blockChainHeader._ackTypes |= Message::AckType::Persisted;
      position += getBlockChainHeaderSize();
      blockHeader._totalRemaining -= (amps_uint32_t)getBlockChainHeaderSize();
      pBuffer->setPosition(position);

      if (blockHeader._totalRemaining
          <   blockChainHeader._commandIdLen
          + blockChainHeader._correlationIdLen
          + blockChainHeader._expirationLen
          + blockChainHeader._sowKeyLen
          + blockChainHeader._topicLen)
      {
        std::ostringstream os;
        os << "Corrupted message found with invalid lengths. "
           << "Attempting to replay " << block_->_sequence
           << ". Block sequence " << blockHeader._seq
           << ", topic length " << blockChainHeader._topicLen
           << ", data length " << blockHeader._totalRemaining
           << ", command ID length " << blockChainHeader._commandIdLen
           << ", correlation ID length " << blockChainHeader._correlationIdLen
           << ", expiration length " << blockChainHeader._expirationLen
           << ", sow key length " << blockChainHeader._sowKeyLen
           << ", start " << start
           << ", position " << position
           << ", buffer size " << pBuffer->getSize();
        throw StoreException(os.str());
      }

      // Start prepping the message
      _message.reset();
      _message.setCommandEnum((Message::Command::Type)blockChainHeader._operation);
      _message.setAckTypeEnum((unsigned)blockChainHeader._ackTypes
                              | Message::AckType::Persisted);
      _message.setSequence(blockHeader._seq);
      // Read the data and calculate the CRC
      Block* current = block_;
      size_t blockBytesRemaining = getBlockDataSize() - getBlockChainHeaderSize();
      amps_uint64_t crcCalc = (amps_uint64_t)0ULL;
      // Use tmpBuffers for any fields split across Block boundaries
      char** tmpBuffers = (blockHeader._blocksToWrite > 1) ? new char* [blockHeader._blocksToWrite - 1] : 0;
      size_t blockNum = 0;
      if (blockChainHeader._commandIdLen > 0)
      {
        if (blockChainHeader._commandIdLen <= blockBytesRemaining)
        {
          _message.assignCommandId(pBuffer->getBytes(blockChainHeader._commandIdLen)._data,
                                   blockChainHeader._commandIdLen);
          blockBytesRemaining -= blockChainHeader._commandIdLen;
        }
        else
        {
          tmpBuffers[blockNum] = new char[blockChainHeader._commandIdLen]; // -V522
          size_t totalLeft = blockChainHeader._commandIdLen;
          size_t totalRead = 0;
          size_t readLen = 0;
          while (totalLeft)
          {
            readLen = blockBytesRemaining < totalLeft ?
                      blockBytesRemaining : totalLeft;
            pBuffer->copyBytes(tmpBuffers[blockNum] + totalRead, readLen);
            if (!(totalLeft -= readLen))
            {
              break;
            }
            if (!(current = current->_nextInChain))
            {
              break;
            }
            totalRead += readLen;
            blockBytesRemaining = getBlockDataSize();
            position = current->_offset + getBlockHeaderSize();
            pBuffer->setPosition(position);
          }
          blockBytesRemaining -= readLen;
          _message.assignCommandId(tmpBuffers[blockNum++], blockChainHeader._commandIdLen);
        }
        blockHeader._totalRemaining -= blockChainHeader._commandIdLen;
        crcCalc = _crc(_message.getCommandId().data(),
                       blockChainHeader._commandIdLen, crcCalc);
      }
      if (blockChainHeader._correlationIdLen > 0)
      {
        if (blockChainHeader._correlationIdLen <= blockBytesRemaining)
        {
          _message.assignCorrelationId(
            pBuffer->getBytes(blockChainHeader._correlationIdLen)._data,
            blockChainHeader._correlationIdLen);
          blockBytesRemaining -= blockChainHeader._correlationIdLen;
        }
        else
        {
          tmpBuffers[blockNum] = new char[blockChainHeader._correlationIdLen]; // -V522
          size_t totalLeft = blockChainHeader._correlationIdLen;
          size_t totalRead = 0;
          size_t readLen = 0;
          while (totalLeft)
          {
            readLen = blockBytesRemaining < totalLeft ?
                      blockBytesRemaining : totalLeft;
            pBuffer->copyBytes(tmpBuffers[blockNum] + totalRead, readLen);
            if (!(totalLeft -= readLen))
            {
              break;
            }
            if (!(current = current->_nextInChain))
            {
              break;  // -V522
            }
            totalRead += readLen;
            blockBytesRemaining = getBlockDataSize();
            position = current->_offset + getBlockHeaderSize();
            pBuffer->setPosition(position);
          }
          blockBytesRemaining -= readLen;
          _message.assignCorrelationId(tmpBuffers[blockNum++], blockChainHeader._correlationIdLen);
        }
        blockHeader._totalRemaining -= blockChainHeader._correlationIdLen;
        crcCalc = _crc(_message.getCorrelationId().data(),
                       blockChainHeader._correlationIdLen, crcCalc);
      }
      if (blockChainHeader._expirationLen > 0)
      {
        if (blockChainHeader._expirationLen <= blockBytesRemaining)
        {
          _message.assignExpiration(
            pBuffer->getBytes(blockChainHeader._expirationLen)._data,
            blockChainHeader._expirationLen);
          blockBytesRemaining -= blockChainHeader._expirationLen;
        }
        else
        {
          tmpBuffers[blockNum] = new char[blockChainHeader._expirationLen]; // -V522
          size_t totalLeft = blockChainHeader._expirationLen;
          size_t totalRead = 0;
          size_t readLen = 0;
          while (totalLeft)
          {
            readLen = blockBytesRemaining < totalLeft ?
                      blockBytesRemaining : totalLeft;
            pBuffer->copyBytes(tmpBuffers[blockNum] + totalRead, readLen);
            if (!(totalLeft -= readLen))
            {
              break;
            }
            if (!(current = current->_nextInChain))
            {
              break;
            }
            totalRead += readLen;
            blockBytesRemaining = getBlockDataSize();
            position = current->_offset + getBlockHeaderSize();
            pBuffer->setPosition(position);
          }
          blockBytesRemaining -= readLen;
          _message.assignExpiration(tmpBuffers[blockNum++], blockChainHeader._expirationLen);
        }
        blockHeader._totalRemaining -= blockChainHeader._expirationLen;
        crcCalc = _crc(_message.getExpiration().data(),
                       blockChainHeader._expirationLen, crcCalc);
      }
      if (blockChainHeader._sowKeyLen > 0)
      {
        if (blockChainHeader._sowKeyLen <= blockBytesRemaining)
        {
          _message.assignSowKey(pBuffer->getBytes(blockChainHeader._sowKeyLen)._data,
                                blockChainHeader._sowKeyLen);
          blockBytesRemaining -= blockChainHeader._sowKeyLen;
        }
        else
        {
          tmpBuffers[blockNum] = new char[blockChainHeader._sowKeyLen]; // -V522
          size_t totalLeft = blockChainHeader._sowKeyLen;
          size_t totalRead = 0;
          size_t readLen = 0;
          while (totalLeft)
          {
            readLen = blockBytesRemaining < totalLeft ?
                      blockBytesRemaining : totalLeft;
            pBuffer->copyBytes(tmpBuffers[blockNum] + totalRead, readLen);
            if (!(totalLeft -= readLen))
            {
              break;
            }
            if (!(current = current->_nextInChain))
            {
              break;
            }
            totalRead += readLen;
            blockBytesRemaining = getBlockDataSize();
            position = current->_offset + getBlockHeaderSize();
            pBuffer->setPosition(position);
          }
          blockBytesRemaining -= readLen;
          _message.assignSowKey(tmpBuffers[blockNum++], blockChainHeader._sowKeyLen);
        }
        blockHeader._totalRemaining -= blockChainHeader._sowKeyLen;
        crcCalc = _crc(_message.getSowKey().data(), blockChainHeader._sowKeyLen, crcCalc);
      }
      if (blockChainHeader._topicLen > 0)
      {
        if (blockChainHeader._topicLen <= blockBytesRemaining)
        {
          _message.assignTopic(pBuffer->getBytes(blockChainHeader._topicLen)._data,
                               blockChainHeader._topicLen);
          blockBytesRemaining -= blockChainHeader._topicLen;
        }
        else
        {
          tmpBuffers[blockNum] = new char[blockChainHeader._topicLen]; // -V522
          size_t totalLeft = blockChainHeader._topicLen;
          size_t totalRead = 0;
          size_t readLen = 0;
          while (totalLeft)
          {
            readLen = blockBytesRemaining < totalLeft ?
                      blockBytesRemaining : totalLeft;
            pBuffer->copyBytes(tmpBuffers[blockNum] + totalRead, readLen);
            if (!(totalLeft -= readLen))
            {
              break;
            }
            if (!(current = current->_nextInChain))
            {
              break;
            }
            totalRead += readLen;
            blockBytesRemaining = getBlockDataSize();
            position = current->_offset + getBlockHeaderSize();
            pBuffer->setPosition(position);
          }
          blockBytesRemaining -= readLen;
          _message.assignTopic(tmpBuffers[blockNum++], blockChainHeader._topicLen);
        }
        blockHeader._totalRemaining -= blockChainHeader._topicLen;
        crcCalc = _crc(_message.getTopic().data(), blockChainHeader._topicLen, crcCalc);
      }
      if (blockHeader._totalRemaining > 0)
      {
        if (blockHeader._totalRemaining <= blockBytesRemaining)
        {
          if (blockChainHeader._flag == -1 || blockChainHeader._flag == SOW_DELETE_DATA)
          {
            _message.assignData(
              pBuffer->getBytes(blockHeader._totalRemaining)._data,
              blockHeader._totalRemaining);
            crcCalc = _crc(_message.getData().data(),
                           blockHeader._totalRemaining, crcCalc);
          }
          else if (blockChainHeader._flag == SOW_DELETE_FILTER)
          {
            _message.assignFilter(
              pBuffer->getBytes(blockHeader._totalRemaining)._data,
              blockHeader._totalRemaining);
            crcCalc = _crc(_message.getFilter().data(),
                           blockHeader._totalRemaining, crcCalc);
          }
          else if (blockChainHeader._flag == SOW_DELETE_KEYS)
          {
            _message.assignSowKeys(
              pBuffer->getBytes(blockHeader._totalRemaining)._data,
              blockHeader._totalRemaining);
            crcCalc = _crc(_message.getSowKeys().data(),
                           blockHeader._totalRemaining, crcCalc);
          }
          else if (blockChainHeader._flag == SOW_DELETE_BOOKMARK)
          {
            _message.assignBookmark(
              pBuffer->getBytes(blockHeader._totalRemaining)._data,
              blockHeader._totalRemaining);
            crcCalc = _crc(_message.getBookmark().data(),
                           blockHeader._totalRemaining, crcCalc);
          }
          else if (blockChainHeader._flag == SOW_DELETE_BOOKMARK_CANCEL)
          {
            _message.assignBookmark(
              pBuffer->getBytes(blockHeader._totalRemaining)._data,
              blockHeader._totalRemaining);
            crcCalc = _crc(_message.getBookmark().data(),
                           blockHeader._totalRemaining, crcCalc);
            _message.assignOptions(AMPS_OPTIONS_CANCEL, 6);
          }
        }
        else
        {
          tmpBuffers[blockNum] = new char[blockHeader._totalRemaining]; // -V522
          size_t totalLeft = blockHeader._totalRemaining;
          size_t totalRead = 0;
          size_t readLen = 0;
          while (totalLeft)
          {
            readLen = blockBytesRemaining < totalLeft ?
                      blockBytesRemaining : totalLeft;
            pBuffer->copyBytes(tmpBuffers[blockNum] + totalRead, readLen);
            if (!(totalLeft -= readLen))
            {
              break;
            }
            if (!(current = current->_nextInChain))
            {
              break;
            }
            totalRead += readLen;
            blockBytesRemaining = getBlockDataSize();
            position = current->_offset + getBlockHeaderSize();
            pBuffer->setPosition(position);
          }
          position += readLen;
          if (blockChainHeader._flag == -1 || blockChainHeader._flag == SOW_DELETE_DATA)
          {
            _message.assignData(tmpBuffers[blockNum], blockHeader._totalRemaining);
          }
          else if (blockChainHeader._flag == SOW_DELETE_FILTER)
          {
            _message.assignFilter(tmpBuffers[blockNum], blockHeader._totalRemaining);
          }
          else if (blockChainHeader._flag == SOW_DELETE_KEYS)
          {
            _message.assignSowKeys(tmpBuffers[blockNum], blockHeader._totalRemaining);
          }
          else if (blockChainHeader._flag == SOW_DELETE_BOOKMARK)
          {
            _message.assignBookmark(tmpBuffers[blockNum], blockHeader._totalRemaining);
          }
          else if (blockChainHeader._flag == SOW_DELETE_BOOKMARK_CANCEL)
          {
            _message.assignBookmark(tmpBuffers[blockNum], blockHeader._totalRemaining);
            _message.assignOptions(AMPS_OPTIONS_CANCEL, 6);
          }
          crcCalc = _crc(tmpBuffers[blockNum++], blockHeader._totalRemaining, crcCalc); // -V595
        }
      }

      // Validate the crc and seq
      if (crcCalc != blockHeader._crcVal || blockHeader._seq != block_->_sequence)
      {
        std::ostringstream os;
        os << "Corrupted message found by CRC or sequence "
           << "Attempting to replay " << block_->_sequence
           << ". Block sequence " << blockHeader._seq
           << ", expiration length " << blockChainHeader._expirationLen
           << ", sowKey length " << blockChainHeader._sowKeyLen
           << ", topic length " << blockChainHeader._topicLen
           << ", data length " << blockHeader._totalRemaining
           << ", command ID length " << blockChainHeader._commandIdLen
           << ", correlation ID length " << blockChainHeader._correlationIdLen
           << ", flag " << blockChainHeader._flag
           << ", expected CRC " << blockHeader._crcVal
           << ", actual CRC " << crcCalc
           << ", start " << start
           << ", position " << position
           << ", buffer size " << pBuffer->getSize();
        for (Block* block = block_; block; block = block->_nextInChain)
        {
          os << "\n BLOCK " << block->_offset;
        }
        if (tmpBuffers)
        {
          for (amps_uint32_t i = 0; i < blockNum; ++i)
          {
            delete[] tmpBuffers[i];  // -V522
          }
          delete[] tmpBuffers;
        }
        throw StoreException(os.str());
      }
      // Replay the message
      replayer_.execute(_message);
      // Free the buffer if allocated
      if (tmpBuffers)
      {
        for (amps_uint32_t i = 0; i < blockNum; ++i)
        {
          delete[] tmpBuffers[i];  // -V522
        }
        delete[] tmpBuffers;
      }
    }

    // Lock should already be held
    // Read an older format file and update it.
    void recoverOldFormat(Block* blocks)
    {
      Buffer* pBuffer = _blockStore.getBuffer();
      amps_uint64_t maxIdx = 0;
      amps_uint64_t minIdx = 0;
      size_t size = pBuffer->getSize();
      size_t location = 0;
      pBuffer->setPosition(location);
      pBuffer->putUint32((amps_uint32_t)getBlockSize());
      pBuffer->putUint32((amps_uint32_t)_blockStore.getBlockHeaderSize());
      _metadataBlock->_sequence = pBuffer->getUint64();
      if (_metadataBlock->_sequence < (amps_uint64_t)1000000)
      {
        pBuffer->setPosition(_metadataBlock->_offset + 8);
        pBuffer->putUint64((amps_uint64_t)0);
        _metadataBlock->_sequence = 0;
      }
      else
      {
        // Set _maxDiscarded and _lastSequence
        _maxDiscarded = _metadataBlock->_sequence;
        _lastSequence = _maxDiscarded;
      }
      // Write the current client version
      pBuffer->putUint64((amps_uint64_t)VersionInfo::parseVersion(AMPS_CLIENT_VERSION));
      // No next in chain
      pBuffer->putUint64((amps_uint64_t)0);
      // No checks currently
      location += getBlockSize();
      amps_uint32_t freeCount = 0;
      Block* firstFree = NULL;
      Block* endOfFreeList = NULL;
      size_t blockSize = getBlockSize();
      size_t numBlocks = size / blockSize;
      size_t blockNum = 0;
      // Used to create used list in order after recovery
      typedef std::map<amps_uint64_t, Block*> RecoverMap;
      RecoverMap recoveredBlocks;
      RecoverMap growingBlocks;
      amps_uint32_t growthBlocksNeeded = 0;
      while (location < size)
      {
        // Get seq and check if non-zero
        pBuffer->setPosition(location);
        BlockHeader blockHeader;
        pBuffer->copyBytes((char*)&blockHeader, sizeof(BlockHeader));
        size_t blockCount = (size_t)blockHeader._blocksToWrite;
        if (blockHeader._totalRemaining > 0 && blockHeader._seq > 0
            && blockHeader._totalRemaining < size
            && blockHeader._blocksToWrite < numBlocks
            && (blockHeader._blocksToWrite * blockSize)
            >= blockHeader._totalRemaining)
        {
          size_t oldFormatSize = blockHeader._totalRemaining;
          // Old format total was storage bytes plus 64 bytes for block
          // and chain headers.
          blockHeader._totalRemaining -= 64;
          // New format counts only chain header size
          blockHeader._totalRemaining += getBlockChainHeaderSize();
          // Get the rest of the header
          BlockChainHeader chainHeader;
          // Need to reset location to after OLD header:
          // amps_uint32_t blocks, amps_uint32_t totalRemaining,
          // amps_uint64_t seq, amps_uint64_t crc
          pBuffer->setPosition(location + (sizeof(amps_uint32_t) * 2)
                               + (sizeof(amps_uint64_t) * 2) );
          // Read old chain header which uses same order, but not
          // as many bytes (everything is 32bit):
          // operation, commandIdLen, correlationIdLen,
          // expirationLen, sowKeyLen, topicLen, flag, ackTypes
          pBuffer->copyBytes((char*)&chainHeader,
                             sizeof(amps_uint32_t) * 8);
          // Check for garbage, likely indicating this is part of a chain
          if ((chainHeader._commandIdLen + chainHeader._correlationIdLen
               + chainHeader._expirationLen + chainHeader._sowKeyLen
               + chainHeader._topicLen) > blockHeader._totalRemaining)
          {
            // Skip this block, can't be real data
            location += getBlockSize();
            continue;
          }
          // Check if data fits in current number of blocks
          amps_uint32_t blocksNeeded = (blockHeader._totalRemaining
                                        / getBlockDataSize())
                                       + (blockHeader._totalRemaining
                                          % getBlockDataSize()
                                          ? 1 : 0);
          if (blocksNeeded == blockHeader._blocksToWrite)
          {
            Block* first = blocks[++blockNum].setOffset(location);
            first->_nextInList = 0;
            first->_sequence = blockHeader._seq;
            if (blockHeader._blocksToWrite > 1)
            {
              // CRC is only set on the first block
              amps_uint64_t crcVal = blockHeader._crcVal;
              blockHeader._crcVal = 0;
              Block* current = 0;
              // It fits, just need to adjust the block formats
              // and set up the chain. Start with the last block
              // and move data as needed starting at the end.
              size_t currentBlockNum = blockNum
                                       + blockHeader._blocksToWrite
                                       - 1;
              // Last item could wrap to beginning, but beginning is
              // block 1, not 0, which is the metadata block.
              if (currentBlockNum >= numBlocks)
              {
                currentBlockNum = currentBlockNum - numBlocks + 1;
              }
              if (currentBlockNum < blockNum)
              {
                Block* last = blocks[currentBlockNum]
                              .init(currentBlockNum, getBlockSize());
                if ((current = firstFree) == last)
                {
                  firstFree = firstFree->_nextInList; // -V522
                  if (!firstFree)
                  {
                    endOfFreeList = 0;
                  }
                  --freeCount;
                }
                else
                {
                  while (current)
                  {
                    if (current->_nextInList == last)
                    {
                      current->_nextInList = last->_nextInList;
                      current = last;
                      --freeCount;
                      break;
                    }
                    current = current->_nextInList;
                  }
                }
              }
              if (!current)
              {
                current = blocks[currentBlockNum]
                          .init(currentBlockNum, getBlockSize());
              }
              // Initially, the number of bytes in last block
              size_t dataBytes = oldFormatSize % getBlockSize();
              while (current != first)
              {
                current->_nextInList = 0;
                current->_sequence = blockHeader._seq;
                // Set _nextInChain on previous Block, will include first
                if (--currentBlockNum < 1
                    || currentBlockNum > numBlocks)
                {
                  currentBlockNum = numBlocks - 1;
                }
                Block* previous = blocks[currentBlockNum]
                                  .init(currentBlockNum,
                                        getBlockSize());
                previous->_nextInChain = current;
                // Shift to make room for a header in every block
                // Not growing, so this won't write past the end.
                // Shift amount accounts for a header added to each
                // block after the first plus any change in the
                // chain header size from 32, which is the old size.
                size_t bytesToMove = --blockCount
                                     * getBlockHeaderSize()
                                     + (getBlockChainHeaderSize()
                                        - 32);
                pBuffer->copyBytes(current->_offset + bytesToMove,
                                   current->_offset,
                                   dataBytes);
                dataBytes = getBlockSize();
                if (bytesToMove > getBlockHeaderSize())
                {
                  bytesToMove -= getBlockHeaderSize();
                  dataBytes -= bytesToMove;
                  pBuffer->copyBytes(current->_offset
                                     + getBlockHeaderSize(),
                                     previous->_offset
                                     + dataBytes,
                                     bytesToMove);
                }
                // Set next in chain for this block's header
                blockHeader._nextInChain = (current->_nextInChain
                                            ? current->_nextInChain->_offset
                                            : (amps_uint64_t)0);
                // Write the header for this block
                pBuffer->setPosition(current->_offset);
                pBuffer->putBytes((const char*)&blockHeader,
                                  sizeof(BlockHeader));
                if (firstFree == previous)
                {
                  firstFree = firstFree->_nextInList;
                  if (!firstFree)
                  {
                    endOfFreeList = 0;
                  }
                  --freeCount;
                }
                else
                {
                  current = firstFree;
                  while (current)
                  {
                    if (current->_nextInList == previous)
                    {
                      current->_nextInList = previous->_nextInList;
                      --freeCount;
                      break;
                    }
                    current = current->_nextInList;
                  }
                }
                current = previous;
              }
              blockNum += blockHeader._blocksToWrite - 1;
              blockHeader._crcVal = crcVal;
            }
            // Move bytes for chain header expansion from 32 bytes
            size_t bytesToMove = getBlockDataSize() - 32
                                 - (getBlockChainHeaderSize() - 32);
            pBuffer->copyBytes(first->_offset + getBlockHeaderSize()
                               + getBlockChainHeaderSize(),
                               first->_offset + getBlockHeaderSize() + 32,
                               bytesToMove);
            // Rewrite the header and chain header for first Block.
            pBuffer->setPosition(first->_offset);
            blockHeader._nextInChain = (first->_nextInChain
                                        ? first->_nextInChain->_offset
                                        : (amps_uint64_t)0);
            pBuffer->putBytes((const char*)&blockHeader,
                              sizeof(BlockHeader));
            pBuffer->putBytes((const char*)&chainHeader,
                              sizeof(BlockChainHeader));
            // Add first Block to recovered for building the used
            // list later
            recoveredBlocks[blockHeader._seq] = first;
          }
          else
          {
            // This will need at least one more Block due to a header in
            // every Block. Check how many and save for later.
            growingBlocks[blockHeader._seq] = blocks[++blockNum].setOffset(location);
            growthBlocksNeeded += (blocksNeeded - blockHeader._blocksToWrite);
            blockNum += blockHeader._blocksToWrite - 1;
          }
          // Track min and max
          if (maxIdx < blockHeader._seq)
          {
            maxIdx = blockHeader._seq;
          }
          if (minIdx > blockHeader._seq)
          {
            minIdx = blockHeader._seq;
          }
          // Advance past read blocks
          location += blockHeader._blocksToWrite * getBlockSize();
          // Either we're exiting loop, or blockNum is in range
          assert(location >= size || blockNum < numBlocks);
        }
        else
        {
          // Put this Block on the free list
          Block* block = blocks[++blockNum].setOffset(location);
          if (endOfFreeList)
          {
            endOfFreeList->_nextInList = block;
          }
          else
          {
            firstFree = block;
          }
          endOfFreeList = block;
          ++freeCount;
          location += blockSize;
        }
      }
      for (RecoverMap::iterator i = growingBlocks.begin();
           i != growingBlocks.end(); ++i)
      {
        Block* first = i->second;
        pBuffer->setPosition(first->_offset);
        BlockHeader blockHeader;
        // Read an old BlockHeader, which is only 24 bytes.
        // The bytes match current BlockHeader, and _nextInChain is 0.
        pBuffer->copyBytes((char*)&blockHeader, 24);
        // Old format total was storage bytes plus 64 bytes for block
        // and chain headers.
        blockHeader._totalRemaining -= 64;
        // New format counts only chain header size
        blockHeader._totalRemaining += getBlockChainHeaderSize();
        if (freeCount < growthBlocksNeeded)
        {
          // We have to resize, let's try to do it once.
          amps_uint32_t minBlocksRequired = growthBlocksNeeded - freeCount;
          amps_uint32_t growthBlocks = _blockStore.getDefaultResizeBlocks();
          if (growthBlocks < minBlocksRequired)
          {
            amps_uint32_t defaultBlocks = _blockStore.getDefaultResizeBlocks();
            if (minBlocksRequired % defaultBlocks)
              minBlocksRequired = (minBlocksRequired / defaultBlocks + 1)
                                  * defaultBlocks;
            growthBlocks = minBlocksRequired;
          }
          amps_uint32_t newBlocks = 0;
          Block* addedBlocks = _blockStore.resizeBuffer(
                                 pBuffer->getSize()
                                 + growthBlocks * blockSize,
                                 &newBlocks);
          if (!addedBlocks)
          {
            throw StoreException("Failed to grow store buffer during recovery");
          }
          _blockStore.addBlocks(addedBlocks);
          freeCount += newBlocks;
          growthBlocksNeeded = (growthBlocksNeeded > freeCount)
                               ? growthBlocksNeeded - freeCount : 0;
          if (endOfFreeList)
          {
            endOfFreeList->_nextInList = addedBlocks;
          }
          else
          {
            firstFree = addedBlocks;
          }
          endOfFreeList = &(addedBlocks[newBlocks - 1]);
          endOfFreeList->_nextInList = 0;
        }
        expandBlocks(blocks, first->_offset, first, blockHeader,
                     &firstFree, &freeCount, pBuffer);
        // Add first Block to recovered for building the used list later
        recoveredBlocks[blockHeader._seq] = first;
        if (!firstFree)
        {
          endOfFreeList = 0;
        }
      }
      if (endOfFreeList)
      {
        endOfFreeList->_nextInList = 0;
      }
      _blockStore.setFreeList(firstFree, freeCount);
      if (maxIdx > _lastSequence)
      {
        _lastSequence = maxIdx;
      }
      if (minIdx > _maxDiscarded + 1)
      {
        _maxDiscarded = minIdx - 1;
      }
      if (_maxDiscarded > _metadataBlock->_sequence)
      {
        _metadataBlock->_sequence = _maxDiscarded;
        pBuffer->setPosition(_metadataBlock->_offset + 8);
        pBuffer->putUint64(_maxDiscarded);
      }
      Block* end = NULL;
      AMPS_FETCH_ADD(&_stored, (long)(recoveredBlocks.size()));
      for (RecoverMap::iterator i = recoveredBlocks.begin();
           i != recoveredBlocks.end(); ++i)
      {
        if (_blockStore.front())
        {
          end->_nextInList = i->second; // -V522
        }
        else
        {
          _blockStore.setUsedList(i->second);
        }
        end = i->second;
      }
      if (end)
      {
        end->_nextInList = 0;
      }
      _blockStore.setEndOfUsedList(end);
    }

    // For recovering an old format store to current format when more Blocks
    // are needed with the new format.
    void expandBlocks(Block* blocks_, size_t location_, Block* first_,
                      BlockHeader blockHeader_,
                      Block** pFreeList_, amps_uint32_t* pFreeCount_,
                      Buffer* pBuffer_)
    {
      // First create the chain, then we'll fill in reverse
      Block* current = first_;
      // Old format total was storage bytes plus 64 bytes for block
      // and chain headers.
      amps_uint32_t oldTotalRemaining = blockHeader_._totalRemaining;
      blockHeader_._totalRemaining -= 64;
      // New format counts only chain header size
      blockHeader_._totalRemaining += getBlockChainHeaderSize();
      // Check how many Blocks needed and if we have enough free.
      amps_uint32_t blocksNeeded = blockHeader_._totalRemaining
                                   / getBlockDataSize()
                                   + (blockHeader_._totalRemaining
                                      % getBlockDataSize()
                                      ? 1 : 0);
      // Last data block size, remove bytes saved in first block
      // then mod by block size.
      const amps_uint32_t blockSize = getBlockSize();
      // Old total remaining had all header included
      size_t endBlockSize = oldTotalRemaining % blockSize;
      if (!endBlockSize)
      {
        endBlockSize = blockSize;
      }
      size_t endOfData = 0;
      // Hang on to CRC until first block is written
      amps_uint64_t crcVal = blockHeader_._crcVal;
      blockHeader_._crcVal = 0;

      std::stack<Block*> blocksUsed;
      for (amps_uint32_t i = 1; i < blocksNeeded; ++i)
      {
        blocksUsed.push(current);
        current->_sequence = blockHeader_._seq;
        if (i >= blockHeader_._blocksToWrite)
        {
          if (i == blockHeader_._blocksToWrite)
          {
            endOfData = current->_offset + endBlockSize;
          }
          current->_nextInChain = *pFreeList_;
          --(*pFreeCount_);
          *pFreeList_ = (*pFreeList_)->_nextInList;
        }
        else
        {
          current->_nextInChain = current->_nextInList;
          if (current->_nextInChain)
          {
            if (current->_offset + blockSize < pBuffer_->getSize())
            {
              current->_nextInChain->setOffset(current->_offset
                                               + blockSize);
            }
            else
            {
              current->_nextInChain->setOffset(blockSize);
            }
          }
          else
          {
            current->_nextInChain = blocks_[1].init(1, blockSize);
          }
          if (current->_nextInChain == *pFreeList_)
          {
            *pFreeList_ = (*pFreeList_)->_nextInList;
            --(*pFreeCount_);
          }
          else
          {
            for (Block* free = *pFreeList_; free;
                 free = free->_nextInList)
            {
              if (free->_nextInList == current->_nextInChain)
              {
                free->_nextInList = free->_nextInList->_nextInList;
                --(*pFreeCount_);
                break;
              }
            }
          }
        }
        current->_nextInList = 0;
        current = current->_nextInChain;
      }
      // Make sure we write the correct number of blocks to write
      blockHeader_._blocksToWrite = blocksNeeded;
      // Finish setting up current
      current->_nextInList = 0;
      current->_sequence = blockHeader_._seq;
      // Now shift data, starting at the last Block
      // The total shift is for number of Blocks beyond the first
      // times Block header size, since previous format only wrote
      // the header in the first Block and had contiguous data,
      // with only wrap from end to beginning of buffer possible.

      // First time through, this is bytes in last block. After,
      // it will be block data size.
      size_t dataBytes = blockHeader_._totalRemaining % getBlockDataSize();
      while (current != first_)
      {
        size_t chunkBytesAvail = endOfData > location_
                                 ? endOfData - location_
                                 : endOfData - 2048;
        if (chunkBytesAvail < dataBytes)
        {
          // Original was wrapped from end to start of buffer
          // Need to copy what's left at start to end of Block,
          // then start working from the end.
          // This can ONLY occur during wrap because the first
          // Block doesn't get moved in this loop.
          pBuffer_->copyBytes(current->_offset
                              + getBlockSize()
                              - chunkBytesAvail,
                              getBlockSize(),
                              chunkBytesAvail);
          chunkBytesAvail = dataBytes - chunkBytesAvail;
          endOfData = pBuffer_->getSize() - chunkBytesAvail;
          pBuffer_->copyBytes(current->_offset + getBlockHeaderSize(),
                              endOfData,
                              chunkBytesAvail);
        }
        else
        {
          endOfData -= dataBytes;
          pBuffer_->copyBytes(current->_offset + getBlockHeaderSize(),
                              endOfData,
                              dataBytes);
        }
        // Set next in chain in block header
        blockHeader_._nextInChain = (current->_nextInChain
                                     ? current->_nextInChain->_offset
                                     : (amps_uint64_t)0);
        // Write the header for this block
        pBuffer_->setPosition(current->_offset);
        pBuffer_->putBytes((const char*)&blockHeader_, sizeof(BlockHeader));
        current = blocksUsed.top();
        blocksUsed.pop();
        dataBytes = getBlockDataSize();
      }
      // Move bytes for chain header expansion from 32 bytes
      pBuffer_->copyBytes(first_->_offset
                          + getBlockHeaderSize()
                          + getBlockChainHeaderSize(),
                          first_->_offset + getBlockHeaderSize() + 32,
                          getBlockDataSize() - getBlockChainHeaderSize());
      // Set the CRC to indicate first block and set nextInChain
      blockHeader_._crcVal = crcVal;
      blockHeader_._nextInChain = first_->_nextInChain->_offset;
      // Need to reset location to after OLD header:
      // amps_uint32_t blocks, amps_uint32_t totalRemaining,
      // amps_uint64_t seq, amps_uint64_t crc
      pBuffer_->setPosition(location_ + (sizeof(amps_uint32_t) * 2)
                            + (sizeof(amps_uint64_t) * 2) );
      // Read old chain header which uses same order, but not
      // as many bytes (everything is 32bit):
      // operation, commandIdLen, correlationIdLen,
      // expirationLen, sowKeyLen, topicLen, flag, ackTypes
      BlockChainHeader chainHeader;
      pBuffer_->copyBytes((char*)&chainHeader,
                          sizeof(amps_uint32_t) * 8);
      // Rewrite the header and chain header for first Block.
      pBuffer_->setPosition(location_);
      pBuffer_->putBytes((const char*)&blockHeader_, sizeof(BlockHeader));
      pBuffer_->putBytes((const char*)&chainHeader, sizeof(BlockChainHeader));
    }

    void chooseCRC(bool isFile)
    {
      if (!isFile)
      {
        _crc = noOpCRC;
        return;
      }

#ifndef AMPS_SSE_42
      _crc = AMPS::CRC<0>::crcNoSSE;
#else
      if (AMPS::CRC<0>::isSSE42Enabled())
      {
        _crc = AMPS::CRC<0>::crc;
      }
      else
      {
        _crc = AMPS::CRC<0>::crcNoSSE;
      }
#endif
    }

    static amps_uint64_t noOpCRC(const char*, size_t, amps_uint64_t)
    {
      return 0;
    }

  protected:
    mutable BlockStore     _blockStore;
  private:
    // Block used to hold metadata, currently:
    // the last persisted
    Block*                 _metadataBlock;
    // Highest sequence that has been discarded
    amps_uint64_t          _maxDiscarded;
    // Track the assigned sequence numbers
#if __cplusplus >= 201103L || _MSC_VER >= 1900
    std::atomic<amps_uint64_t> _lastSequence;
#else
    volatile amps_uint64_t _lastSequence;
#endif
    // Track how many messages are stored
    AMPS_ATOMIC_TYPE _stored;

    // Message used for doing replay
    Message                _message;

    typedef amps_uint64_t (*CRCFunction)(const char*, size_t, amps_uint64_t);

    // Function used to calculate the CRC if one is used
    CRCFunction     _crc;

  };

}

#endif

