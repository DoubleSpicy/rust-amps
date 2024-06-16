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

#ifndef _MEMORYPUBLISHSTORE_H_
#define _MEMORYPUBLISHSTORE_H_

#include <amps/ampsplusplus.hpp>
#include <amps/BlockPublishStore.hpp>
#include <amps/MemoryStoreBuffer.hpp>

/// \file MemoryPublishStore.hpp
/// \brief Provides AMPS::MemoryPublishStore, a publish store that holds
/// messages in memory.

namespace AMPS
{
///
/// A StoreImpl implementation that uses MemoryStoreBuffer as its buffer to
/// hold published messages in memory. This store does not persist the
/// published messages, so this store cannot be used to guarantee message
/// publication if the application restarts.
  class MemoryPublishStore : public BlockPublishStore
  {
  public:
    ///
    /// Create a MemoryPublishStore with a specified initial capacity in bytes
    /// \param blockPerRealloc_ The number of blocks to grow by when capacity
    /// has been exceeded.
    /// \param errorOnPublishGap_ If true, PublishStoreGapException can be
    /// thrown by the store if the client logs onto a server that appears
    /// to be missing messages no longer held in the store.
    MemoryPublishStore(size_t blockPerRealloc_, bool errorOnPublishGap_ = false)
      : BlockPublishStore(new MemoryStoreBuffer(),
                          (amps_uint32_t)blockPerRealloc_,
                          false, errorOnPublishGap_)
    {
    }

  };//end MemoryPublishStore

}//end namespace AMPS

#endif //_MEMORYPUBLISHSTORE_H_

