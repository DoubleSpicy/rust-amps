////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2012-2024 60East Technologies Inc., All Rights Reserved.
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
// is subject to the restrictions set forth by 60East Technologies Inc.
//
////////////////////////////////////////////////////////////////////////////

#ifndef _AMPS_SDK_AMPS_COMMON_HPP_
#define _AMPS_SDK_AMPS_COMMON_HPP_

#include <module/amps_module.h>
#include <sys/types.h>
#include <stdint.h>
#include <string.h>
#include <phmap.h>

namespace amps_sdk
{
  // -----------------------------------------------------------------------------
  // phmap::flat_hash_map
  // -----------------------------------------------------------------------------
  //
  // An `phmap::flat_hash_map<K, V>` is an unordered associative container which
  // has been optimized for both speed and memory footprint in most common use
  // cases. Its interface is similar to that of `std::unordered_map<K, V>` with
  // the following notable differences:
  //
  // * Supports heterogeneous lookup, through `find()`, `operator[]()` and
  //   `insert()`, provided that the map is provided a compatible heterogeneous
  //   hashing function and equality operator.
  // * Invalidates any references and pointers to elements within the table after
  //   `rehash()`.
  // * Contains a `capacity()` member function indicating the number of element
  //   slots (open, deleted, and empty) within the hash map.
  // * Returns `void` from the `_erase(iterator)` overload.
  // -----------------------------------------------------------------------------

  template<class KeyType,
           class ValueType,
           class Hash  = phmap::Hash<KeyType>,
           class Equal = phmap::EqualTo<KeyType> >
  class flat_hash_map : public phmap::flat_hash_map<KeyType, ValueType, Hash, Equal>
  {
  public:
    using Base = typename phmap::flat_hash_map<KeyType, ValueType, Hash, Equal>;

    flat_hash_map(void)
    {;}

    using Base::Base;

    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::capacity;
    using Base::empty;
    using Base::max_size;
    using Base::size;
    using Base::clear;
    using Base::erase;
    using Base::_erase;
    using Base::insert;
    using Base::insert_or_assign;
    using Base::emplace;
    using Base::emplace_hint;
    using Base::try_emplace;
    using Base::extract;
    using Base::merge;
    using Base::swap;
    using Base::rehash;
    using Base::reserve;
    using Base::at;
    using Base::contains;
    using Base::count;
    using Base::equal_range;
    using Base::find;
    using Base::operator[];
    using Base::bucket_count;
    using Base::load_factor;
    using Base::max_load_factor;
    using Base::get_allocator;
    using Base::hash_function;
    using Base::hash;
    using Base::key_eq;

    size_t memoryByteCount(void) const
    {
      return phmap::priv::hashtable_debug_internal::HashtableDebugAccess<Base>::AllocatedByteSize(*this);
    }

  };

  // -----------------------------------------------------------------------------
  // phmap::node_hash_map
  // -----------------------------------------------------------------------------
  //
  // An `phmap::node_hash_map<K, V>` is an unordered associative container which
  // has been optimized for both speed and memory footprint in most common use
  // cases. Its interface is similar to that of `std::unordered_map<K, V>` with
  // the following notable differences:
  //
  // * Supports heterogeneous lookup, through `find()`, `operator[]()` and
  //   `insert()`, provided that the map is provided a compatible heterogeneous
  //   hashing function and equality operator.
  // * Contains a `capacity()` member function indicating the number of element
  //   slots (open, deleted, and empty) within the hash map.
  // * Returns `void` from the `erase(iterator)` overload.
  // -----------------------------------------------------------------------------

  template<class KeyType,
           class ValueType,
           class Hash  = phmap::Hash<KeyType>,
           class Equal = phmap::EqualTo<KeyType> >
  class node_hash_map : public phmap::node_hash_map<KeyType, ValueType, Hash, Equal>
  {
  public:
    using Base = typename phmap::node_hash_map<KeyType, ValueType, Hash, Equal>;

    node_hash_map(void)
    {;}

    using Base::Base;

    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::capacity;
    using Base::empty;
    using Base::max_size;
    using Base::size;
    using Base::clear;
    using Base::erase;
    using Base::_erase;
    using Base::insert;
    using Base::insert_or_assign;
    using Base::emplace;
    using Base::emplace_hint;
    using Base::try_emplace;
    using Base::extract;
    using Base::merge;
    using Base::swap;
    using Base::rehash;
    using Base::reserve;
    using Base::at;
    using Base::contains;
    using Base::count;
    using Base::equal_range;
    using Base::find;
    using Base::operator[];
    using Base::bucket_count;
    using Base::load_factor;
    using Base::max_load_factor;
    using Base::get_allocator;
    using Base::hash_function;
    using Base::hash;
    using Base::key_eq;

    size_t memoryByteCount(void) const
    {
      return phmap::priv::hashtable_debug_internal::HashtableDebugAccess<Base>::AllocatedByteSize(*this);
    }

  };

  // -----------------------------------------------------------------------------
  // phmap::flat_hash_set
  // -----------------------------------------------------------------------------
  // An `phmap::flat_hash_set<T>` is an unordered associative container which has
  // been optimized for both speed and memory footprint in most common use cases.
  // Its interface is similar to that of `std::unordered_set<T>` with the
  // following notable differences:
  //
  // * Supports heterogeneous lookup, through `find()`, `operator[]()` and
  //   `insert()`, provided that the set is provided a compatible heterogeneous
  //   hashing function and equality operator.
  // * Invalidates any references and pointers to elements within the table after
  //   `rehash()`.
  // * Contains a `capacity()` member function indicating the number of element
  //   slots (open, deleted, and empty) within the hash set.
  // * Returns `void` from the `_erase(iterator)` overload.
  // -----------------------------------------------------------------------------

  template<class KeyType,
           class Hash  = phmap::Hash<KeyType>,
           class Equal = phmap::EqualTo<KeyType> >
  class flat_hash_set : public phmap::flat_hash_set<KeyType, Hash, Equal>
  {
  public:
    using Base = typename phmap::flat_hash_set<KeyType, Hash, Equal>;

    flat_hash_set(void)
    {;}

    using Base::Base;

    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::capacity;
    using Base::empty;
    using Base::max_size;
    using Base::size;
    using Base::clear; // may shrink - To avoid shrinking `erase(begin(), end())`
    using Base::erase;
    using Base::insert;
    using Base::emplace;
    using Base::emplace_hint;
    using Base::extract;
    using Base::merge;
    using Base::swap;
    using Base::rehash;
    using Base::reserve;
    using Base::contains;
    using Base::count;
    using Base::equal_range;
    using Base::find;
    using Base::bucket_count;
    using Base::load_factor;
    using Base::max_load_factor;
    using Base::get_allocator;
    using Base::hash_function;
    using Base::hash;
    using Base::key_eq;

    size_t memoryByteCount(void) const
    {
      return phmap::priv::hashtable_debug_internal::HashtableDebugAccess<Base>::AllocatedByteSize(*this);
    }
  };

  // -----------------------------------------------------------------------------
  // phmap::node_hash_set
  // -----------------------------------------------------------------------------
  // An `phmap::node_hash_set<T>` is an unordered associative container which
  // has been optimized for both speed and memory footprint in most common use
  // cases. Its interface is similar to that of `std::unordered_set<T>` with the
  // following notable differences:
  //
  // * Supports heterogeneous lookup, through `find()`, `operator[]()` and
  //   `insert()`, provided that the map is provided a compatible heterogeneous
  //   hashing function and equality operator.
  // * Contains a `capacity()` member function indicating the number of element
  //   slots (open, deleted, and empty) within the hash set.
  // * Returns `void` from the `erase(iterator)` overload.
  // -----------------------------------------------------------------------------

  template<class KeyType,
           class Hash  = phmap::Hash<KeyType>,
           class Equal = phmap::EqualTo<KeyType> >
  class node_hash_set : public phmap::node_hash_set<KeyType, Hash, Equal>
  {
  public:
    using Base = typename phmap::node_hash_set<KeyType, Hash, Equal>;

    node_hash_set(void)
    {;}

    using Base::Base;

    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::capacity;
    using Base::empty;
    using Base::max_size;
    using Base::size;
    using Base::clear; // may shrink - To avoid shrinking `erase(begin(), end())`
    using Base::erase;
    using Base::insert;
    using Base::emplace;
    using Base::emplace_hint;
    using Base::extract;
    using Base::merge;
    using Base::swap;
    using Base::rehash;
    using Base::reserve;
    using Base::contains;
    using Base::count;
    using Base::equal_range;
    using Base::find;
    using Base::bucket_count;
    using Base::load_factor;
    using Base::max_load_factor;
    using Base::get_allocator;
    using Base::hash_function;
    using Base::hash;
    using Base::key_eq;

    size_t memoryByteCount(void) const
    {
      return phmap::priv::hashtable_debug_internal::HashtableDebugAccess<Base>::AllocatedByteSize(*this);
    }
  };

  // -----------------------------------------------------------------------------
  // phmap::parallel_node_hash_set
  // -----------------------------------------------------------------------------
  // An `phmap::parallel_node_hash_set<T>` is an unordered associative container which
  // has been optimized for both speed and memory footprint in most common use
  // cases. Its interface is similar to that of `std::unordered_set<T>` with the
  // following notable differences:
  //
  // * Supports heterogeneous lookup, through `find()`, `operator[]()` and
  //   `insert()`, provided that the map is provided a compatible heterogeneous
  //   hashing function and equality operator.
  // * Contains a `capacity()` member function indicating the number of element
  //   slots (open, deleted, and empty) within the hash set.
  // * Returns `void` from the `erase(iterator)` overload.
  //
  // Per docs, when a non-null lock type (intended to be std::mutex) is provided to
  // the template, lookups and functions that take a lambda (such as
  // modify_if, lazy_emplace, etc.) will be threadsafe for the container
  // using a lock at the bucket level, and a lock will be held while the lambda
  // runs. *However*, no locking is done on returned objects.
  // 
  // This also implies that parallelism is limited based on the number
  // of buckets available and how items hash into the buckets.
  // -----------------------------------------------------------------------------


   template <class T,
             class Hash  = phmap::Hash<T>,
             class Eq    = phmap::EqualTo<T>,
             class Alloc = phmap::Allocator<T>, // alias for std::allocator
             size_t N    = 4,                  // 2**N submaps
             class Mutex = phmap::NullMutex>   // use std::mutex to enable internal locks
  class parallel_node_hash_set : public phmap::parallel_node_hash_set<T, Hash, Eq, Alloc, N, Mutex>
  {
  public:
      using Base = typename phmap::parallel_node_hash_set<T, Hash, Eq, Alloc, N, Mutex>;

      using Base::Base;

      using Base::hash;
      using Base::subidx;
      using Base::subcnt;
      using Base::begin;
      using Base::cbegin;
      using Base::cend;
      using Base::end;
      using Base::capacity;
      using Base::empty;
      using Base::max_size;
      using Base::size;
      using Base::clear;
      using Base::erase;
      using Base::insert;
      using Base::emplace;
      using Base::emplace_hint;
      using Base::emplace_with_hash;
      using Base::emplace_hint_with_hash;
      using Base::extract;
      using Base::merge;
      using Base::swap;
      using Base::rehash;
      using Base::reserve;
      using Base::contains;
      using Base::count;
      using Base::equal_range;
      using Base::find;
      using Base::bucket_count;
      using Base::load_factor;
      using Base::max_load_factor;
      using Base::get_allocator;
      using Base::hash_function;
      using Base::key_eq;
      typename Base::hasher hash_funct() { return this->hash_function(); }
      void resize(typename Base::size_type hint) { this->rehash(hint); }
  };

  // -----------------------------------------------------------------------------
  // phmap::parallel_node_hash_map
  // -----------------------------------------------------------------------------
  //
  // An `phmap::parallel_node_hash_map<K, V>` is an unordered associative container which
  // has been optimized for both speed and memory footprint in most common use
  // cases. Its interface is similar to that of `std::unordered_map<K, V>` with
  // the following notable differences:
  //
  // * Supports heterogeneous lookup, through `find()`, `operator[]()` and
  //   `insert()`, provided that the map is provided a compatible heterogeneous
  //   hashing function and equality operator.
  // * Contains a `capacity()` member function indicating the number of element
  //   slots (open, deleted, and empty) within the hash map.
  // * Returns `void` from the `erase(iterator)` overload.
  //
  // Per docs, when a non-null lock type (intended to be std::mutex) is provided to
  // the template, lookups and functions that take a lambda (such as
  // modify_if, lazy_emplace, etc.) will be threadsafe for the container
  // using a lock at the bucket level, and a lock will be held while the lambda
  // runs. *However*, no locking is done on returned objects.
  //
  // This also implies that parallelism is limited based on the number
  // of buckets available and how items hash into the buckets.
  // -----------------------------------------------------------------------------


    template <class Key, class Value, 
              class Hash  = phmap::Hash<Key>,
              class Eq    = phmap::EqualTo<Key>,
              class Alloc = phmap::Allocator<
                            phmap::Pair<const Key, Value>>,
              size_t N    = 4,                  
              class Mutex = phmap::NullMutex>
  class parallel_node_hash_map : public phmap::parallel_node_hash_map<Key, Value, Hash, Eq, Alloc, N, Mutex>
  {
  public:
      using Base = typename phmap::parallel_node_hash_map<Key, Value, Hash, Eq, Alloc, N, Mutex>;
      
      using Base::Base;
      using Base::hash;
      using Base::subidx;
      using Base::subcnt;
      using Base::begin;
      using Base::cbegin;
      using Base::cend;
      using Base::end;
      using Base::capacity;
      using Base::empty;
      using Base::max_size;
      using Base::size;
      using Base::clear;
      using Base::erase;
      using Base::insert;
      using Base::insert_or_assign;
      using Base::emplace;
      using Base::emplace_hint;
      using Base::try_emplace;
      using Base::emplace_with_hash;
      using Base::emplace_hint_with_hash;
      using Base::try_emplace_with_hash;
      using Base::extract;
      using Base::merge;
      using Base::swap;
      using Base::rehash;
      using Base::reserve;
      using Base::at;
      using Base::contains;
      using Base::count;
      using Base::equal_range;
      using Base::find;
      using Base::operator[];
      using Base::bucket_count;
      using Base::load_factor;
      using Base::max_load_factor;
      using Base::get_allocator;
      using Base::hash_function;
      using Base::key_eq;
      typename Base::hasher hash_funct() { return this->hash_function(); }
      void resize(typename Base::size_type hint) { this->rehash(hint); }
};


} // namespace amps_sdk

#endif
