/*//////////////////////////////////////////////////////////////////////////
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
///////////////////////////////////////////////////////////////////////// */
#include <amps/amps_impl.h>
#ifndef _WIN32
#ifdef __APPLE__
#include <signal.h>
#endif
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#if __STDC_VERSION__ >= 201100
#include <stdatomic.h>
#endif
static pthread_once_t _amps_atfork_register_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t _amps_atfork_registry_mutex = PTHREAD_MUTEX_INITIALIZER;

const size_t _AMPS_ATFORK_DEFAULT_LARGE_BUCKET_COUNT = 4999;
const size_t _AMPS_ATFORK_DEFAULT_RESIZE             =    8;

void amps_cleanup_unlock_registry_mutex(void* unused)
{
  pthread_mutex_unlock(&_amps_atfork_registry_mutex);
}

struct _amps_atfork_bucket
{
#if __STDC_VERSION__ >= 201100
  atomic_uint_fast64_t       _size;
  atomic_uint_fast64_t       _capacity;
#else
  volatile size_t            _size;
  volatile size_t            _capacity;
#endif
  void**                     _array;
};

struct _amps_atfork_entry
{
  _amps_atfork_callback_function  callback;
  struct _amps_atfork_bucket*     buckets;
#if __STDC_VERSION__ >= 201100
  atomic_uint_fast64_t            maxBucket;
#else
  volatile size_t                 maxBucket;
#endif
  size_t                          capacity;
};

static struct _amps_atfork_entry* _amps_atfork_array = 0;
static size_t _amps_atfork_array_size                = 0;
static size_t _amps_atfork_array_capacity            = 0;
#ifdef AMPS_DEBUG_ATFORK
#if __STDC_VERSION__ >= 201100
static atomic_uint_fast64_t _amps_atfork_bucket_max_size  = 0;
static atomic_uint_fast64_t _amps_atfork_entries          = 0;
static atomic_uint_fast64_t _amps_atfork_max_entries      = 0;
#else
static volatile size_t _amps_atfork_bucket_max_size  = 0;
static volatile size_t _amps_atfork_entries          = 0;
static volatile size_t _amps_atfork_max_entries      = 0;
#endif
FILE* __amps_atfork_debug_file = 0;
#endif

void __amps_prefork(void);
void __amps_postfork_parent(void);
void __amps_postfork_child(void);

void __amps_atfork_mutex_init(void)
{
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutex_init(&_amps_atfork_registry_mutex, &attr);
  pthread_mutexattr_destroy(&attr);
  /* Every call to this must call unlock (and does) */
  pthread_mutex_lock(&_amps_atfork_registry_mutex);
#ifdef AMPS_DEBUG_ATFORK
  char dbgName[32];
  snprintf(dbgName, 32, "%datfork", (int)getpid());
  __amps_atfork_debug_file = fopen(dbgName, "a+");
#endif
#ifdef __APPLE__
  /* No SIGPIPE */
  signal(SIGPIPE, SIG_IGN);
#endif
}

void _amps_atfork_register(void)
{
  pthread_atfork(__amps_prefork, __amps_postfork_parent, __amps_postfork_child);
}

void _amps_atfork_init(void)
{
  pthread_once(&_amps_atfork_register_once, _amps_atfork_register);
  if (!_amps_atfork_array)
  {
    /* Init the main array */
    _amps_atfork_array = malloc(_AMPS_ATFORK_DEFAULT_RESIZE * sizeof(struct _amps_atfork_entry));
    memset(_amps_atfork_array, 0, _AMPS_ATFORK_DEFAULT_RESIZE * sizeof(struct _amps_atfork_entry));
    _amps_atfork_array_capacity = _AMPS_ATFORK_DEFAULT_RESIZE;
    _amps_atfork_array_size = 1; /* We're setting up first entry here */

    /* Init the first entry */
    _amps_atfork_array[0].callback = amps_mutex_pair_atfork;
    _amps_atfork_array[0].maxBucket = 0;
    _amps_atfork_array[0].capacity = _AMPS_ATFORK_DEFAULT_LARGE_BUCKET_COUNT;
    _amps_atfork_array[0].buckets = malloc(_AMPS_ATFORK_DEFAULT_LARGE_BUCKET_COUNT * sizeof(struct _amps_atfork_bucket));
    memset(_amps_atfork_array[0].buckets, 0, _AMPS_ATFORK_DEFAULT_LARGE_BUCKET_COUNT * sizeof(struct _amps_atfork_bucket));
  }
}

void __amps_atfork_dispatch(int code_)
{
  if (!_amps_atfork_array)
  {
    return;
  }
  for (size_t h = 0; h < _amps_atfork_array_size; ++h)
  {
    struct _amps_atfork_entry* pEntry = &_amps_atfork_array[h];
    if (!pEntry || !pEntry->capacity || !pEntry->buckets || !pEntry->callback)
    {
      continue;
    }
    for (size_t i = 0; i < pEntry->maxBucket; ++i)
    {
      struct _amps_atfork_bucket* pBucket = &pEntry->buckets[i];
      for (size_t j = 0; j < pBucket->_size; ++j)
      {
        if (pBucket->_array[j])
        {
          pEntry->callback(pBucket->_array[j], code_);
        }
      }
    }
  }
}

void __amps_prefork(void)
{
  pthread_mutex_lock(&_amps_atfork_registry_mutex);
  /* We don't currently do anything with this, so leaving commented */
  /* __amps_atfork_dispatch(0); */
}

void __amps_postfork_parent(void)
{
  /* We don't currently do anything with this, so leaving commented */
  /* __amps_atfork_dispatch(1); */
  pthread_mutex_unlock(&_amps_atfork_registry_mutex);
}

void __amps_postfork_child(void)
{
  __amps_atfork_dispatch(2);
  __amps_atfork_mutex_init();
  pthread_mutex_unlock(&_amps_atfork_registry_mutex);
}

void amps_mutex_pair_atfork(void* vpMutexCond_, int code_)
{
  switch (code_)
  {
  case 0:
    break;
  case 1:
    break;
  case 2:
    /* Reinitialize the lock in the forked child */
    {
      pthread_mutexattr_t attr;
      pthread_mutexattr_init(&attr);
      pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
      pthread_mutex_init((pthread_mutex_t*)vpMutexCond_, &attr);
      pthread_mutexattr_destroy(&attr);
      /* Reinitialize the condition in the forked child */
      pthread_cond_init((pthread_cond_t*)(vpMutexCond_ + sizeof(pthread_mutex_t)), NULL);
    }
    break;
  }
}

void amps_atfork_init(void)
{
  pthread_mutex_lock(&_amps_atfork_registry_mutex);
  pthread_cleanup_push(amps_cleanup_unlock_registry_mutex, NULL);
  _amps_atfork_init();
  pthread_cleanup_pop(0);
  pthread_mutex_unlock(&_amps_atfork_registry_mutex);
}

void amps_atfork_add(void* user_data_, _amps_atfork_callback_function callback_)
{
  if (!user_data_ || !callback_)
  {
    return;
  }
  pthread_mutex_lock(&_amps_atfork_registry_mutex);
  pthread_cleanup_push(amps_cleanup_unlock_registry_mutex, NULL);
  _amps_atfork_init();
#ifdef AMPS_DEBUG_ATFORK
  if (++_amps_atfork_entries > _amps_atfork_max_entries)
  {
     _amps_atfork_max_entries = _amps_atfork_entries;
  }
#endif
  /* Find the entry for this callback function or the next entry */
  struct _amps_atfork_entry* pEntry = 0;
  for (size_t i = 0; i < _amps_atfork_array_capacity; ++i)
  {
    if (!_amps_atfork_array[i].callback)
    {
      /* Not found, use this unused one */
      pEntry = &_amps_atfork_array[i];
      ++_amps_atfork_array_size;
      break;
    }
    if (_amps_atfork_array[i].callback == callback_)
    {
      /* Found it */
      pEntry = &_amps_atfork_array[i];
      if (!pEntry->capacity)
      {
        /* Special case if main callback got cleared, more buckets */
        pEntry->capacity = i ? (_AMPS_ATFORK_DEFAULT_RESIZE - 1) :_AMPS_ATFORK_DEFAULT_LARGE_BUCKET_COUNT;
        pEntry->buckets = malloc(pEntry->capacity * sizeof(struct _amps_atfork_bucket));
        memset(pEntry->buckets, 0, pEntry->capacity * sizeof(struct _amps_atfork_bucket));
        pEntry->maxBucket = 0;
#ifdef AMPS_DEBUG_ATFORK
        fprintf(__amps_atfork_debug_file, "ATFORK reallocate buckets for cb %p buckets %p capacity %lu size %lu\n", pEntry->callback, pEntry->buckets, pEntry->capacity, pEntry->maxBucket); fflush(__amps_atfork_debug_file);
#endif
      }
      break;
    }
  }
  if (!pEntry)
  {
    /* Resize the main array */
    struct _amps_atfork_entry* pNew = malloc((_amps_atfork_array_capacity + _AMPS_ATFORK_DEFAULT_RESIZE) * sizeof(struct _amps_atfork_entry));
    memcpy(pNew, _amps_atfork_array, _amps_atfork_array_capacity * sizeof(struct _amps_atfork_entry));
    memset(pNew + _amps_atfork_array_capacity, 0, _AMPS_ATFORK_DEFAULT_RESIZE * sizeof(struct _amps_atfork_entry));
    free(_amps_atfork_array);
    _amps_atfork_array = pNew;
    _amps_atfork_array_capacity += _AMPS_ATFORK_DEFAULT_RESIZE;
    /* Use the first newly created entry */
    pEntry = &_amps_atfork_array[_amps_atfork_array_size++];
  }
  if (!pEntry->callback)
  {
    pEntry->callback = callback_;
    pEntry->buckets = malloc((_AMPS_ATFORK_DEFAULT_RESIZE - 1) * sizeof(struct _amps_atfork_bucket));
    memset(pEntry->buckets, 0, (_AMPS_ATFORK_DEFAULT_RESIZE - 1) * sizeof(struct _amps_atfork_bucket));
    pEntry->capacity = _AMPS_ATFORK_DEFAULT_RESIZE - 1;
    pEntry->maxBucket = 0;
#ifdef AMPS_DEBUG_ATFORK
    fprintf(__amps_atfork_debug_file, "ATFORK allocate buckets for new cb %p buckets %p capacity %lu size %lu\n", pEntry->callback, pEntry->buckets, pEntry->capacity, pEntry->maxBucket); fflush(__amps_atfork_debug_file);
#endif
  }

  /* Now find the right bucket */
  size_t bucketNum = (size_t)user_data_ % pEntry->capacity;
  if (pEntry->maxBucket < bucketNum)
  {
    pEntry->maxBucket = bucketNum;
  }
  struct _amps_atfork_bucket* pBucket = &pEntry->buckets[bucketNum];
  if (pBucket->_size == pBucket->_capacity)
  {
#ifdef AMPS_DEBUG_ATFORK
    fprintf(__amps_atfork_debug_file, "ATFORK resize bucket %lu from array %p capacity %lu size %lu\n", bucketNum, pBucket->_array, pBucket->_capacity, pBucket->_size); fflush(__amps_atfork_debug_file);
#endif
    pBucket->_capacity += _AMPS_ATFORK_DEFAULT_RESIZE;
    /* Create new array, copy old array, then 0 the rest */
    void* pNew = malloc(pBucket->_capacity * sizeof(void*));
    if (pBucket->_size)
    {
      memcpy(pNew, pBucket->_array, pBucket->_size * sizeof(void*));
      memset(pNew + pBucket->_size, 0, _AMPS_ATFORK_DEFAULT_RESIZE * sizeof(void*));
      free(pBucket->_array);
    }
    else
    {
      memset(pNew, 0, _AMPS_ATFORK_DEFAULT_RESIZE * sizeof(void*));
    }
    pBucket->_array = pNew;
#ifdef AMPS_DEBUG_ATFORK
    fprintf(__amps_atfork_debug_file, "ATFORK resize bucket %lu to array %p capacity %lu size %lu\n", bucketNum, pBucket->_array, pBucket->_capacity, pBucket->_size); fflush(__amps_atfork_debug_file);
#endif
  }
  pBucket->_array[pBucket->_size++] = user_data_;
#ifdef AMPS_DEBUG_ATFORK
  if (pBucket->_size > _amps_atfork_bucket_max_size)
  {
    _amps_atfork_bucket_max_size = pBucket->_size;
    if (_amps_atfork_bucket_max_size > 8)
    {
      fprintf(__amps_atfork_debug_file, "ATFORK entries: %lu max entries: %lu bucket max size %lu\n", _amps_atfork_entries, _amps_atfork_max_entries, _amps_atfork_bucket_max_size);
      fflush(__amps_atfork_debug_file);
    }
  }
#endif

  pthread_cleanup_pop(0);
  pthread_mutex_unlock(&_amps_atfork_registry_mutex);
}

void amps_atfork_remove(void* user_data_, _amps_atfork_callback_function callback_)
{
  pthread_mutex_lock(&_amps_atfork_registry_mutex);
  pthread_cleanup_push(amps_cleanup_unlock_registry_mutex, NULL);

  /* Find the entry for this callback function */
  struct _amps_atfork_entry* pEntry = 0;
  for (size_t i = 0; i < _amps_atfork_array_size; ++i)
  {
    if (_amps_atfork_array[i].callback == callback_)
    {
      pEntry = &_amps_atfork_array[i];
      break;
    }
  }
  if (pEntry)
  {
    /* Now find the right bucket */
    size_t bucketNum = (size_t)user_data_ % pEntry->capacity;
    if (pEntry->maxBucket >= bucketNum)
    {
      struct _amps_atfork_bucket* pBucket = &pEntry->buckets[bucketNum];
      for (size_t i = 0; i < pBucket->_size; ++i)
      {
        if (pBucket->_array[i] == user_data_)
        {
#ifdef AMPS_DEBUG_ATFORK
          --_amps_atfork_entries;
          fprintf(__amps_atfork_debug_file, "ATFORK remove from bucket %lu array %p capacity %lu size %lu position %lu\n", bucketNum, pBucket->_array, pBucket->_capacity, pBucket->_size, i); fflush(__amps_atfork_debug_file);
#endif
          if (i != --pBucket->_size)
          {
#ifdef AMPS_DEBUG_ATFORK
            fprintf(__amps_atfork_debug_file, "ATFORK move %p from %lu to %lu\n", pBucket->_array[pBucket->_size], pBucket->_size, i); fflush(__amps_atfork_debug_file);
#endif
            pBucket->_array[i] = pBucket->_array[pBucket->_size];
          }
          pBucket->_array[pBucket->_size] = 0;
          if (pBucket->_size == 0UL && bucketNum == pEntry->maxBucket)
          {
            for ( ; pEntry->maxBucket; --pEntry->maxBucket)
            {
              pBucket = &pEntry->buckets[pEntry->maxBucket];
              if (!pBucket->_size)
              {
                if (pBucket->_array)
                {
                  free(pBucket->_array);
                  pBucket->_array = 0;
                  pBucket->_capacity = 0;
                }
#ifdef AMPS_DEBUG_ATFORK
                fprintf(__amps_atfork_debug_file, "ATFORK cleared bucket %lu array %p capacity %lu size %lu\n", pEntry->maxBucket, pBucket->_array, pBucket->_capacity, pBucket->_size); fflush(__amps_atfork_debug_file);
#endif
              }
              else
              {
                break;
              }
            }
            if (pEntry->maxBucket == 0)
            {
              pBucket = &pEntry->buckets[pEntry->maxBucket];
              if (!pBucket->_size)
              {
                if (pBucket->_array)
                {
                  free(pBucket->_array);
                  pBucket->_array = 0;
                  pBucket->_capacity = 0;
                }
                /* We've emptied everything */
#ifdef AMPS_DEBUG_ATFORK
                fprintf(__amps_atfork_debug_file, "ATFORK cleared bucket %lu array %p capacity %lu size %lu\n", pEntry->maxBucket, pBucket->_array, pBucket->_capacity, pBucket->_size); fflush(__amps_atfork_debug_file);
                fprintf(__amps_atfork_debug_file, "ATFORK IS EMPTY max entries: %lu bucket max size %lu\n", _amps_atfork_max_entries, _amps_atfork_bucket_max_size);
                fflush(__amps_atfork_debug_file);
                fclose(__amps_atfork_debug_file);
#endif
                pEntry->capacity = 0;
                free(pEntry->buckets);
                size_t capacity = 0;
                for (size_t i = 0; i < _amps_atfork_array_size && capacity == 0; ++i)
                {
                  capacity += (size_t)(_amps_atfork_array[i].capacity);
                }
                if (capacity == 0)
                {
                  free(_amps_atfork_array);
                  _amps_atfork_array = 0;
                  _amps_atfork_array_size = 0;
                  _amps_atfork_array_capacity = 0;
                }
              }
            }
          }
          break;
        }
      }
    }
  }

  pthread_cleanup_pop(0);
  pthread_mutex_unlock(&_amps_atfork_registry_mutex);
}

#else
AMPSDLL void amps_atfork_init(void)
{
}
AMPSDLL void amps_atfork_add(void* vp_, _amps_atfork_callback_function fn_)
{
}
AMPSDLL void amps_atfork_remove(void* vp_, _amps_atfork_callback_function fn_)
{
}
#endif

