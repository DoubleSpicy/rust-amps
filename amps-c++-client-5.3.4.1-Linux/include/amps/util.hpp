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
#ifndef __AMPS_UTIL_HPP_
#define __AMPS_UTIL_HPP_
#define _AMPS_SKIP_AMPSPLUSPLUS
#include <amps/amps.h>
#undef _AMPS_SKIP_AMPSPLUSPLUS
#include <amps/ampsuri.h>
#include <stdlib.h>
#include <ctype.h>
#include <map>
#include <stdexcept>

#ifdef _WIN32
  #include <WinSock2.h>
  #define OFF_T size_t
#else
  #include <netdb.h>
  #include <netinet/ip.h>
  #include <errno.h>
  #include <pthread.h>
  #include <unistd.h>
  #include <sys/time.h>
  #define OFF_T off_t
#endif

#ifndef _WIN32
  #include <inttypes.h>
extern "C" {
  #include <amps/amps_impl.h>
}
#endif
#if defined(sun)
  #include <sys/atomic.h>
#endif

#if __cplusplus >= 201100L || _MSC_VER >= 1900
#include <atomic>
  #define AMPS_ATOMIC_TYPE_8 std::atomic<char>
  #define AMPS_FETCH_AND_8(x, y) (x)->fetch_and(y)
  #define AMPS_FETCH_OR_8(x, y) (x)->fetch_or(y)
#if defined(_WIN32 )
  #define AMPS_ATOMIC_BASE_TYPE LONG64
  #define AMPS_ATOMIC_TYPE std::atomic<LONG64>
#else
  #define AMPS_ATOMIC_BASE_TYPE long
  #define AMPS_ATOMIC_TYPE std::atomic<long>
#endif
  #define AMPS_FETCH_ADD(x, y) (x)->fetch_add(y)
  #define AMPS_FETCH_SUB(x, y) (x)->fetch_sub(y)
  #define AMPS_FETCH_AND(x, y) (x)->fetch_and(y)
  #define AMPS_FETCH_OR(x, y) (x)->fetch_or(y)
#elif defined(_WIN32 )
  #define AMPS_ATOMIC_TYPE_8 volatile char
  #define AMPS_FETCH_AND_8(ptr, value) InterlockedAnd8((char volatile*)(ptr), (char)(value))
  #define AMPS_FETCH_OR_8(ptr, value) InterlockedOr8((char volatile*)(ptr), (char)(value))
  #define AMPS_ATOMIC_BASE_TYPE LONG64
  #define AMPS_ATOMIC_TYPE volatile LONG64
  #define AMPS_FETCH_ADD(ptr, value) InterlockedExchangeAdd64((LONG64 volatile*)(ptr), (LONG64)(value))
  #define AMPS_FETCH_SUB(ptr, value) InterlockedExchangeAdd64((LONG64 volatile*)(ptr), (LONG64)(-1 * (value)))
  #define AMPS_FETCH_AND(ptr, value) InterlockedAnd64((LONG64 volatile*)(ptr), (LONG64)(value))
  #define AMPS_FETCH_OR(ptr, value) InterlockedOr64((LONG64 volatile*)(ptr), (LONG64)(value))
#elif defined(sun)
  #define AMPS_ATOMIC_TYPE_8 volatile char
  #define AMPS_FETCH_AND_8(x, y) atomic_and_8_nv((volatile char*)(x), (y))
  #define AMPS_FETCH_OR_8(x, y) atomic_or_8_nv((volatile char*)(x), (y))
  #define AMPS_ATOMIC_BASE_TYPE unsigned long
  #define AMPS_ATOMIC_TYPE volatile unsigned long
  #define AMPS_FETCH_ADD(x, y) atomic_add_long_nv((volatile unsigned long*)(x), (y))
  #define AMPS_FETCH_SUB(x, y) atomic_add_long_nv((volatile unsigned long*)(x), -1*(y))
  #define AMPS_FETCH_AND(x, y) atomic_and_ulong_nv((volatile unsigned long*)(x), (y))
  #define AMPS_FETCH_OR(x, y) atomic_or_ulong_nv((volatile unsigned long*)(x), (y))
#else
  #define AMPS_ATOMIC_TYPE_8 volatile char
  #define AMPS_FETCH_AND_8(x, y) __sync_fetch_and_and((x), y)
  #define AMPS_FETCH_OR_8(x, y) __sync_fetch_and_or((x), y)
  #define AMPS_ATOMIC_BASE_TYPE long
  #define AMPS_ATOMIC_TYPE volatile long
  #define AMPS_FETCH_ADD(x, y) __sync_fetch_and_add((x), y)
  #define AMPS_FETCH_SUB(x, y) __sync_fetch_and_sub((x), y)
  #define AMPS_FETCH_AND(x, y) __sync_fetch_and_and((x), y)
  #define AMPS_FETCH_OR(x, y) __sync_fetch_and_or((x), y)
#endif

#define AMPS_DEFAULT_MIN_VERSION 99999999
namespace
{
  const amps_uint64_t AMPS_DEFAULT_SERVER_VERSION = 9900990009900099;
}
///
/// Simple wrapper around a CRITICAL_SECTION or pthread_mutex_t,
/// with acquire/release and wait/signalAll support.
///

namespace AMPS
{
  class Mutex
  {
    // Not implemented.
    Mutex& operator=(const Mutex& rhs);
    Mutex(const Mutex& rhs);
#ifdef _WIN32
    CRITICAL_SECTION _lock;
    HANDLE _sem;
    int _waiters;
  public:
    Mutex()
    {
      InitializeCriticalSection(&_lock);
      _sem = CreateSemaphore(NULL, 0, LONG_MAX, NULL);
      _waiters = 0;
    }
    ~Mutex()
    {
      DeleteCriticalSection(&_lock);
      CloseHandle(_sem);
    }
    void acquireRead()
    {
      EnterCriticalSection(&_lock);
    }
    void releaseRead()
    {
      LeaveCriticalSection(&_lock);
    }
    void acquireWrite()
    {
      EnterCriticalSection(&_lock);
    }
    void releaseWrite()
    {
      LeaveCriticalSection(&_lock);
    }
    void wait()
    {
      ++_waiters;
      LeaveCriticalSection(&_lock);
      WaitForSingleObject(_sem, INFINITE);
      EnterCriticalSection(&_lock);
    }
    bool wait(long timeoutMillis)
    {
      ++_waiters;
      LeaveCriticalSection(&_lock);
      DWORD result = WaitForSingleObject(_sem, (int)timeoutMillis);
      EnterCriticalSection(&_lock);

      return WAIT_OBJECT_0 == result;
    }
    void signalAll()
    {
      if (_waiters)
      {
        BOOL rc = ReleaseSemaphore(_sem, _waiters, NULL);
        assert(rc);
        _waiters = 0;
      }
    }
#else
    pthread_mutex_t _lock;
    pthread_cond_t _condition;
  public:
    Mutex()
    {
      pthread_mutexattr_t attr;
      pthread_mutexattr_init(&attr);
      pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
      pthread_mutex_init(&_lock, &attr);
      pthread_cond_init(&_condition, NULL);
      pthread_mutexattr_destroy(&attr);
      amps_atfork_add((void*)&_lock, amps_mutex_pair_atfork);
    }
    ~Mutex()
    {
      amps_atfork_remove((void*)&_lock, amps_mutex_pair_atfork);
      pthread_mutex_destroy(&_lock);
      pthread_cond_destroy(&_condition);
    }
    void acquireRead()
    {
      pthread_mutex_lock(&_lock);
    }
    void releaseRead()
    {
      pthread_mutex_unlock(&_lock);
    }
    void acquireWrite()
    {
      acquireRead();
    }
    void releaseWrite()
    {
      releaseRead();
    }
    void wait()
    {
      pthread_cond_wait(&_condition, &_lock);
    }
    bool wait(long timeoutMillis)
    {
      struct timespec now;
      const long BILLION = 1000L * 1000L * 1000L;
      clock_gettime(CLOCK_REALTIME, &now);
      now.tv_sec += timeoutMillis / 1000;
      now.tv_nsec += (1000 * 1000 * (timeoutMillis % 1000));
      // tv_nsec may not exceed one billion, according to the man pages.
      now.tv_sec  += now.tv_nsec / BILLION;
      now.tv_nsec %= BILLION;
      return pthread_cond_timedwait(&_condition, &_lock, &now) == 0;
    }
    void signalAll()
    {
      pthread_cond_broadcast(&_condition);
    }
#endif
  };


///
/// Simple wrapper around the native reader/writer lock, on POSIX,
/// and just a plain critical section on Windows.
///
  class ReadersWriterLock
  {
    ReadersWriterLock(const ReadersWriterLock& rhs);
    ReadersWriterLock& operator= (const ReadersWriterLock& rhs);

#ifdef _WIN32
    CRITICAL_SECTION _lock;
#else
    pthread_rwlock_t _lock;
#endif
  public:
    ReadersWriterLock()
    {
#ifdef _WIN32
      InitializeCriticalSection(&_lock);
#else
      pthread_rwlock_init(&_lock, NULL);
#endif
    }
    ~ReadersWriterLock()
    {
#ifndef _WIN32
      pthread_rwlock_destroy(&_lock);
#else
      DeleteCriticalSection(&_lock);
#endif
    }
    void acquireRead()
    {
#ifdef _WIN32
      EnterCriticalSection(&_lock);
#else
      pthread_rwlock_rdlock(&_lock);
#endif
    }

    void releaseRead()
    {
#ifdef _WIN32
      LeaveCriticalSection(&_lock);
#else
      pthread_rwlock_unlock(&_lock);
#endif
    }

    void acquireWrite()
    {
#ifdef _WIN32
      EnterCriticalSection(&_lock);
#else
      pthread_rwlock_wrlock(&_lock);
#endif
    }
    void releaseWrite()
    {
      releaseRead();
    }

  };

///
///
/// Simple RAII-style class for automatic locking/unlocking
///
  template <class T>
  class Lock
  {
    T& _lock;
    Lock(const Lock<T>& rhs); // not implemented
    Lock<T>& operator=(const Lock<T>& rhs); // not implemented
  public:
    Lock(T& lock) : _lock(lock)
    {
      _lock.acquireRead();
    }
    ~Lock()
    {
      _lock.releaseRead();
    }
  };

///
///
/// Simple RAII-style class for automatic locking/unlocking
///
  template <class T>
  class LockRead
  {
    T& _lock;
    LockRead(const LockRead<T>& rhs); // not implemented
    LockRead<T>& operator=(const LockRead<T>& rhs); // not implemented
  public:
    LockRead(T& lock) : _lock(lock)
    {
      _lock.acquireRead();
    }
    ~LockRead()
    {
      _lock.releaseRead();
    }
  };

///
/// Simple RAII-style class for automatic locking/unlocking.
/// When used with a a ReadersWriterLock, takes an exclusive lock.
///
  template <class T>
  class LockWrite
  {
    T& _lock;
    LockWrite(const LockWrite<T>& rhs); // not implemented
    LockWrite<T>& operator=(const LockWrite<T>& rhs); // not implemented

  public:
    LockWrite(T& lock) :  _lock(lock)
    {
      _lock.acquireWrite();
    }
    ~LockWrite()
    {
      _lock.releaseWrite();
    }
  };

///
/// Simple RAII-style class for automatic unlocking/relocking
///
  template <class T>
  class Unlock
  {
    T& _lock;
    Unlock(const Unlock<T>& rhs); // not implemented
    Unlock<T>& operator=(const Unlock<T>& rhs); //ni

  public:
    Unlock(T& lock) : _lock(lock)
    {
      _lock.releaseRead();
    }
    ~Unlock()
    {
      _lock.acquireRead();
    }
  };

///
/// Simple RAII-style class for automatic unlocking/relocking
///
  template <class T>
  class UnlockRead
  {
    T& _lock;
    UnlockRead(const UnlockRead<T>& rhs); // not implemented
    UnlockRead<T>& operator=(const UnlockRead<T>& rhs); //ni

  public:
    UnlockRead(T& lock) : _lock(lock)
    {
      _lock.releaseRead();
    }
    ~UnlockRead()
    {
      _lock.acquireRead();
    }
  };

///
///
/// Lock guard that defers locking until requested
///
  template <class T>
  class DeferLock
  {
    T& _lock;
    bool _isLocked;
    DeferLock(const DeferLock<T>& rhs); // not implemented
    DeferLock<T>& operator=(const DeferLock<T>& rhs); // not implemented
  public:
    DeferLock(T& lock) : _lock(lock), _isLocked(false)
    {
    }
    bool isLocked(void) const
    {
      return _isLocked;
    }
    void lock(void)
    {
      _lock.acquireRead();
      _isLocked = true;
    }
    void unlock(void)
    {
      _lock.releaseRead();
      _isLocked = false;
    }
    ~DeferLock()
    {
      if (_isLocked)
      {
        _lock.releaseRead();
      }
    }
  };

///
/// Limited unique pointer implementation
///
  template<class T>
  class amps_unique_ptr
  {
  public:
    amps_unique_ptr() : _body(NULL) {;}
    amps_unique_ptr(T* t_) : _body(t_) {;}
    ~amps_unique_ptr()
    {
      delete _body;
    }
    T& operator=(T* t_)
    {
      reset(t_);
      return *_body;
    }

    void reset(T* t_)
    {
      delete _body;
      _body = t_;
    }

    T& operator*()
    {
      return *_body;
    }
    T* operator->()
    {
      return _body;
    }
    const T& operator*() const
    {
      return *_body;
    }
    const T* operator->() const
    {
      return _body;
    }

    operator bool() const
    {
      return _body != NULL;
    }

    T* get()
    {
      return _body;
    }
    const T* get() const
    {
      return _body;
    }
  private:
    T* _body;
  };

///
/// Simple reference-counted body class for AMPS.
///
  class RefBody
  {
    AMPS_ATOMIC_TYPE _refs;
  protected:
    // we don't want the compiler's version of this, nor should
    // refcounted bodies have one
    const RefBody& operator=(const RefBody&);
    RefBody(const RefBody&);
  public:
    RefBody() : _refs(0) {;}
    virtual ~RefBody() {;}
    void addRef()
    {
      AMPS_FETCH_ADD(&_refs, 1);
    }
    void removeRef()
    {
      // return of 1 means it was set to 0
      if (AMPS_FETCH_SUB(&_refs, 1) == 1)
      {
        destroy();
      }
    }
    virtual void destroy()
    {
      delete this;
    }
  };

///
/// Template for optionally reference counted handle on a RefBody.
///
  template<class T>
  class BorrowRefHandle
  {
    T* _body;
    bool _isRef;
  public:
    BorrowRefHandle() : _body(NULL), _isRef(false)
    {;}

    virtual ~BorrowRefHandle()
    {
      if (_isRef && _body != NULL)
      {
        _body->removeRef();
      }
      _body = NULL;
    }

    BorrowRefHandle(T* body, bool isRef)
    {
      if (isRef && body != NULL)
      {
        body->addRef();
      }
      _body = body;
      _isRef = isRef;
    }

    BorrowRefHandle(const BorrowRefHandle& rhs)
    {
      if (rhs._isRef && rhs._body != NULL)
      {
        rhs._body->addRef();
      }
      _body = rhs._body;
      _isRef = rhs._isRef;
    }

#if defined(__GXX_EXPERIMENTAL_CXX0X__) || _MSC_VER >= 1600
    BorrowRefHandle(BorrowRefHandle&& rhs)
      : _body(rhs._body)
      , _isRef(rhs._isRef)
    {
      rhs._body = NULL;
      rhs._isRef = false;
    }

    const BorrowRefHandle& operator=(BorrowRefHandle&& rhs)
    {
      if (this == &rhs)
      {
        return *this;
      }
      if (_isRef && _body != NULL)
      {
        _body->removeRef();
      }
      _body = rhs._body;
      _isRef = rhs._isRef;
      rhs._body = NULL;
      rhs._isRef = false;
      return *this;
    }
#endif // defined(__GXX_EXPERIMENTAL_CXX0X__) || _MSC_VER >= 1600

    const BorrowRefHandle& operator=(const BorrowRefHandle& rhs)
    {
      if (this == &rhs)
      {
        return *this;
      }
      T* body = _body;
      _body = rhs._body;
      if (rhs._isRef && rhs._body != NULL)
      {
        rhs._body->addRef();
      }
      if (_isRef && body != NULL)
      {
        body->removeRef();
      }
      _isRef = rhs._isRef;

      return *this;
    }

    const T& get() const
    {
      return *_body;
    }

    T& get()
    {
      return *_body;
    }

    T* operator->(void)
    {
      return _body;
    }

    const T* operator->(void) const
    {
      return _body;
    }

    bool isRef(void) const
    {
      return _isRef;
    }

    bool isValid() const
    {
      return (_body != NULL);
    }

    bool release(void)
    {
      T* body = _body;
      _body = NULL;
      if (_isRef && body != NULL)
      {
        return body->removeRef();
      }
      return false;
    }
  };


///
/// Template for reference counted handle on a RefBody.
///
  template<class T>
  class RefHandle
  {
    T* _body;
  public:
    RefHandle() : _body(NULL) {;}
    virtual ~RefHandle()
    {
      if (_body != NULL)
      {
        _body->removeRef();
      }
      _body = NULL;
    }

    RefHandle(T* body)
    {
      if (body != NULL)
      {
        body->addRef();
      }
      _body = body;
    }
    RefHandle(const RefHandle& rhs)
    {
      if (rhs._body != NULL)
      {
        rhs._body->addRef();
      }
      _body = rhs._body;
    }
#if defined(__GXX_EXPERIMENTAL_CXX0X__) || _MSC_VER >= 1600
    RefHandle(RefHandle&& rhs) : _body(rhs._body)
    {
      rhs._body = NULL;
    }

    const RefHandle& operator=(RefHandle&& rhs)
    {
      if (this == &rhs)
      {
        return *this;
      }
      if (_body != NULL)
      {
        _body->removeRef();
      }
      _body = rhs._body;
      rhs._body = NULL;
      return *this;
    }
#endif // defined(__GXX_EXPERIMENTAL_CXX0X__) || _MSC_VER >= 1600
    const RefHandle& operator=(const RefHandle& rhs)
    {
      if (this == &rhs)
      {
        return *this;
      }
      T* body = _body;
      _body = rhs._body;
      if (rhs._body != NULL)
      {
        rhs._body->addRef();
      }
      if (body != NULL)
      {
        body->removeRef();
      }

      return *this;
    }

    const T& get() const
    {
      return *_body;
    }
    T& get()
    {
      return *_body;
    }
    T* operator->(void)
    {
      return _body;
    }
    const T* operator->(void) const
    {
      return _body;
    }


    bool isValid() const
    {
      return (_body != NULL);
    }

  };

///
/// Class that uses RAII to flip a bool only during its lifetime.
///
  class FlagFlip
  {
  public:
#if __cplusplus >= 201100L || _MSC_VER >= 1900
    FlagFlip(std::atomic<bool>* pFlag_) : _pFlag(pFlag_)
#else
    FlagFlip(volatile bool* pFlag_) : _pFlag(pFlag_)
#endif
    {
      *_pFlag = !*_pFlag;
    }
    ~FlagFlip()
    {
      *_pFlag = !*_pFlag;
    }
  private:
#if __cplusplus >= 201100L || _MSC_VER >= 1900
    std::atomic<bool>* _pFlag;
#else
    volatile bool* _pFlag;
#endif
  };

///
/// Class that uses RAII to set/unset an 8bit atomic
///
  class AtomicFlagFlip
  {
  public:
    AtomicFlagFlip(AMPS_ATOMIC_TYPE_8* pCount_) : _pCount(pCount_)
    {
      AMPS_FETCH_OR_8(_pCount, 1);
    }
    ~AtomicFlagFlip()
    {
      AMPS_FETCH_AND_8(_pCount, 0);
    }
  private:
    AMPS_ATOMIC_TYPE_8* _pCount;
  };

///
/// Base class for all exceptions in AMPS.
/// May be constructed with text, or with a client handle,
/// in which case the exception text is made equal to the last
/// error message on that client.
///
  class AMPSException : public std::runtime_error
  {
  protected:
    amps_result _result;
  public:
    ///
    /// Construct an AMPSException from a text message.
    ///
    /// \param message_ The text of the error message.
    /// \param result_ The last amps_result you were given.
    AMPSException(const std::string& message_,
                  amps_result result_)
      : std::runtime_error(message_),
        _result(result_)
    {
    }

    ///
    /// Construct an AMPSException from the last error message
    /// on client.
    /// \param client_ The client to retrieve an error message from.
    /// \param result_ The result code you last retrieved from this client.
    ///
    AMPSException(amps_handle client_,
                  amps_result result_)
      : std::runtime_error(""),
        _result(result_)
    {
      char buffer[1024];
      amps_client_get_error(client_, buffer,
                            sizeof(buffer));
      buffer[sizeof(buffer) - 1] = '\0';
      (std::runtime_error&)*this = std::runtime_error(buffer);
    }
    virtual ~AMPSException() throw() {;}

    ///
    /// Returns a null-terminated string containing self's
    /// error message.
    ///
    const char* toString() const
    {
      return what();
    }

    ///
    /// Returns the actual name of the subclass thrown --
    /// useful when RTTI is disabled, but you'd still like to log
    /// the exact exception type.
    ///
    virtual const char* getClassName() const
    {
      return "AMPSException";
    }

    ///
    /// Constructs and throws the appropriate exception
    /// corresponding to a given result
    /// \param context_ The object to pass to the exception's constructor.
    /// \param result_ the result that should be used to determine the exception type.
    template <class T>
    static void throwFor(const T& context_, amps_result result_);
  };

#define AMPS_EX_TYPE(x, b) \
  class x : public b\
  {\
  public:\
    x(const std::string& message, amps_result result = AMPS_E_OK)\
      : b(message, result) {;}\
    x(amps_handle client, amps_result result)\
      : b(client, result) {;}\
    virtual const char* getClassName() const { return #x; }\
  };
  AMPS_EX_TYPE(CommandException, AMPSException)
  AMPS_EX_TYPE(ConnectionException, AMPSException)
  AMPS_EX_TYPE(UsageException, AMPSException)
  AMPS_EX_TYPE(StoreException, AMPSException)
  AMPS_EX_TYPE(MessageStreamFullException, AMPSException)
  AMPS_EX_TYPE(ConnectionRefusedException, ConnectionException)
  AMPS_EX_TYPE(DisconnectedException, ConnectionException)
  AMPS_EX_TYPE(AlreadyConnectedException, ConnectionException)
  AMPS_EX_TYPE(AuthenticationException, ConnectionException)
  AMPS_EX_TYPE(InvalidURIException, ConnectionException)
  AMPS_EX_TYPE(NameInUseException, ConnectionException)
  AMPS_EX_TYPE(NotEntitledException, ConnectionException)
  AMPS_EX_TYPE(ReconnectMaximumExceededException, ConnectionException)
  AMPS_EX_TYPE(RetryOperationException, ConnectionException)
  AMPS_EX_TYPE(TimedOutException, ConnectionException)
  AMPS_EX_TYPE(TransportTypeException, ConnectionException)
  AMPS_EX_TYPE(BadFilterException, CommandException)
  AMPS_EX_TYPE(BadSowKeyException, CommandException)
  AMPS_EX_TYPE(BadRegexTopicException, CommandException)
  AMPS_EX_TYPE(DuplicateLogonException, CommandException)
  AMPS_EX_TYPE(InvalidBookmarkException, CommandException)
  AMPS_EX_TYPE(InvalidOptionsException, CommandException)
  AMPS_EX_TYPE(InvalidOrderByException, CommandException)
  AMPS_EX_TYPE(InvalidSubIdException, CommandException)
  AMPS_EX_TYPE(InvalidTopicException, CommandException)
  AMPS_EX_TYPE(LogonRequiredException, CommandException)
  AMPS_EX_TYPE(MissingFieldsException, CommandException)
  AMPS_EX_TYPE(PublishException, CommandException)
  AMPS_EX_TYPE(SubscriptionAlreadyExistsException, CommandException)
  AMPS_EX_TYPE(SubidInUseException, CommandException)
  AMPS_EX_TYPE(UnknownException, CommandException)
  AMPS_EX_TYPE(PublishStoreGapException, StoreException)

  template <class T> // static
  void AMPSException::throwFor(const T& context_, amps_result result_)
  {
    switch (result_)
    {
    case AMPS_E_OK:
      return;
    case AMPS_E_COMMAND:
      throw CommandException(context_, result_);
    case AMPS_E_CONNECTION:
      throw ConnectionException(context_, result_);
    case AMPS_E_TOPIC:
      throw InvalidTopicException(context_, result_);
    case AMPS_E_FILTER:
      throw BadFilterException(context_, result_);
    case AMPS_E_RETRY:
      throw RetryOperationException(context_, result_);
    case AMPS_E_DISCONNECTED:
      throw DisconnectedException(context_, result_);
    case AMPS_E_CONNECTION_REFUSED:
      throw ConnectionRefusedException(context_, result_);
    case AMPS_E_URI:
      throw InvalidURIException(context_, result_);
    case AMPS_E_TRANSPORT_TYPE:
      throw TransportTypeException(context_, result_);
    case AMPS_E_USAGE:
      throw UsageException(context_, result_);
    default:
      throw AMPSException(context_, result_);
    }
  }

#if defined (_MSC_VER)

#ifdef _WIN32
#if _WIN32_WINNT < 0x0600
#define AMPS_START_TIMER(timeout) \
  __declspec(align(8)) LARGE_INTEGER timer_freq, timer_start, timer_end; \
  timer_freq.QuadPart = timer_start.QuadPart = timer_end.QuadPart = 0LL; \
  double timer_timeout = (double)timeout; \
  QueryPerformanceFrequency(&timer_freq); \
  double timer_ms_freq = timer_freq.QuadPart/1000.0; \
  QueryPerformanceCounter(&timer_start);

#define AMPS_CHECK_TIMER(x) \
  QueryPerformanceCounter(&timer_end); \
  x = (((timer_end.QuadPart - timer_start.QuadPart)/timer_ms_freq) > timer_timeout);

#define AMPS_RESET_TIMER(timedOut, timeout) \
  QueryPerformanceCounter(&timer_end); \
  timeout = (int)(timer_timeout - ((timer_end.QuadPart - timer_start.QuadPart)/timer_ms_freq)); \
  timedOut = (timeout <= 0);

#else
#define AMPS_START_TIMER(timeout) \
  ULONGLONG timer_start, timer_end; \
  ULONGLONG timer_timeout = (ULONGLONG)timeout; \
  timer_start = timer_end = 0LL; \
  timer_start = GetTickCount64();

#define AMPS_CHECK_TIMER(x) \
  timer_end = 0ULL; \
  while (timer_end == 0ULL) timer_end = GetTickCount64(); \
  x = ((timer_end - timer_start) > timer_timeout);

#define AMPS_RESET_TIMER(timedOut, timeout) \
  timer_end = 0ULL; \
  while (timer_end == 0ULL) timer_end = GetTickCount64(); \
  timeout = (long)(timer_timeout - (timer_end - timer_start)); \
  timedOut = (timeout <= 0);
#endif
#endif


  class Timer
  {
  public:
    Timer(double timeoutMillis_ = 0.0)
      : _timeout(timeoutMillis_)
    {
#if _WIN32_WINNT < 0x0600
      __declspec(align(8)) LARGE_INTEGER freq;
      QueryPerformanceFrequency(&freq);
      _freq = freq.QuadPart / 1000.0;
#endif
      reset();
    }
    void setTimeout(double timeoutMillis_)
    {
      _timeout = timeoutMillis_;
    }
    double getTimeout(void) const
    {
      return _timeout;
    }
    void start(void)
    {
#if _WIN32_WINNT < 0x0600
      QueryPerformanceCounter(&_start);
#else
      _start = 0ULL;
      while (_start == 0ULL)
      {
        _start = GetTickCount64();
      }
#endif
    }
    void reset(void)
    {
#if _WIN32_WINNT < 0x0600
      _start.QuadPart = 0;
#else
      _start = 0ULL;
      _end = 0ULL;
#endif
    }
    bool check(void)
    {
      if (_timeout == 0.0)
      {
        return false;  // -V550
      }
#if _WIN32_WINNT < 0x0600
      if (_start.QuadPart == 0)
      {
        QueryPerformanceCounter(&_start);
      }
      QueryPerformanceCounter(&_end);
      return (((_end.QuadPart - _start.QuadPart) / _freq) > _timeout);
#else
      while (_start == 0ULL)
      {
        _start = GetTickCount64();
      }
      _end = 0ULL;
      while (_end == 0ULL)
      {
        _end = GetTickCount64();
      }
      ULONGLONG elapsed = _end - _start;
      return ((elapsed * 1.0) > _timeout);
#endif
    }
    bool checkAndGetRemaining(double* remaining_)
    {
#if _WIN32_WINNT < 0x0600
      if (_timeout == 0.0)
      {
        return false;  // -V550
      }
      QueryPerformanceCounter(&_end);
      double elapsed = ((_end.QuadPart - _start.QuadPart) / _freq);
#else
      if (_timeout == 0.0)
      {
        *remaining_ = 1000.0;
        return false;
      }
      _end = 0ULL;
      while (_end == 0ULL)
      {
        _end = GetTickCount64();
      }
      ULONGLONG elapsed = _end - _start;
#endif
      *remaining_ = _timeout - elapsed;
      return *remaining_ <= 0.0;
    }
  private:
#if _WIN32_WINNT < 0x0600
    __declspec(align(8)) LARGE_INTEGER _start, _end;
    double _freq;
#else
    ULONGLONG _start, _end;
#endif
    double _timeout;
  };

#else
#define AMPS_START_TIMER(timeout) \
  struct timespec timer_start = {0, 0}; \
  long timer_timeout = timeout; \
  clock_gettime(CLOCK_REALTIME, &timer_start); \
  struct timespec timer_end = {timer_start.tv_sec, timer_start.tv_nsec};

#define AMPS_CHECK_TIMER(x) \
  clock_gettime(CLOCK_REALTIME, &timer_end); \
  if (timer_end.tv_nsec < timer_start.tv_nsec) \
  { \
    timer_end.tv_sec -= 1; \
    timer_end.tv_nsec += 1000000000; \
  } \
  x = ((((timer_end.tv_sec - timer_start.tv_sec) * 1000) + \
        ((timer_end.tv_nsec-timer_start.tv_nsec)/1000000)) > timer_timeout);

#define AMPS_RESET_TIMER(timedOut, timeout) \
  clock_gettime(CLOCK_REALTIME, &timer_end); \
  if (timer_end.tv_nsec < timer_start.tv_nsec) \
  { \
    timer_end.tv_sec -= 1; \
    timer_end.tv_nsec += 1000000000; \
  } \
  timeout = (int)((double)timer_timeout - \
                  ((((double)(timer_end.tv_sec - timer_start.tv_sec)) * 1000.0) + \
                   (((double)(timer_end.tv_nsec-timer_start.tv_nsec))/1000000.0))) + 1; \
  timedOut = (timeout <= 0);

  class Timer
  {
  public:
    Timer(double timeoutMillis_ = 0.0) : _timeout(timeoutMillis_)
    {
      reset();
    }
    void setTimeout(double timeoutMillis_)
    {
      _timeout = timeoutMillis_;
    }
    double getTimeout(void) const
    {
      return _timeout;
    }
    void start(void)
    {
      clock_gettime(CLOCK_REALTIME, &_start);
    }
    void reset(void)
    {
      _start.tv_sec = 0;
      _start.tv_nsec = 0;
      _end.tv_sec = 0;
      _end.tv_nsec = 0;
    }
    bool check(void)
    {
      if (_timeout == 0.0)
      {
        return false;  // -V550
      }
      if (_start.tv_sec == 0 && _start.tv_nsec == 0)
      {
        clock_gettime(CLOCK_REALTIME, &_start);
      }
      clock_gettime(CLOCK_REALTIME, &_end);
      return (((double)((_end.tv_sec - _start.tv_sec) * 1000) + \
               (((double)(_end.tv_nsec - _start.tv_nsec)) / 1000000.0)) > _timeout);
    }
    bool checkAndGetRemaining(double* remaining_)
    {
      if (_timeout == 0.0) // -V550
      {
        *remaining_ = 1000.0;
        return false;
      }
      clock_gettime(CLOCK_REALTIME, &_end);
      double elapsed = ((double)((_end.tv_sec - _start.tv_sec) * 1000) + \
                        (((double)(_end.tv_nsec - _start.tv_nsec)) / 1000000.0));
      *remaining_ = _timeout - elapsed;
      return *remaining_ <= 0.0;
    }
  private:
    struct timespec _start, _end;
    double _timeout;
  };
#endif

  /**
   * Wrapper class around amps_uri_parse. Construct with a URI and
   * then use the member functions to extract part values from the URI.
   */
  class URI
  {
  public:
    URI(const std::string& uri_)
      : _uri(uri_),
        _port(0),
        _isValid(false)
    {
      parse();
    }

    const std::string& uri(void) const
    {
      return _uri;
    }
    const std::string& host(void) const
    {
      return _host;
    }
    const std::string& transport(void) const
    {
      return _transport;
    }
    const std::string& user(void) const
    {
      return _user;
    }
    const std::string& password(void) const
    {
      return _password;
    }
    int port(void) const
    {
      return _port;
    }
    const std::string& protocol(void) const
    {
      return _protocol;
    }
    const std::string& messageType(void) const
    {
      return _messageType;
    }
    bool hasOption(const std::string& key_) const
    {
      return _map.find(key_) != _map.end();
    }
    const std::string& option(const std::string& key_)
    {
      return _map[key_];
    }
    bool isValid(void) const
    {
      return _isValid;
    }

    bool isTrue(const std::string& key_)
    {
      if (_map.find(key_) != _map.end())
      {
        const std::string& value = _map[key_];
        if (value.size() == 1)
        {
          return value == "T" || value == "t" || value == "1";
        }
        else if (value.size() == 4)
        {
          return value == "True" || value == "true" || value == "TRUE";
        }
      }
      return false;
    }

    int getInt(const std::string& key_)
    {
      if (_map.find(key_) != _map.end())
      {
        const std::string& value = _map[key_];
        return atoi(value.c_str());
      }
      return -1;
    }

  private:
    void parse(void)
    {
      amps_uri_state uriState;
      memset(&uriState, 0, sizeof(amps_uri_state));
      while (uriState.part_id < AMPS_URI_ERROR)
      {
        amps_uri_parse(_uri.c_str(), _uri.size(), &uriState);
        switch (uriState.part_id)
        {
        case AMPS_URI_TRANSPORT:
          _transport.assign(uriState.part, uriState.part_length);
          break;
        case AMPS_URI_USER:
          _user = _unescape(uriState.part, uriState.part_length);
          break;
        case AMPS_URI_PASSWORD:
          _password = _unescape(uriState.part, uriState.part_length);
          break;
        case AMPS_URI_HOST:
          _host.assign(uriState.part, uriState.part_length);
          break;
        case AMPS_URI_PORT:
        {
          std::string port(uriState.part, uriState.part_length);
          _port = atoi(port.c_str());
        }
        break;
        case AMPS_URI_PROTOCOL:
          _protocol.assign(uriState.part, uriState.part_length);
          break;
        case AMPS_URI_MESSAGE_TYPE:
          _messageType.assign(uriState.part, uriState.part_length);
          break;
        case AMPS_URI_OPTION_KEY:
          _lastKey.assign(uriState.part, uriState.part_length);
          break;
        case AMPS_URI_OPTION_VALUE:
          _map[_lastKey] = std::string(uriState.part, uriState.part_length);
          break;
        default:
          break;
        }
      }
      _isValid = uriState.part_id == AMPS_URI_END;
    }

    static char _hexval(char c)
    {
      if (c >= '0' && c <= '9')
      {
        return static_cast<char>(c - '0');
      }
      if (c >= 'a' && c <= 'f')
      {
        return static_cast<char>(0xa + c - 'a');
      }
      if (c >= 'A' && c <= 'F')
      {
        return static_cast<char>(0xa + c - 'A');
      }
      return 0;
    }

    std::string _unescape(const char* data_, size_t length_)
    {
      std::string result;
      for (const char* p = data_, *pe = data_ + length_; p < pe; ++p)
      {
        if (*p == '%' && pe - p >= 3)
        {
          result += (char)( (0x10 * _hexval(p[1])) + _hexval(p[2]) );
          p += 2;
        }
        else
        {
          result += *p;
        }
      }
      return result;
    }


    std::string _uri;
    std::string _transport;
    std::string _user;
    std::string _password;
    std::string _host;
    std::string _protocol;
    std::string _messageType;
    std::string _lastKey;

    std::map<std::string, std::string> _map;
    int         _port;
    bool        _isValid;
  };

/// Converts a string version, such as "3.8.1.5" into the same numeric
/// form used internally and returned by getServerVersion. This can
/// be used to do comparisons such as
/// client.getServerVersion() >= "3.8"
/// Conversion works with any string that starts with a number, and will
/// assume 0 for any portions of the version not present. So "4" will
/// return 4000000, equivalent to "4.0.0.0"
/// \param data_ The pointer the start of the character data to convert.
/// \param len_ The length of the character data to convert.
/// \param defaultVersion_ The value to return for unreleased versions.
/// \return The size_t equivalent of the version.
/// \throw CommandException If any characters other than . or 0-9 are found
  inline size_t convertVersionToNumber(const char* data_, size_t len_,
                                       size_t defaultVersion_ = AMPS_DEFAULT_MIN_VERSION)
  {
    if (len_ == 0)
    {
      return 0;
    }
    size_t result = 0;
    // Has a version, use it
    // named instead of numbered, default to defaultVersion_
    if (data_[0] < '0' || data_[0] > '9')
    {
      // Must be a named branch, assume minimum version
      return defaultVersion_;
    }
    else
    {
      size_t dots = 0;
      long lastDot = -1;
      for (size_t i = 0; dots < 4 && i < len_; ++i)
      {
        if (data_[i] == '.')
        {
          ++dots;
          result *= 10;
          if ((long)i - lastDot > 5)
          {
            throw CommandException("Too many digits between dots found translating version string.");
          }
          else if (i - (size_t)lastDot >= 4)
          {
            result *= 10;
            result += 99;
          }
          else
          {
            if (i - (size_t)lastDot == 3)
            {
              result += (size_t)(data_[i - 2] - '0');
            }
            result *= 10;
            result += (size_t)(data_[i - 1] - '0');
          }
          lastDot = (long)i;
        }
        else if (data_[i] < '0' || data_[i] > '9')
        {
          if (dots == 3 && i - (size_t)lastDot <= 5 // -V658
              && (long)i - lastDot > 1)
          {
            ++dots;
            result *= 10;
            if (i - (size_t)lastDot >= 4)
            {
              result *= 10;
              result += 99;
            }
            else
            {
              if (i - (size_t)lastDot == 3)
              {
                result += (size_t)(data_[i - 2] - '0');
              }
              result *= 10;
              result += (size_t)(data_[i - 1] - '0');
            }
          }
          else
          {
            throw CommandException("Invalid character found in version string");
          }
        }
        if (i == len_ - 1)
        {
          ++dots;
          result *= 10;
          if (i - (size_t)lastDot > 4)
          {
            throw CommandException("Too many digits between dots found translating version string.");
          }
          else if (i - (size_t)lastDot >= 3)
          {
            result *= 10;
            result += 99;
          }
          else
          {
            if (i - (size_t)lastDot == 2)
            {
              result += (size_t)(data_[i - 1] - '0');
            }
            result *= 10;
            result += (size_t)(data_[i] - '0');
          }
          lastDot = (long)i;
        }
      }
      // For shortened versions, need to add zeroes
      for (; dots < 4; ++dots)
      {
        result *= 100;
      }
    }
    return result;
  }

  class VersionInfo
  {
  public:
    /// Create a default VersionInfo.
    VersionInfo() : _versionString("default"), _versionNum(0)
    {
    }

    /// Create a VersionInfo to represent the given version string.
    /// \param version_ The version string to represent.
    VersionInfo(const char* version_)
      : _versionString(version_ ? version_ : "default")
      , _versionNum(0)
    {
    }

    /// Create a VersionInfo to represent the given version string.
    /// \param version_ The version string to represent.
    VersionInfo(const std::string& version_)
      : _versionString(version_)
      , _versionNum(0)
    {
    }

    /// Create a VersionInfo to represent the given version string.
    /// \param version_ The version to copy.
    VersionInfo(const VersionInfo& info_)
      : _versionString(info_._versionString)
      , _versionNum(info_._versionNum)
    { ; }

    /// Set the version string.
    /// \param version_ The version string to represent.
    void setVersion(const char* version_)
    {
      _versionString = version_;
    }

    /// Set the version string.
    /// \param version_ The version string to represent.
    void setVersion(const std::string& version_)
    {
      _versionString = version_;
    }

    /// Get the version string.
    /// \returns The version string.
    std::string getVersionString() const
    {
      return _versionString;
    }

    /// Get the version as a amps_uint64_t with 4 digits for
    /// major version, 4 digits for minor version, 5 digits for
    /// maintenance version, and 5 digits for patch version.
    /// \returns The version string as an unsigned 64-bit value.
    amps_uint64_t getVersionUint64() const
    {
      if (_versionNum == 0)
      {
        _versionNum = parseVersion(_versionString);
      }
      return _versionNum;
    }

    /// Get the version as a size_t with 2 digits each for major,
    /// minor, maintenance, and patch.
    /// \returns The version as a size_t with 2 digits each for major,
    /// minor, maintenance, and patch. This will use 99 for any value
    /// that is 99 or larger. This is how previous versions of the client
    /// represented server versions.
    size_t getOldStyleVersion() const
    {
      return convertVersionToNumber(_versionString.data(), _versionString.length());
    }

    const VersionInfo& operator=(const VersionInfo& info_)
    {
      _versionString = info_._versionString;
      _versionNum = info_._versionNum;
      return *this;
    }

    bool operator<(const VersionInfo& info_) const
    {
      return getVersionUint64() < info_.getVersionUint64();
    }

    bool operator<=(const VersionInfo& info_) const
    {
      return getVersionUint64() <= info_.getVersionUint64();
    }

    bool operator==(const VersionInfo& info_) const
    {
      return getVersionUint64() == info_.getVersionUint64();
    }

    bool operator!=(const VersionInfo& info_) const
    {
      return getVersionUint64() != info_.getVersionUint64();
    }

    bool operator>=(const VersionInfo& info_) const
    {
      return getVersionUint64() >= info_.getVersionUint64();
    }

    bool operator>(const VersionInfo& info_) const
    {
      return getVersionUint64() > info_.getVersionUint64();
    }

    /// Parse a version string into an amps_uint64_t with 4 digits for
    /// major version, 4 digits for minor version, 5 digits for
    /// maintenance version, and 5 digits for patch version.
    /// This will fill in 0's for any missing portions of the version
    /// string and will return the default server version for any
    /// string with non-numeric, non-dot characters. It will only
    /// parse the first 4 levels, if there is an additional dot it will
    /// stop parsing and return the value for the first 4 levels only.
    /// The return value for "5" would 500000000000000.
    /// The return value for "5.2.3.0.1" would 500020000300000.
    /// \param version_ The version string to parse.
    /// \returns The version string as an unsigned 64-bit value.
    static amps_uint64_t parseVersion(const std::string& version_)
    {
      const int MAXVALUES = 4;
      const int MAXDIGITS[] = { 4, 4, 5, 5 };

      amps_uint64_t versionNum = 0;
      if (version_.length() == 0 || !isdigit(version_[0]))
      {
        // Start with non-numeric is a special build
        return AMPS_DEFAULT_SERVER_VERSION;
      }
      int digits = 0;
      int values = 0;
      amps_uint64_t current = 0;
      for (std::string::const_iterator c = version_.begin(), e = version_.end(); c != e; ++c)
      {
        if (isdigit(*c))
        {
          if (++digits > MAXDIGITS[values])
          {
            // Too many digits, just return default
            return AMPS_DEFAULT_SERVER_VERSION;
          }
          current *= (amps_uint64_t)10;
          current += (amps_uint64_t)(*c - '0');
        }
        else
        {
          if (*c == '.')
          {
            versionNum += current;
            // Ninja releases add beyond 4 levels, treat them as the first 4
            if (++values >= MAXVALUES)
            {
              return versionNum;
            }
            // Move over for next current value
            for (int i = 0; i < MAXDIGITS[values]; ++i)
            {
              versionNum *= (amps_uint64_t)10;
            }
            digits = 0;
            current = 0;
          }
          else
          {
            // A non-numeric, non-dot means it's a special build
            return AMPS_DEFAULT_SERVER_VERSION;
          }
        }
      }
      versionNum += current;
      // Fill in missing 0's from last value and if necessary add more for missing parts of version.
      while (++values < MAXVALUES)
      {
        for (int i = 0; i < MAXDIGITS[values]; ++i)
        {
          versionNum *= (amps_uint64_t)10;
        }
      }
      return versionNum;
    }
  private:
    std::string _versionString;
    mutable amps_uint64_t _versionNum;
  };


}

#endif
