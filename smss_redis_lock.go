package dgmq

import (
	redisdk "github.com/darwinOrg/go-redis"
	"github.com/google/uuid"
	"github.com/rolandhe/smss-client/dlock"
	"github.com/rolandhe/smss-client/logger"
	"sync"
	"sync/atomic"
	"time"
)

const (
	lockedLife     = time.Second * 30
	tryLockTimeout = time.Second * 10
	leaseInterval  = time.Second * 25
)

const luaExtendScript = `
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("expire", KEYS[1], ARGV[2])
    else
        return 0
    end
`
const luaReleaseScript = `
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
`

type redisLocker struct {
	notSupportLua   bool
	runInMainThread bool

	st *state
}

// NewSimpleRedisSubLock 创建生成环境中的锁, 所有锁的操作在一个goroutine中执行
//
//	notSupportLua 是否支持lua脚本,一些类redis的产品不支持lua，比如 pika
func NewSimpleRedisSubLock(notSupportLua bool) dlock.SubLock {
	return NewRedisSubLock(notSupportLua)
}

// NewSimpleRedisSubLockInMainThread 与NewSimpleRedisSubLock类似，支持有关所的操作在当前的主goroutine中执行，一般用于测试
func NewSimpleRedisSubLockInMainThread(notSupportLua bool) dlock.SubLock {
	return NewRedisSubLockInMainThread(notSupportLua)
}

// NewRedisSubLock 创建redis 锁, 需要指定创建redis客户端的工厂方法，与NewSimpleRedisSubLock类似，所有锁的操作在一个goroutine中执行
func NewRedisSubLock(notSupportLua bool) dlock.SubLock {
	return newRedisSubLock(notSupportLua, false)
}

// NewRedisSubLockInMainThread 与NewRedisSubLock类似，只是锁操作当当前主goroutine内运行
func NewRedisSubLockInMainThread(notSupportLua bool) dlock.SubLock {
	return newRedisSubLock(notSupportLua, true)
}

func newRedisSubLock(notSupportLua, runInMainThread bool) dlock.SubLock {
	return &redisLocker{
		notSupportLua:   notSupportLua,
		runInMainThread: runInMainThread,
	}
}

func (r *redisLocker) LockWatcher(key string, watcherFunc func(event dlock.WatchState)) error {
	r.st = &state{
		key:                  key,
		value:                uuid.New().String(),
		shutdownChan:         make(chan struct{}),
		waitShutdownComplete: make(chan struct{}),
	}

	if r.runInMainThread {
		r.lock(watcherFunc)
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		r.lock(watcherFunc)
		watcherFunc(dlock.LockerShutdown)
		close(r.st.waitShutdownComplete)
		logger.Infof("lock goroutine exit")
	}()
	wg.Wait()
	return nil
}

func (r *redisLocker) Shutdown() {
	r.st.shutdownState.Store(true)
	close(r.st.shutdownChan)
	<-r.st.waitShutdownComplete
	logger.Infof("Shutdown complate")
}

func (r *redisLocker) release(key string, value string, canRm bool) bool {
	if r.notSupportLua {
		if !canRm {
			return false
		}
		_, err := redisdk.Del(key)
		logger.Infof("redisLocker release(del) %s,err:%v", key, err)
		if err != nil {
			return false
		}
		return true
	}
	val, err := redisdk.Eval(luaReleaseScript, []string{key}, value)
	logger.Infof("redisLocker release(Eval script) %s,err:%v", key, err)
	if err != nil {
		return false
	}
	return val.(int64) == 1
}

func (r *redisLocker) lock(watcherFunc func(event dlock.WatchState)) {
	st := r.st
	sm := 0

	for !st.shutdownState.Load() {
		if sm == 0 {
			st.record(r.notSupportLua)
			ok, _ := redisdk.SetNX(st.key, st.value, lockedLife)
			timeout := tryLockTimeout
			if ok {
				timeout = leaseInterval
				watcherFunc(dlock.Locked)
				sm = 1
			} else {
				st.resetExpire(r.notSupportLua)
				watcherFunc(dlock.LockTimeout)
			}
			sleep(timeout, st.shutdownChan)
			continue
		}
		if sm == 1 {
			timeout := leaseInterval
			st.resetExpire(r.notSupportLua)
			if !r.lease(st.key, st.value) {
				watcherFunc(dlock.LostLock)
				timeout = tryLockTimeout
				sm = 0
			} else {
				st.record(r.notSupportLua)
				watcherFunc(dlock.Leased)
			}
			sleep(timeout, st.shutdownChan)
			continue
		}
	}
	r.release(st.key, st.value, st.canRemove(r.notSupportLua))
	logger.Infof("release redis client and locker")
}

func sleep(d time.Duration, shutdownChan chan struct{}) {
	select {
	case <-shutdownChan:
	case <-time.After(d):
	}
}

func (r *redisLocker) lease(key, value string) bool {
	if r.notSupportLua {
		v, err := redisdk.Get(key)
		if err != nil {
			return false
		}
		if v != value {
			return false
		}
		cmdRet, err := redisdk.Expire(key, lockedLife)
		if err != nil {
			return false
		}
		return cmdRet
	}
	val, err := redisdk.Eval(luaExtendScript, []string{key}, value, lockedLife.Seconds())
	if err != nil {
		return false
	}
	return val.(int64) == 1
}

type state struct {
	key   string
	value string

	shutdownState        atomic.Bool
	shutdownChan         chan struct{}
	waitShutdownComplete chan struct{}

	expiredAt int64
}

func (s *state) record(notLua bool) {
	if !notLua {
		return
	}
	s.expiredAt = time.Now().Add(lockedLife).UnixMilli()
}

func (s *state) resetExpire(notLua bool) {
	if !notLua {
		return
	}
	s.expiredAt = 0
}

func (s *state) canRemove(notLua bool) bool {
	if !notLua {
		return false
	}
	return s.expiredAt-time.Now().UnixMilli() > 5000
}
