package common

import "sync"

type IntExtLock struct {
	internal *sync.RWMutex
	external *sync.RWMutex
	mod      *sync.Mutex
}

func NewIntExtLock() *IntExtLock {
	return &IntExtLock{internal: &sync.RWMutex{}, external: &sync.RWMutex{}, mod: &sync.Mutex{}}
}

func (lock *IntExtLock) RIntLock() {
	lock.mod.Lock()
	lock.external.Lock()
	lock.internal.RLock()
	lock.external.Unlock()
	lock.mod.Unlock()
}

func (lock *IntExtLock) RIntUnlock() {
	lock.internal.RUnlock()
}

func (lock *IntExtLock) IntLock() {
	lock.mod.Lock()
	lock.external.Lock()
	lock.internal.Lock()
	lock.external.Unlock()
	lock.mod.Unlock()
}

func (lock *IntExtLock) IntUnlock() {
	lock.internal.Unlock()
}

func (lock *IntExtLock) RExtLock() {
	lock.mod.Lock()
	//fmt.Println("modlock")
	lock.internal.Lock()
	//fmt.Println("intlock")
	lock.external.RLock()
	lock.internal.Unlock()
	lock.mod.Unlock()
}

func (lock *IntExtLock) RExtUnlock() {
	lock.external.RUnlock()
}

func (lock *IntExtLock) ExtLock() {
	lock.mod.Lock()
	lock.internal.Lock()
	lock.external.RLock()
	lock.internal.Unlock()
	lock.mod.Unlock()
}

func (lock *IntExtLock) ExtUnlock() {
	lock.external.Unlock()
}
