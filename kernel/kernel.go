package kernel

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrNoExistantId            = errors.New("no existant id")
	ErrInconsistentTTL         = errors.New("inconsistent TTL")
	ErrInconsistentIdExistance = errors.New("inconsistent id existance")
)

type intMapOfIntMap map[int64]map[uint64]int64

func New() *Kernel {
	return &Kernel{
		idMapping:      make(map[uint64]int64),
		ttlToIdMapping: make(intMapOfIntMap),
	}
}

type Kernel struct {
	rwmu           sync.RWMutex
	mu             sync.Mutex
	idMapping      map[uint64]int64
	ttlToIdMapping intMapOfIntMap
}

func (k *Kernel) lock()    { k.mu.Lock() }
func (k *Kernel) unlock()  { k.mu.Unlock() }
func (k *Kernel) rlock()   { k.rwmu.RLock() }
func (k *Kernel) runlock() { k.rwmu.RUnlock() }

func (k *Kernel) Expire(id uint64) {
	k.SetTTL(id, -1)
}

func (k *Kernel) TTL(id uint64) (ttl int64, err error) {
	k.rlock()
	defer k.runlock()

	idValueTTL, exists := k.idMapping[id]
	if !exists {
		err = ErrNoExistantId
		return
	}

	// We need to ensure that we always have a copy of
	// the ttl also registered in k.ttlToIdMapping
	ttlMapping := k.ttlToIdMapping[idValueTTL]
	if ttlMapping == nil {
		err = ErrInconsistentIdExistance
		return
	}
	ttlValue, consistent := ttlMapping[id]
	if !consistent || ttlValue != idValueTTL {
		err = ErrInconsistentTTL
		return
	}

	return ttlValue, nil
}

func (k *Kernel) SetTTL(id uint64, ttl int64) {
	k.lock()
	defer k.unlock()

	k.idMapping[id] = ttl
	mapping := k.ttlToIdMapping[ttl]
	if mapping == nil {
		mapping = make(map[uint64]int64)
	}
	mapping[id] = ttl
	k.ttlToIdMapping[ttl] = mapping
}

// stopTheWorldPrune
func (k *Kernel) stopTheWorldPrune() {
	k.lock()
	defer k.unlock()

	currentTime := time.Now().Unix()
	deletionCandidates := []int64{}

	for ttl, _ := range k.ttlToIdMapping {
		if ttl < currentTime {
			deletionCandidates = append(deletionCandidates, ttl)
		}
	}

	idMapping := k.idMapping
	ttlToIdMapping := k.ttlToIdMapping

	for _, candidateTTL := range deletionCandidates {
		idMap := ttlToIdMapping[candidateTTL]
		for id, _ := range idMap {
			delete(idMapping, id)
		}
		delete(ttlToIdMapping, candidateTTL)
	}

	k.idMapping = idMapping
	k.ttlToIdMapping = ttlToIdMapping
}
