
// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lease

import (
	"container/heap"
	"math"
	"time"

	"github.com/coreos/etcd/lease/leasepb"
	"github.com/coreos/etcd/mvcc/backend"
)

type expiryLessor struct {
	*lessor
}

func NewExpiryLessor(b backend.Backend, minLeaseTTL int64) Lessor {
	return newExpiryLessor(b, minLeaseTTL)
}

func newExpiryLessor(b backend.Backend, minLeaseTTL int64) *expiryLessor {

	l := &expiryLessor{
		leaseMap:    make(map[LeaseID]*Lease),
		itemMap:     make(map[LeaseItem]LeaseID),
		leaseHeap:   make(LeaseQueue, 0),
		b:           b,
		minLeaseTTL: minLeaseTTL,
		// expiredC is a small buffered chan to avoid unnecessary blocking.
		expiredC: make(chan []*Lease, 16),
		stopC:    make(chan struct{}),
		doneC:    make(chan struct{}),
	}
	l.initAndRecover()

	go l.runLoop()

	return l
}

func (le *expiryLessor) Grant(id LeaseID, ttl int64) (*Lease, error) {
	if id == NoLease {
		return nil, ErrLeaseNotFound
	}

	if ttl > MaxLeaseTTL {
		return nil, ErrLeaseTTLTooLarge
	}

	// TODO: when lessor is under high load, it should give out lease
	// with longer TTL to reduce renew load.
	l := &Lease{
		ID:      id,
		ttl:     ttl,
		itemSet: make(map[LeaseItem]struct{}),
		revokec: make(chan struct{}),
	}

	le.mu.Lock()
	defer le.mu.Unlock()

	if _, ok := le.leaseMap[id]; ok {
		return nil, ErrLeaseExists
	}

	if l.ttl < le.minLeaseTTL {
		l.ttl = le.minLeaseTTL
	}

	l.refresh(0)

	le.leaseMap[id] = l
	item := &LeaseWithTime{leaseId: l.ID, expiration: l.expiry.UnixNano()}
	heap.Push(&le.leaseHeap, item)
	l.persistWithExpiry(le.b)

	return l, nil
}

// Renew renews an existing lease. If the given lease does not exist or
// has expired, an error will be returned.
func (le *expiryLessor) Renew(id LeaseID) (int64, error) {
	le.mu.Lock()

	unlock := func() { le.mu.Unlock() }
	defer func() { unlock() }()

	if !le.isPrimary() {
		// forward renew request to primary instead of returning error.
		return -1, ErrNotPrimary
	}

	demotec := le.demotec

	l := le.leaseMap[id]
	if l == nil {
		return -1, ErrLeaseNotFound
	}

	if l.expired() {
		le.mu.Unlock()
		unlock = func() {}
		select {
		// A expired lease might be pending for revoking or going through
		// quorum to be revoked. To be accurate, renew request must wait for the
		// deletion to complete.
		case <-l.revokec:
			return -1, ErrLeaseNotFound
		// The expired lease might fail to be revoked if the primary changes.
		// The caller will retry on ErrNotPrimary.
		case <-demotec:
			return -1, ErrNotPrimary
		case <-le.stopC:
			return -1, ErrNotPrimary
		}
	}

	l.refresh(0)
	item := &LeaseWithTime{leaseId: l.ID, expiration: l.expiry.UnixNano()}
	heap.Push(&le.leaseHeap, item)
	l.persistWithExpiry(le.b)
	return l.ttl, nil
}

func (le *expiryLessor) Promote(extend time.Duration) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.demotec = make(chan struct{})
}

func (le *expiryLessor) Demote() {
	le.mu.Lock()
	defer le.mu.Unlock()

	if le.demotec != nil {
		close(le.demotec)
		le.demotec = nil
	}
}

func (le *expiryLessor) initAndRecover() {
	tx := le.b.BatchTx()
	tx.Lock()

	tx.UnsafeCreateBucket(leaseBucketName)
	_, vs := tx.UnsafeRange(leaseBucketName, int64ToBytes(0), int64ToBytes(math.MaxInt64), 0)
	// TODO: copy vs and do decoding outside tx lock if lock contention becomes an issue.
	for i := range vs {
		var lpb leasepb.Lease
		err := lpb.Unmarshal(vs[i])
		if err != nil {
			tx.Unlock()
			panic("failed to unmarshal lease proto item")
		}
		ID := LeaseID(lpb.ID)
		if lpb.TTL < le.minLeaseTTL {
			lpb.TTL = le.minLeaseTTL
		}
		le.leaseMap[ID] = &Lease{
			ID:  ID,
			ttl: lpb.TTL,
			// itemSet will be filled in when recover key-value pairs
			itemSet: make(map[LeaseItem]struct{}),
			expiry:  time.Unix(0, lpb.Expiry),
			revokec: make(chan struct{}),
		}
	}
	heap.Init(&le.leaseHeap)
	tx.Unlock()

	le.b.ForceCommit()
}

func (l *Lease) persistWithExpiry(b backend.Backend) {
	key := int64ToBytes(int64(l.ID))
	lpb := leasepb.Lease{ID: int64(l.ID), TTL: int64(l.ttl), Expiry: l.expiry.UnixNano()}
	val, err := lpb.Marshal()
	if err != nil {
		panic("failed to marshal lease proto item")
	}

	b.BatchTx().Lock()
	b.BatchTx().UnsafePut(leaseBucketName, key, val)
	b.BatchTx().Unlock()
}
