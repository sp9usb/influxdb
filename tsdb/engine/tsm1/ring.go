package tsm1

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cespare/xxhash"
)

type ring struct {
	partitions []*partition // The unique set of partitions in the ring.
	continuum  []*partition // A mapping of parition to location on the ring continuum.

	// Number of entries held within the ring. This is used to provide a
	// hint for allocating a []string to return all keys. It will not be
	// perfectly accurate since it doesn't consider adding duplicate keys,
	// or trying to remove non-existent keys.
	entryN int64
}

func newring(n int) (*ring, error) {
	if n <= 0 || n > 256 {
		return nil, fmt.Errorf("invalid number of paritions: %d", n)
	}

	r := ring{
		continuum: make([]*partition, 256), // maximum number of partitions.
	}

	// The trick here is to map N partitions to all points on the continuum,
	// such that the first eight bits of a given hash will map directly to one
	// of the N partitions.
	for i := 0; i < len(r.continuum); i++ {
		if (i == 0 || i%(256/n) == 0) && len(r.partitions) < n {
			r.partitions = append(r.partitions, &partition{store: make(map[string]*entry)})
		}
		r.continuum[i] = r.partitions[len(r.partitions)-1]
	}
	return &r, nil
}

func (r *ring) getPartition(key string) *partition {
	// Prevent allocation.
	k := *(*[]byte)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&key))))
	return r.continuum[int(uint8(xxhash.Sum64(k)))]
}

// entry returns the entry for the given key.
func (r *ring) entry(key string) (*entry, bool) {
	return r.getPartition(key).entry(key)
}

// entryOrCreate creates a new entry under key if one does not already exist.
func (r *ring) entryOrCreate(key string) *entry {
	return r.getPartition(key).entryOrCreate(key)
}

func (r *ring) entryOrCreateLockFree(key string) *entry {
	return r.getPartition(key).entryOrCreateLockFree(key)
}

// add adds an entry to the ring.
func (r *ring) add(key string, entry *entry) {
	r.getPartition(key).add(key, entry)
	atomic.AddInt64(&r.entryN, 1)
}

// remove deletes the entry for the given key.
func (r *ring) remove(key string) {
	r.getPartition(key).remove(key)
	if r.entryN > 0 {
		atomic.AddInt64(&r.entryN, -1)
	}
}

// keys returns all the keys from all partitions in the hash ring. The returned
// keys will be in order if sorted is true.
func (r *ring) keys(sorted bool) []string {
	keys := make([]string, 0, atomic.LoadInt64(&r.entryN))
	for _, p := range r.partitions {
		keys = append(keys, p.keys()...)
	}

	if sorted {
		sort.Strings(keys)
	}
	return keys
}

// apply applies the provided function to every entry in the ring under a read
// lock. The provided function will be called with each key and the
// corresponding entry. The first error encountered will be returned, if any.
func (r *ring) apply(f func(string, *entry) error) error {

	var (
		wg  sync.WaitGroup
		res = make(chan error, len(r.partitions))
	)

	for _, p := range r.partitions {
		wg.Add(1)

		go func(p *partition) {
			defer wg.Done()

			p.mu.RLock()
			for k, e := range p.store {
				if err := f(k, e); err != nil {
					res <- err
					p.mu.RUnlock()
					return
				}
			}
			p.mu.RUnlock()
		}(p)
	}

	wg.Wait()
	close(res)

	// Collect results.
	for err := range res {
		if err != nil {
			return err
		}
	}
	return nil
}

type partition struct {
	mu    sync.RWMutex
	store map[string]*entry
}

func (p *partition) entry(key string) (*entry, bool) {
	p.mu.RLock()
	e, ok := p.store[key]
	p.mu.RUnlock()
	return e, ok
}

func (p *partition) entryOrCreate(key string) *entry {
	p.mu.Lock()
	e, ok := p.store[key]
	if ok {
		p.mu.Unlock()
		return e
	}

	e = newEntry()
	p.store[key] = e
	p.mu.Unlock()
	return e
}

func (p *partition) entryOrCreateLockFree(key string) *entry {
	e, ok := p.store[key]
	if ok {
		return e
	}

	e = newEntry()
	p.store[key] = e
	return e
}

func (p *partition) add(key string, entry *entry) {
	p.mu.Lock()
	p.store[key] = entry
	p.mu.Unlock()
}

func (p *partition) remove(key string) {
	p.mu.Lock()
	delete(p.store, key)
	p.mu.Unlock()
}

func (p *partition) keys() []string {
	p.mu.RLock()
	keys := make([]string, 0, len(p.store))
	for k, _ := range p.store {
		keys = append(keys, k)
	}
	p.mu.RUnlock()
	return keys
}
