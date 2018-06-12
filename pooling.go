package dbflex

import (
	"context"
	"sync"
	"time"

	"github.com/eaciit/toolkit"
)

type DbPooling struct {
	sync.RWMutex
	size  int
	items []*PoolItem
	fnNew func() (IConnection, error)

	Timeout time.Duration
}

type PoolItem struct {
	sync.RWMutex
	conn IConnection
	used bool
}

func NewDbPooling(size int, fnNew func() (IConnection, error)) *DbPooling {
	dbp := new(DbPooling)
	dbp.size = size
	dbp.fnNew = fnNew
	dbp.Timeout = time.Second * 2
	return dbp
}

// Get new connection. If all connection is being used and number of connection is less than
// pool capacity, new connection will be spin off. If capabity has been max out. It will waiting for
// any connection to be released before timeout reach
func (p *DbPooling) Get() (*PoolItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.Timeout)
	defer cancel()

	cpi := make(chan *PoolItem)
	cerr := make(chan error)

	go func(ctx context.Context) {
		for _, pi := range p.items {
			if pi.IsFree() {
				pi.Use()
				cpi <- pi
			}
		}

		if len(p.items) < p.size {
			p.Lock()
			pi, err := p.newItem()
			if err != nil {
				cerr <- err
			}
			p.items = append(p.items, pi)
			pi.Use()
			p.Unlock()
			cpi <- pi
		}

		for done := false; !done; {
			select {
			case <-time.After(10 * time.Millisecond):
				for _, pi := range p.items {
					if pi.IsFree() {
						pi.Use()
						cpi <- pi
					}
				}

			case <-ctx.Done():
				done = true
			}
		}
	}(ctx)

	select {
	case pi := <-cpi:
		return pi, nil

	case err := <-cerr:
		return nil, toolkit.Errorf("unable to create new pool item. %s", err.Error())

	case <-ctx.Done():
		return nil, toolkit.Errorf("Pool size (%d) has been reached", p.size)
	}
}

// Count number of connection within connection pooling
func (p *DbPooling) Count() int {
	return len(p.items)
}

// Size number of connection can be hold within the connection pooling
func (p *DbPooling) Size() int {
	return p.size
}

// Close all connection within connection pooling
func (p *DbPooling) Close() {
	p.Lock()
	for _, pi := range p.items {
		pi.conn.Close()
	}

	p.items = []*PoolItem{}
	p.Unlock()
}

func (p *DbPooling) newItem() (*PoolItem, error) {
	conn, err := p.fnNew()
	if err != nil {
		return nil, toolkit.Errorf("unable to open connection for DB pool. %s", err.Error())
	}

	pi := &PoolItem{conn: conn, used: false}
	return pi, nil
}

func (pi *PoolItem) Release() {
	pi.Lock()
	pi.used = false
	pi.Unlock()
}

func (pi *PoolItem) IsFree() bool {
	free := false
	pi.RLock()
	free = !pi.used
	pi.RUnlock()

	return free
}

func (pi *PoolItem) Use() {
	pi.Lock()
	pi.used = true
	pi.Unlock()
}
