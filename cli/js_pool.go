package cli

import (
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
)

var (
	jsPoolMu     sync.Mutex
	jsConnPool   []*nats.Conn
	jsPoolInited bool
)

// InitJSConnPool creates `parallel` dedicated nats connections (if not already created).
// servers may be empty (nats.Connect will use default URL from options in natsOpts()).
// It is safe to call multiple times; initialization is only performed once.
func InitJSConnPool(servers string, parallel int) error {
	jsPoolMu.Lock()
	defer jsPoolMu.Unlock()

	if jsPoolInited {
		// already initialized
		return nil
	}
	if parallel <= 0 {
		parallel = 1
	}

	conns := make([]*nats.Conn, 0, parallel)
	for i := 0; i < parallel; i++ {
		nc, err := nats.Connect(servers, natsOpts()...)
		if err != nil {
			// on error, close any already-opened connections
			for _, c := range conns {
				_ = c.Drain()
				c.Close()
			}
			return fmt.Errorf("init js conn pool: connect %d: %w", i, err)
		}
		conns = append(conns, nc)
	}

	jsConnPool = conns
	jsPoolInited = true
	return nil
}

// GetJSConn returns a connection from the pool by index (round-robin using idx % len).
// Returns nil if the pool is not initialized.
func GetJSConn(idx int) *nats.Conn {
	jsPoolMu.Lock()
	defer jsPoolMu.Unlock()
	if len(jsConnPool) == 0 {
		return nil
	}
	return jsConnPool[idx%len(jsConnPool)]
}

// CloseJSConnPool drains and closes all pooled connections and resets the pool.
func CloseJSConnPool() {
	jsPoolMu.Lock()
	defer jsPoolMu.Unlock()
	for _, nc := range jsConnPool {
		_ = nc.Drain()
		nc.Close()
	}
	jsConnPool = nil
	jsPoolInited = false
}
