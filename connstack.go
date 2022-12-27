package memcached

import (
	"sync"
)

// connStack is thread-safe stack of connections.
type connStack struct {
	data  []memConn
	mu    sync.Mutex
	index int
}

// newStack instantiates new stack.
func newStack(size int) *connStack {
	return &connStack{
		data:  make([]memConn, size),
		index: 0,
	}
}

// push pushes a connection to stack and returns false in case the stack is full.
func (s *connStack) push(value memConn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.index >= len(s.data) {
		return false
	}
	s.data[s.index] = value
	s.index = s.index + 1
	return true
}

// pop retrieves a connection from stack and returns false in case the stack is empty.
func (s *connStack) pop() (memConn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.index == 0 {
		return nil, false
	}
	s.index = s.index - 1
	return s.data[s.index], true
}

func (s *connStack) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.index
}
