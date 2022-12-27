package memcached

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// defaultIdleConn is default number of idle connections.
const defaultIdleConn = 20

// internalBuffSize is size of a buffer used for reading network data.
const internalBuffSize = 1024

// defaultIdleConnectionTimeout is default value for memcached server connection timeout.
const defaultIdleConnectionTimeout = 2 * time.Minute

// ErrKeyNotFound returned when given key not found.
var ErrKeyNotFound = errors.New("key not found")

// Client is used for basic operations such as set and get.
type Client struct {
	addr        string
	pool        *connStack
	provider    connProvider
	idleTimeout time.Duration
	timeNow     func() time.Time
	profiler    Profiler
	connCounter uint32
}

// Response structure for operations such as set and get.
type Response struct {
	// Key is a unique key that has some value
	Key string
	// Data is the value associated with the key
	Data []byte
}

type connProvider interface {
	NewConn(address string) (memConn, error)
}

var (
	endResp = []byte{'E', 'N', 'D', '\r', '\n'}
	sep     = []byte{'\r', '\n'}
)

// NewClient instantiates a client with a given address in a form of host:port
// and maxIdleConn conn pool.
// Max idle connection specifies the max number of reused connections and should
// be greater than zero.
func NewClient(addr string, maxIdleConn int) (*Client, error) {
	if maxIdleConn < 1 {
		return nil, fmt.Errorf("maxIdleConn must be greater than zero")
	}
	return &Client{
		addr:        addr,
		pool:        newStack(maxIdleConn),
		provider:    &tcpConnProvider{},
		idleTimeout: defaultIdleConnectionTimeout,
		timeNow:     time.Now,
		profiler:    &nopProfiler{},
	}, nil
}

// Set sets value associated with a key. If the context contains timeout,
// a deadline for the connection will be used. It is up to the client to set the timeout.
// Returns an error in case NOT_STORED response received from the server or if
// any other error occurred such as connection error, etc.
func (c *Client) Set(ctx context.Context, key string, data []byte, expiration int) error {
	start := time.Now()
	defer func() { c.profiler.Report("set_duration", time.Since(start)) }()
	// preferably reuse existing conn per memcached protocol
	conn, err := c.acquireConn(c.addr)
	if err != nil {
		return err
	}
	defer c.releaseConn(conn)

	if deadline, ok := ctx.Deadline(); ok {
		_ = c.setConnDeadline(conn, deadline)
	}

	// data can be empty in which case length is 0
	var length int
	if data != nil {
		length = len(data)
	}

	// write command and data
	command := fmt.Sprintf("%v %v %v %v %v\r\n", "set", key, uint16(0), expiration, length)
	t := time.Now()
	if _, err := conn.Write([]byte(command)); err != nil {
		return fmt.Errorf("error write conn data: %v", err)
	}
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("error write conn data: %v", err)
	}
	if _, err := conn.Write([]byte("\r\n")); err != nil {
		return fmt.Errorf("error write conn data: %v", err)
	}
	c.profiler.Report("set_write_data", time.Since(t))

	// read response
	var mv maxValueCollector
	buf := make([]byte, internalBuffSize)
	end := 0
	t = time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if internalBuffSize == end {
			return fmt.Errorf("error read response: internal buffer overflow")
		}
		rt := time.Now()
		br, err := conn.Read(buf[end:])
		mv.Add(time.Since(rt))
		if err != nil {
			return fmt.Errorf("error read response: %v", err)
		}
		end += br
		if bytes.Contains(buf, sep) {
			break
		}
	}
	c.profiler.Report("set_read_response", time.Since(t))
	c.profiler.Report("set_read_one_max", mv.Max())

	result := string(buf[:end])

	switch result {
	case "STORED\r\n":
		return nil
	case "NOT_STORED\r\n":
		return fmt.Errorf("server NOT_STORED received: condition for set command isn't met")
	}

	return fmt.Errorf("set error: %v", result)
}

// Get returns the value associated with the key. If the context contains timeout,
// a deadline for the connection will be used. It is up to the client to set the timeout.
// Returns ErrKeyNotFound for non-existing key.
func (c *Client) Get(ctx context.Context, key string) (*Response, error) {
	start := time.Now()
	defer func() { c.profiler.Report("get_duration", time.Since(start)) }()
	const (
		ParseValueSection = 0
		ParseDataSection  = 1
		Complete          = 2
	)
	// preferably reuse existing conn per memcached protocol
	conn, err := c.acquireConn(c.addr)
	if err != nil {
		return nil, fmt.Errorf("error acquire conn: %v", err)
	}
	defer c.releaseConn(conn)

	if deadline, ok := ctx.Deadline(); ok {
		_ = c.setConnDeadline(conn, deadline)
	}

	// write command and data
	command := fmt.Sprintf("%v %v\r\n", "get", key)
	t := time.Now()
	if _, err := conn.Write([]byte(command)); err != nil {
		return nil, fmt.Errorf("error write conn data: %v", err)
	}
	c.profiler.Report("get_write_data", time.Since(t))

	response := Response{
		Key: key,
	}

	// read response VALUE part
	buf := make([]byte, internalBuffSize)
	end := 0     // position right after the data in the result array where new data can be written
	dataLen := 0 // contains data section length (without \r\n and END\r\n)
	t = time.Now()
	for step := ParseValueSection; step != Complete; {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context error: %v", ctx.Err())
		default:
		}
		if internalBuffSize == end && step == ParseValueSection {
			return nil, fmt.Errorf("error read response: internal buffer overflow")
		}

		br, err := conn.Read(buf[end:])
		if err != nil {
			return nil, fmt.Errorf("error read response: %v", err)
		}
		end += br

		if step == ParseValueSection {
			// analyze data - it can contain VALUE section and optionally part of the data section
			if bytes.Equal(buf[:end], endResp) {
				return nil, ErrKeyNotFound
			}
			if parts := split(buf[:end]); parts != nil {
				_, dataLen, err = parseValueResp(string(parts[0]))
				if err != nil {
					return nil, fmt.Errorf("error parse resp: %v", err)
				}
				buf = make([]byte, dataLen+len(sep)+len(endResp))
				end = copy(buf, parts[1])
				step = ParseDataSection
			}
		}
		if step == ParseDataSection {
			data := buf[:end]
			if len(data) < dataLen {
				continue
			}
			idx := bytes.LastIndex(data, endResp)
			if idx == -1 || idx < dataLen {
				continue
			}
			data = data[:idx]
			if bytes.HasSuffix(data, sep) {
				// make sure to only trim the last 'sep'
				response.Data = data[:len(data)-len(sep)]
			} else {
				response.Data = data
			}
			if response.Data == nil {
				response.Data = make([]byte, 0)
			}
			step = Complete
		}
	}
	c.profiler.Report("get_read_response", time.Since(t))

	return &response, nil
}

// SetProfiler sets a profiler to measure network io read/write times.
func (c *Client) SetProfiler(prof Profiler) {
	c.profiler = prof
}

// GetProfiler returns profiler.
func (c *Client) GetProfiler() Profiler {
	return c.profiler
}

// GetStats returns internal stats such as number of connections, etc.
func (c *Client) GetStats() Stats {
	return Stats{
		ConnPoolLen:   c.pool.len(),
		OpenConnCount: atomic.LoadUint32(&c.connCounter),
	}
}

func (c *Client) acquireConn(address string) (memConn, error) {
	t := time.Now()
	defer func() { c.profiler.Report("conn_acquire", time.Since(t)) }()
	// maxTime is time in the past which starts counting since
	// when a connection is still active on the server side.
	// If this connection was last used by the client any time
	// before it, the server has likely closed it by timeNow, and we
	// need to discard it
	maxTime := c.timeNow().Add(-c.idleTimeout)
	for {
		conn, ok := c.pool.pop()
		if !ok {
			return c.newConn(address)
		}
		if conn.GetLastUsed().After(maxTime) {
			// since we want to move to the next connection if
			// SetDeadline fails, unlike typical GoLang "if"
			// this doesn't check for err != nil
			if conn.SetDeadline(time.Time{}) == nil {
				return conn, nil
			}
		}
		_ = c.closeConn(conn) // ignore returned error since it is not essential to this function
	}
}

func (c *Client) releaseConn(conn memConn) error {
	t := time.Now()
	defer func() { c.profiler.Report("conn_release", time.Since(t)) }()
	// this connection may not be healthy. We don't want
	// to put it back in the pool
	if conn.LastErr() != nil {
		return c.closeConn(conn)
	}
	// set when this connection was last used
	// to properly close idle connections due to timeout
	conn.SetLastUsed(c.timeNow())
	// put connection back in the pool
	if ok := c.pool.push(conn); !ok {
		return c.closeConn(conn)
	}
	return nil
}

func (c *Client) newConn(address string) (memConn, error) {
	t := time.Now()
	newConn, err := c.provider.NewConn(address)
	d := time.Since(t)
	if err != nil {
		c.profiler.Report("conn_create_new_err", d)
	} else {
		atomic.AddUint32(&c.connCounter, 1)
		c.profiler.Report("conn_create_new", d)
	}
	return newConn, err
}

func (c *Client) closeConn(conn memConn) error {
	atomic.AddUint32(&c.connCounter, ^uint32(0)) // idiomatic decrement - https://pkg.go.dev/sync/atomic#AddUint32
	t := time.Now()
	err := conn.Close()
	d := time.Since(t)
	if err != nil {
		c.profiler.Report("conn_close_err", d)
	} else {
		c.profiler.Report("conn_close", d)
	}
	return err
}

func (c *Client) setConnDeadline(conn memConn, deadline time.Time) error {
	t := time.Now()
	err := conn.SetDeadline(deadline)
	c.profiler.Report("conn_set_deadline", time.Since(t))
	return err
}

// Stats represents internal stats such as connections count, etc.
type Stats struct {
	ConnPoolLen   int    // ConnPoolLen is the current size of connections pool
	OpenConnCount uint32 // OpenConnCount is the current number of opened connections
}

type tcpConnProvider struct{}

func (p *tcpConnProvider) NewConn(address string) (memConn, error) {
	conn, err := net.Dial("tcp", address)
	return &tcpConn{tcp: conn, lastErr: err}, err
}

// Profiler is an interface for profiling the client network io performance.
type Profiler interface {
	Report(name string, duration time.Duration)
}

type nopProfiler struct{}

func (n *nopProfiler) Report(name string, duration time.Duration) {
}

type FuncProfiler struct {
	callbackFunc func(string, time.Duration)
}

// NewFuncProfiler instantiates profiler. func f is exposed to the client of
// this method to customize reported metrics handling.
func NewFuncProfiler(f func(string, time.Duration)) *FuncProfiler {
	return &FuncProfiler{
		callbackFunc: f,
	}
}

// Report reports given metric name and duration to the caller.
func (f *FuncProfiler) Report(name string, duration time.Duration) {
	if f.callbackFunc != nil {
		f.callbackFunc(name, duration)
	}
}

// memConn encapsulates net.Conn interface and saves last error since net.Conn
// doesn't provide any information on whether the connection is closed
type memConn interface {
	net.Conn
	LastErr() error
	GetLastUsed() time.Time
	SetLastUsed(t time.Time)
}

type tcpConn struct {
	tcp      net.Conn
	lastErr  error
	lastUsed time.Time
}

func (c *tcpConn) Read(b []byte) (int, error) {
	var n int
	n, c.lastErr = c.tcp.Read(b)
	return n, c.lastErr
}

func (c *tcpConn) Write(b []byte) (int, error) {
	var n int
	n, c.lastErr = c.tcp.Write(b)
	return n, c.lastErr
}

func (c *tcpConn) Close() error {
	c.lastErr = c.tcp.Close()
	return c.lastErr
}

func (c *tcpConn) LocalAddr() net.Addr {
	return c.tcp.LocalAddr()
}

func (c *tcpConn) RemoteAddr() net.Addr {
	return c.tcp.RemoteAddr()
}

func (c *tcpConn) SetDeadline(t time.Time) error {
	c.lastErr = c.tcp.SetDeadline(t)
	return c.lastErr
}

func (c *tcpConn) SetReadDeadline(t time.Time) error {
	c.lastErr = c.tcp.SetReadDeadline(t)
	return c.lastErr
}

func (c *tcpConn) SetWriteDeadline(t time.Time) error {
	c.lastErr = c.tcp.SetWriteDeadline(t)
	return c.lastErr
}

func (c *tcpConn) LastErr() error {
	return c.lastErr
}

func (c *tcpConn) GetLastUsed() time.Time {
	return c.lastUsed
}

func (c *tcpConn) SetLastUsed(t time.Time) {
	c.lastUsed = t
}

type maxValueCollector struct {
	values []time.Duration
}

func (m *maxValueCollector) Add(value time.Duration) {
	m.values = append(m.values, value)
}

func (m *maxValueCollector) Max() time.Duration {
	var max time.Duration
	for _, v := range m.values {
		if v > max {
			max = v
		}
	}
	return max
}

// split returns VALUE section and further (possibly partial)
// data section if \r\n delimiter is present, otherwise returns nil
func split(data []byte) [][]byte {
	parts := bytes.SplitAfterN(data, sep, 2)
	if len(parts) != 2 {
		return nil
	}
	return parts
}

// parseValueResp parses VALUE section of the response and returns
// key, length of data section and whether an error occurred
func parseValueResp(raw string) (string, int, error) {
	value := strings.TrimSuffix(raw, "\r\n")
	terms := strings.Split(value, " ")
	if terms == nil || strings.Compare(terms[0], "VALUE") == -1 || len(terms) != 4 {
		return "", 0, fmt.Errorf("incorrect value response %v", terms)
	}
	length, err := strconv.Atoi(terms[3])
	if err != nil {
		return "", 0, fmt.Errorf("error parse data len field: %v", err)
	}
	return terms[1], length, nil
}
