package memcached

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func NewTestClient(addr string, maxIdleConn int, p connProvider) *Client {
	return &Client{
		addr:        addr,
		pool:        newStack(maxIdleConn),
		provider:    p,
		idleTimeout: defaultIdleConnectionTimeout,
		timeNow:     time.Now,
		profiler:    &nopProfiler{},
	}
}

func TestGet(t *testing.T) {
	type testCase struct {
		name       string
		key        string
		data       string
		dataChunks []string
	}

	testConn := mockConn{}
	provider := mockConnProvider{
		conn: &testConn,
	}
	client := NewTestClient("localhost", 1, &provider)

	tests := []testCase{
		{
			name: "unified",
			key:  "key/3",
			data: "data/3", dataChunks: []string{"VALUE key/3 0 " + strconv.Itoa(len("data/3")) + "\r\ndata/3\r\nEND\r\n"},
		},
		{
			name:       "partially chunked data",
			key:        "key/1",
			data:       "data/1",
			dataChunks: []string{"VALUE ", "key/1", " 0", " ", strconv.Itoa(len("data/1")), "\r\n", "data/1", "\r", "\n", "END\r", "\n"},
		},
		{
			name:       "fully chunked data",
			key:        "key/2",
			data:       "data/2",
			dataChunks: []string{"V", "A", "L", "U", "E", ",", " ", "k", "e", "y", "/", "2", " ", "0", " ", strconv.Itoa(len("data/2")), "\r", "\n", "d", "a", "t", "a", "/", "2", "\r", "\n", "E", "N", "D", "\r", "\n"},
		},
		{
			name:       "empty",
			key:        "key/4",
			data:       "",
			dataChunks: []string{"VALUE key/4 0 " + strconv.Itoa(len("")) + "\r\n\r\nEND\r\n"},
		},
		{
			name:       "double-crlf",
			key:        "key/5",
			data:       "data/5\r\n",
			dataChunks: []string{"VALUE key/5 0 " + strconv.Itoa(len("data/5\r\n")) + "\r\ndata/5\r\n\r\nEND\r\n"},
		},
		{
			name:       "extracr",
			key:        "key/6",
			data:       "data/6\r",
			dataChunks: []string{"VALUE key/6 0 " + strconv.Itoa(len("data/6\r")) + "\r\ndata/6\r\r\nEND\r\n"},
		},
		{
			name:       "extralf",
			key:        "key/7",
			data:       "data/7\n",
			dataChunks: []string{"VALUE key/7 0 " + strconv.Itoa(len("data/7\n")) + "\r\ndata/7\n\r\nEND\r\n"},
		},
		{
			name:       "crlf in the middle of the data",
			key:        "key/8",
			data:       "da\r\nta/8\n",
			dataChunks: []string{"VALUE key/7 0 " + strconv.Itoa(len("da\r\nta/8\n")) + "\r\nda\r\nta/8\n\r\nEND\r\n"},
		},
		{
			name:       "ENDcrlf in the middle of the data",
			key:        "key/9",
			data:       "daEND\r\nta/9\n",
			dataChunks: []string{"VALUE key/9 0 " + strconv.Itoa(len("daEND\r\nta/9\n")) + "\r\ndaEND\r\nta/9\n\r\nEND\r\n"},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer testConn.Reset()
			testConn.dataChunks = test.dataChunks
			rsp, err := client.Get(ctx, test.key)
			if err != nil {
				t.Fatal("get error:", err)
			}
			if rsp.Data == nil || string(rsp.Data) != test.data {
				t.Fatalf("expected data field %v but got %v", test.data, string(rsp.Data))
			}
		})
	}
}

func TestGetJunkData(t *testing.T) {
	type testCase struct {
		name   string
		key    string
		length int
	}

	testConn := mockConnDataGenerator{}
	provider := mockConnProvider{
		conn: &testConn,
	}
	client := NewTestClient("localhost", 1, &provider)

	tests := []testCase{
		{
			name:   "generator",
			key:    "key/1",
			length: internalBuffSize * 2,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer testConn.Reset()
			testConn.JunkData = make([]byte, test.length)
			_, err := client.Get(ctx, test.key)
			if err == nil {
				t.Fatal("expected buffer overflow error")
			}
		})
	}
}

func TestGetTimeout(t *testing.T) {
	type testCase struct {
		name       string
		key        string
		dataChunks []string
		invoked    bool
	}

	testConn := mockConn{}
	provider := mockConnProvider{
		conn: &testConn,
	}
	client := NewTestClient("localhost", 1, &provider)

	tests := []testCase{
		{
			name:       "timeout",
			key:        "test/1",
			dataChunks: []string{"VALUE key/1 0 " + strconv.Itoa(len("data/1")) + "\r\ndata/1\r\nEND\r\n"},
			invoked:    true,
		},
	}

	deadline := time.Now().Add(1 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer testConn.Reset()
			testConn.dataChunks = test.dataChunks
			_, err := client.Get(ctx, test.key)
			if err != nil {
				t.Fatalf("get error: %v", err)
			}
			if testConn.readDeadline != test.invoked {
				t.Fatalf("expected get timeout")
			}
			if testConn.deadline != deadline {
				t.Fatalf("expected deadline %v but got %v", deadline, testConn.deadline)
			}
		})
	}
}

func TestGetConcurrencyRace(t *testing.T) {
	type testCase struct {
		name       string
		key        string
		data       string
		dataChunks []string
	}

	provider := mockMultiConnProvider{}
	client := NewTestClient("localhost", 1, &provider)

	test := testCase{
		name:       "fully chunked data",
		key:        "key/2",
		data:       "data/2",
		dataChunks: []string{"V", "A", "L", "U", "E", " ", "k", "e", "y", "/", "2", " ", "0", " ", strconv.Itoa(len("data/2")), "\r", "\n", "d", "a", "t", "a", "/", "2", "\r", "\n", "E", "N", "D", "\r", "\n"},
	}

	provider.dataChunks = test.dataChunks

	for i := 0; i < 8000; i++ { // 8128 is a limit on simultaneously alive goroutines with -race
		test2 := test
		t.Run(fmt.Sprintf("test_%v", i+1), func(t *testing.T) {
			t.Parallel()
			rsp, err := client.Get(context.Background(), test2.key)
			if err != nil {
				t.Fatal("get error:", err)
			}
			if rsp.Data == nil || string(rsp.Data) != test2.data {
				t.Fatalf("expected data field %v but got %v", test.data, string(rsp.Data))
			}
		})
	}
}

func TestSet(t *testing.T) {
	type testCase struct {
		name     string
		key      string
		data     []byte
		exp      int
		expected string
	}

	testConn := mockConn{writeMode: true}
	provider := mockConnProvider{
		conn: &testConn,
	}
	client := NewTestClient("localhost", 1, &provider)

	tests := []testCase{
		{
			name:     "key 3",
			key:      "key/3",
			data:     []byte("data/3"),
			exp:      3,
			expected: "set key/3 0 3 6\r\ndata/3\r\n",
		},
		{
			name:     "key 1",
			key:      "key/1",
			data:     []byte("data/1"),
			exp:      4,
			expected: "set key/1 0 4 6\r\ndata/1\r\n",
		},
		{
			name:     "key 2",
			key:      "key/2",
			data:     []byte("data/2"),
			exp:      5,
			expected: "set key/2 0 5 6\r\ndata/2\r\n",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				testConn.dataChunks = nil
				testConn.Reset()
			}()
			if err := client.Set(ctx, test.key, test.data, test.exp); err != nil {
				t.Fatal("set error:", err)
			}
			data := strings.Join(testConn.dataChunks, "")
			if data != test.expected {
				t.Fatalf("expected: [%v] but got: [%v]\n", prettify(test.expected), prettify(data))
			}
		})
	}
}

func TestIdleConnTimeout(t *testing.T) {
	type testCase struct {
		name                string
		time                time.Time
		expectNewConnection bool
	}

	testConn := mockConn{writeMode: true}
	provider := mockConnProvider{
		conn: &testConn,
	}
	client := NewTestClient("localhost", 1, &provider)
	client.idleTimeout = 1 * time.Second

	t1, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	t2, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:07Z")
	t3, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:07Z")
	t4, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:09Z")
	t5, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:11Z")

	tests := []testCase{
		{
			name:                "First new connection",
			time:                t1,
			expectNewConnection: true,
		},
		{
			name:                "Second new connection",
			time:                t2,
			expectNewConnection: true,
		},
		{
			name:                "Old connection",
			time:                t3,
			expectNewConnection: false,
		},
		{
			name:                "Third new connection",
			time:                t4,
			expectNewConnection: true,
		},
		{
			name:                "Fourth new connection",
			time:                t5,
			expectNewConnection: true,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, test := range tests {
		client.timeNow = func() time.Time {
			return test.time
		}
		if err := client.Set(ctx, "key", []byte("data"), 5); err != nil {
			t.Fatal("set error:", err)
		}
		if provider.newConnCreated != test.expectNewConnection {
			t.Fatalf("expected new connection created: [%v] but got: [%v]\n", test.expectNewConnection, provider.newConnCreated)
		}
		testConn.Reset()
		provider.Reset()
		testConn.dataChunks = nil
	}
}

func TestConnReused(t *testing.T) {
	const maxIdleConn = 100

	testConn := mockConn{writeMode: true}
	provider := mockConnProvider{
		conn: &testConn,
	}
	client := NewTestClient("localhost", maxIdleConn, &provider)
	client.idleTimeout = 500 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	currentTime, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	var connTimes []time.Time

	for i := 0; i < 100; i++ {
		msec := rand.Intn(999)
		newTime := currentTime.Add(time.Duration(msec) * time.Millisecond)
		client.timeNow = func() time.Time {
			return newTime
		}

		expectNewConnection, index := willUseNewConnection(connTimes, newTime, client.idleTimeout)

		if expectNewConnection {
			connTimes = nil
			connTimes = append(connTimes, newTime)
		} else {
			connTimes[index] = newTime
			connTimes = connTimes[index:]
		}

		if err := client.Set(ctx, "key", []byte("data"), 5); err != nil {
			t.Fatal("set error:", err)
		}

		if provider.newConnCreated != expectNewConnection {
			t.Fatalf("expected new connection created: [%v] but got: [%v]\n", expectNewConnection, provider.newConnCreated)
		}

		testConn.Reset()
		provider.Reset()
		testConn.dataChunks = nil

		currentTime = newTime
	}
}

func willUseNewConnection(connTimes []time.Time, newTime time.Time, timeout time.Duration) (bool, int) {
	for i, t := range connTimes {
		if newTime.Sub(t) < timeout {
			return false, i
		}
	}
	return true, -1
}

func prettify(str string) string {
	return strings.ReplaceAll(str, "\r\n", "\\r\\n")
}

type mockConnProvider struct {
	conn           memConn
	newConnCreated bool
}

func (p *mockConnProvider) NewConn(address string) (memConn, error) {
	p.newConnCreated = true
	return p.conn, nil
}

func (p *mockConnProvider) Reset() {
	p.newConnCreated = false
}

type mockMultiConnProvider struct {
	dataChunks []string
}

func (p *mockMultiConnProvider) NewConn(address string) (memConn, error) {
	conn := mockConn{dataChunks: p.dataChunks}
	return &conn, nil
}

type mockConn struct {
	dataChunks    []string
	counter       int
	readDeadline  bool
	writeDeadline bool
	deadline      time.Time
	writeMode     bool
	lastUsed      time.Time
}

func (c *mockConn) Reset() {
	c.counter = 0
	c.readDeadline = false
	c.writeDeadline = false
	c.deadline = time.Time{}
}

func (c *mockConn) Read(b []byte) (n int, err error) {
	var bytesCopied int
	if c.writeMode {
		str := "STORED\r\n"
		raw := []byte(str)
		if len(raw) > len(b) {
			raw = raw[:len(b)]
		}
		bytesCopied = copy(b, raw[c.counter:])
		c.counter += bytesCopied
	} else {
		if c.counter == len(c.dataChunks) {
			c.Reset()
		}
		str := c.dataChunks[c.counter]
		c.counter++
		raw := []byte(str)
		if len(raw) > len(b) {
			raw = raw[:len(b)]
		}
		bytesCopied = copy(b, raw)
	}
	return bytesCopied, nil
}

func (c *mockConn) Write(b []byte) (n int, err error) {
	var bytesCopied int
	if c.writeMode {
		buf := make([]byte, len(b))
		bytesCopied = copy(buf, b)
		c.dataChunks = append(c.dataChunks, string(buf))
	}
	return bytesCopied, nil
}

func (c *mockConn) Close() error {
	return nil
}

func (c *mockConn) LocalAddr() net.Addr {
	return nil
}

func (c *mockConn) RemoteAddr() net.Addr {
	return nil
}

func (c *mockConn) SetDeadline(t time.Time) error {
	c.readDeadline = true
	c.writeDeadline = true
	c.deadline = t
	return nil
}

func (c *mockConn) SetReadDeadline(t time.Time) error {
	c.readDeadline = true
	c.deadline = t
	return nil
}

func (c *mockConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline = true
	c.deadline = t
	return nil
}

func (c *mockConn) LastErr() error {
	return nil
}

func (c *mockConn) GetLastUsed() time.Time {
	return c.lastUsed
}

func (c *mockConn) SetLastUsed(t time.Time) {
	c.lastUsed = t
}

type mockConnDataGenerator struct {
	JunkData []byte
	pointer  int
	lastUsed time.Time
}

func (c *mockConnDataGenerator) Reset() {
	c.pointer = 0
}

func (c *mockConnDataGenerator) Read(b []byte) (n int, err error) {
	if c.JunkData == nil {
		return 0, fmt.Errorf("junk data not allocated")
	}
	if len(c.JunkData) == c.pointer {
		// return 0, io.EOF
		c.Reset()
	}
	var end int
	if len(b) < len(c.JunkData)-c.pointer {
		end = c.pointer + len(b)
	} else {
		end = len(c.JunkData)
	}
	bytesCopied := copy(b, c.JunkData[c.pointer:end])
	c.pointer += bytesCopied
	return bytesCopied, nil
}

func (c *mockConnDataGenerator) Write(b []byte) (n int, err error) {
	return -1, nil
}

func (c *mockConnDataGenerator) Close() error {
	return nil
}

func (c *mockConnDataGenerator) LocalAddr() net.Addr {
	return nil
}

func (c *mockConnDataGenerator) RemoteAddr() net.Addr {
	return nil
}

func (c *mockConnDataGenerator) SetDeadline(t time.Time) error {
	return nil
}

func (c *mockConnDataGenerator) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *mockConnDataGenerator) SetWriteDeadline(t time.Time) error {
	return nil
}

func (c *mockConnDataGenerator) LastErr() error {
	return nil
}

func (c *mockConnDataGenerator) GetLastUsed() time.Time {
	return c.lastUsed
}

func (c *mockConnDataGenerator) SetLastUsed(t time.Time) {
	c.lastUsed = t
}
