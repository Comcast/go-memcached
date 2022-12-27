package memcached

import (
	"testing"
	"time"
)

func TestPushPop(t *testing.T) {
	const stackSize = 10
	cs := newStack(stackSize)
	t1, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	for i := 1; i <= stackSize; i++ {
		testConn := mockConn{}
		testConn.SetLastUsed(t1.Add(time.Duration(i) * time.Minute))
		if ok := cs.push(&testConn); !ok {
			t.Fatal("stack push error: stack full")
		}
	}
	for i := stackSize; i > 0; i-- {
		testConn, ok := cs.pop()
		if !ok {
			t.Fatal("stack pop error: stack empty")
		}
		tm := t1.Add(time.Duration(i) * time.Minute)
		if testConn.GetLastUsed() != tm {
			t.Fatalf("expected time %v but got %v\n", tm, testConn.GetLastUsed())
		}
	}
}

func TestPushPopError(t *testing.T) {
	const stackSize = 5
	cs := newStack(stackSize)
	for i := 0; i < stackSize; i++ {
		testConn := mockConn{}
		if ok := cs.push(&testConn); !ok {
			t.Fatal("stack push error: stack full")
		}
	}
	testConn := mockConn{}
	if ok := cs.push(&testConn); ok {
		t.Fatal("expected stack full")
	}
	for i := 0; i < stackSize; i++ {
		if _, ok := cs.pop(); !ok {
			t.Fatal("stack pop error: stack empty")
		}
	}
	if _, ok := cs.pop(); ok {
		t.Fatal("expected stack empty")
	}
}

func TestPopAfter(t *testing.T) {
	const stackSize = 10

	t1, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")

	type test struct {
		name              string
		time              time.Time
		expectedTimeAfter bool
	}

	tests := []test{
		{
			name:              "Connection not expired",
			time:              t1.Add(1 * time.Minute),
			expectedTimeAfter: true,
		},
		{
			name:              "Connection not expired",
			time:              t1.Add(2 * time.Minute),
			expectedTimeAfter: true,
		},
		{
			name:              "Connection not expired",
			time:              t1.Add(3 * time.Minute),
			expectedTimeAfter: true,
		},
		{
			name:              "Connection expired",
			time:              t1.Add(time.Duration(stackSize+1) * time.Minute),
			expectedTimeAfter: false,
		},
		{
			name:              "Connection not expired",
			time:              t1.Add(4 * time.Minute),
			expectedTimeAfter: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cs := newStack(stackSize)
			for i := 0; i < stackSize; i++ {
				testConn := mockConn{}
				testConn.SetLastUsed(t1.Add(time.Duration(i) * time.Minute))
				if ok := cs.push(&testConn); !ok {
					t.Fatal("stack push error: stack full")
				}
			}
			conn, ok := cs.pop()
			if !ok {
				t.Fatal("connection stack is empty")
			}
			ta := conn.GetLastUsed().After(test.time)
			if test.expectedTimeAfter != ta {
				t.Fatalf("expected time after %v but got %v", test.expectedTimeAfter, ta)
			}
		})
	}
}
