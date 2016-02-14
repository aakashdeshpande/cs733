package main

import (
	"testing"
	"time"
)

var clientCh chan command = make(chan command)

func TestMain(m *testing.M) {
	go serverMain(clientCh) // launch the server as a goroutine.
	time.Sleep(1 * time.Second)
	m.Run()
}

// Serial check of leader and commit
func TestRaftSimple(t *testing.T) {

	time.Sleep(time.Second)

	clientCh <- NewAppend([]byte("Msg"))

    time.Sleep(5*time.Second)

    clientCh <- NewAppend([]byte("Msg2"))
    clientCh <- NewAppend([]byte("Msg3"))

    time.Sleep(10*time.Second)
}

