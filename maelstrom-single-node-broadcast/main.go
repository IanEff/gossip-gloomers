package main

import (
	"encoding/json"
	"log"
	"sync"
	"uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Log struct {
	Messages []int64
	mu       sync.RWMutex
}

func NewLog() *Log {
	return &Log{Messages: make([]int64, 0), mu: sync.RWMutex{}}
}

func (l *Log) Add(value int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Messages = append(l.Messages, value)
}

func (l *Log) GetAll() []int64 {
	return l.Messages
}

func main() {
	l := NewLog()
	n := maelstrom.NewNode()
	n.Handle("echo", func(msg maelstrom.Message) error {
		// unmarshal the message body as a loosely-type map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// update the message type to return back
		body["type"] = "echo_ok"

		// echo the message back with updated message type
		return n.Reply(msg, body)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = uuid.New()

		return n.Reply(msg, body)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		l.mu.Lock()
		defer l.mu.Unlock()
		l.Add(body["message"].(int64))

		body["type"] = "broadcast_ok"

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		l.mu.RLock()
		defer l.mu.RUnlock()
		body["type"] = "read_ok"
		body["messages"] = l.GetAll()

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// nodes := n.NodeIDs()
		body["type"] = "topology_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
