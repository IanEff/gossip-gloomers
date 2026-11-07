package main

import (
	"encoding/json"
	"log"
	"slices"
	"sync"
	"uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)



type Log struct {
	Messages map[string]int64
	mu       sync.RWMutex
}

func NewLog() *Log {
	return &Log{Messages: make(map[string]int64)}
}

func (l *Log) Add(key string, value int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Messages = append(l.Messages, value)
}

func (l *Log) GetAll() []int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return slices.Clone(l.Messages)
}

func main() {
	l := NewLog()
	n := maelstrom.NewNode()

	var cluster map[string]bool

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

		message, ok := body["message"].(float64)
		if !ok {
			return maelstrom.NewRPCError(maelstrom.MalformedRequest, "broadcast: message must be a number")
		}
		l.Add(int64(message))

		body["type"] = "broadcast_ok"

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "read_ok"
		body["messages"] = l.GetAll()

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		nodes := n.NodeIDs()
		for node := range nodes {
			if node.ID() != n.ID() {
			cluster = append(cluster, node)
			cluster[node] = false

			}
		}
		body["type"] = "topology_ok"

		return n.Reply(msg, body)
	})

	go func () {
		// send to nodes

	}

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
