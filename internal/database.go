package internal

import (
	"strconv"
	"sync"
	"time"

	"github.com/tidwall/redcon"
)

type database struct {
	lock      sync.Mutex
	items     map[string][]byte
	deadlines map[string]time.Time
}

func (db *database) Set(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	db.items[string(cmd.Args[1])] = cmd.Args[2]
	conn.WriteString("OK")
}

func (db *database) Get(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	val, ok := db.items[string(cmd.Args[1])]
	if !ok {
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

func (db *database) Del(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	_, ok := db.items[string(cmd.Args[1])]
	delete(db.items, string(cmd.Args[1]))
	delete(db.deadlines, string(cmd.Args[1]))
	if !ok {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

func (db *database) Expire(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 || len(cmd.Args) > 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	sec, err := strconv.Atoi(string(cmd.Args[2]))
	if err != nil {
		conn.WriteError("ERR value is not an integer or out of range")
		return
	}

	key := string(cmd.Args[1])
	_, ok := db.items[key]
	if !ok {
		conn.WriteInt(0)
	}

	db.deadlines[key] = time.Now().Add(time.Second * time.Duration(sec))

	conn.WriteInt(1)
}

func (db *database) expire() {
	now := time.Now()
	toDel := []string{}
	for key, deadline := range db.deadlines {
		if deadline.Before(now) {
			toDel = append(toDel, key)
		}
	}
	for _, key := range toDel {
		delete(db.items, key)
		delete(db.deadlines, key)
	}
}
