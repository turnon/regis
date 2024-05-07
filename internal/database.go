package internal

import (
	"sync"

	"github.com/tidwall/redcon"
)

type database struct {
	lock  sync.Mutex
	items map[string][]byte
}

func (db *database) Set(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	db.lock.Lock()
	db.items[string(cmd.Args[1])] = cmd.Args[2]
	db.lock.Unlock()

	conn.WriteString("OK")
}

func (db *database) Get(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	db.lock.Lock()
	val, ok := db.items[string(cmd.Args[1])]
	db.lock.Unlock()

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

	db.lock.Lock()
	_, ok := db.items[string(cmd.Args[1])]
	delete(db.items, string(cmd.Args[1]))
	db.lock.Unlock()

	if !ok {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}
