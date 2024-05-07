package internal

import (
	"sync"

	"github.com/tidwall/redcon"
)

type Redis struct {
	lock  sync.RWMutex
	items map[string][]byte
}

func New() *Redis {
	r := &Redis{}
	return r
}

func (r *Redis) Ping(conn redcon.Conn) {
	conn.WriteString("PONG")
}

func (r *Redis) Quit(conn redcon.Conn) {
	conn.WriteString("OK")
	conn.Close()
}

func (r *Redis) Set(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	r.lock.Lock()
	r.items[string(cmd.Args[1])] = cmd.Args[2]
	r.lock.Unlock()

	conn.WriteString("OK")
}

func (r *Redis) Get(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	r.lock.RLock()
	val, ok := r.items[string(cmd.Args[1])]
	r.lock.RUnlock()

	if !ok {
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

func (r *Redis) Del(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	r.lock.Lock()
	_, ok := r.items[string(cmd.Args[1])]
	delete(r.items, string(cmd.Args[1]))
	r.lock.Unlock()

	if !ok {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

func (r *Redis) UnknownCmd(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
}
