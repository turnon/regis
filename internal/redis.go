package internal

import (
	"strings"
	"sync"

	"github.com/tidwall/redcon"
)

type redisInMem struct {
	lock  sync.RWMutex
	items map[string][]byte
}

func ListenAndServe(addr string) error {
	r := &redisInMem{}
	r.items = make(map[string][]byte)

	return redcon.ListenAndServe(addr, func(conn redcon.Conn, cmd redcon.Command) {
		// for i, c := range cmd.Args {
		// 	fmt.Println(i, string(c))
		// }

		switch strings.ToLower(string(cmd.Args[0])) {
		default:
			r.UnknownCmd(conn, cmd)
		case "ping":
			r.Ping(conn)
		case "quit":
			r.Quit(conn)
		case "select":
			r.Select(conn, cmd)
		case "set":
			r.Set(conn, cmd)
		case "get":
			r.Get(conn, cmd)
		case "del":
			r.Del(conn, cmd)
		}
	}, func(conn redcon.Conn) bool {
		// Use this function to accept or deny the connection.
		// log.Printf("accept: %s", conn.RemoteAddr())
		return true
	}, func(conn redcon.Conn, err error) {
		// This is called when the connection has been closed
		// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
	})
}

func (r *redisInMem) Ping(conn redcon.Conn) {
	conn.WriteString("PONG")
}

func (r *redisInMem) Quit(conn redcon.Conn) {
	conn.WriteString("OK")
	conn.Close()
}

func (r *redisInMem) Select(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("OK")
}

func (r *redisInMem) Set(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	r.lock.Lock()
	r.items[string(cmd.Args[1])] = cmd.Args[2]
	r.lock.Unlock()

	conn.WriteString("OK")
}

func (r *redisInMem) Get(conn redcon.Conn, cmd redcon.Command) {
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

func (r *redisInMem) Del(conn redcon.Conn, cmd redcon.Command) {
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

func (r *redisInMem) UnknownCmd(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
}
