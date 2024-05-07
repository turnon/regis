package internal

import (
	"net"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
)

type redisInMem struct {
	lock          sync.Mutex
	conn2Dbno     map[net.Conn]string
	dbno2Database map[string]*database
}

func ListenAndServe(addr string) error {
	r := &redisInMem{
		conn2Dbno:     map[net.Conn]string{},
		dbno2Database: map[string]*database{},
	}

	return redcon.ListenAndServe(addr, func(conn redcon.Conn, cmd redcon.Command) {
		// for i, c := range cmd.Args {
		// 	fmt.Println(i, string(c))
		// }

		switch strings.ToLower(string(cmd.Args[0])) {
		case "ping":
			r.Ping(conn)
		case "quit":
			r.Quit(conn)
		case "select":
			r.Select(conn, cmd)
		default:
			r.Exec(conn, cmd)
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

func (r *redisInMem) UnknownCmd(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
}

func (r *redisInMem) Ping(conn redcon.Conn) {
	conn.WriteString("PONG")
}

func (r *redisInMem) Quit(conn redcon.Conn) {
	delete(r.conn2Dbno, conn.NetConn())
	conn.WriteString("OK")
	conn.Close()
}

func (r *redisInMem) Select(conn redcon.Conn, cmd redcon.Command) {
	r.setDbWithConnAndDbno(conn.NetConn(), string(cmd.Args[1]))
	conn.WriteString("OK")
}

func (r *redisInMem) setDbWithConnAndDbno(conn net.Conn, dbno string) *database {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.conn2Dbno[conn] = dbno
	db := r.selectDbByNo(dbno)

	return db
}

func (r *redisInMem) getDbWithConn(conn net.Conn) *database {
	r.lock.Lock()
	defer r.lock.Unlock()

	dbno, ok := r.conn2Dbno[conn]
	if !ok {
		dbno = "0"
	}
	db := r.selectDbByNo(dbno)

	return db
}

func (r *redisInMem) selectDbByNo(dbno string) *database {
	db, ok := r.dbno2Database[dbno]
	if !ok {
		db := &database{items: map[string][]byte{}}
		r.dbno2Database[dbno] = db
	}
	return db
}

func (r *redisInMem) Exec(conn redcon.Conn, cmd redcon.Command) {
	db := r.getDbWithConn(conn.NetConn())

	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		r.UnknownCmd(conn, cmd)
	case "set":
		db.Set(conn, cmd)
	case "get":
		db.Get(conn, cmd)
	case "del":
		db.Del(conn, cmd)
	}
}
