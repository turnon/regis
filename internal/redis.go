package internal

import (
	"net"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/redcon"
)

type server struct {
	lock          sync.Mutex
	conn2Dbno     map[net.Conn]string
	dbno2Database map[string]*database
}

func ListenAndServe(addr string) error {
	r := &server{
		conn2Dbno:     map[net.Conn]string{},
		dbno2Database: map[string]*database{},
	}

	r.startExpireJob()

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

func (r *server) UnknownCmd(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
}

func (r *server) Ping(conn redcon.Conn) {
	conn.WriteString("PONG")
}

func (r *server) Quit(conn redcon.Conn) {
	delete(r.conn2Dbno, conn.NetConn())
	conn.WriteString("OK")
	conn.Close()
}

func (r *server) Select(conn redcon.Conn, cmd redcon.Command) {
	r.setDbWithConnAndDbno(conn.NetConn(), string(cmd.Args[1]))
	conn.WriteString("OK")
}

func (r *server) setDbWithConnAndDbno(conn net.Conn, dbno string) *database {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.conn2Dbno[conn] = dbno
	db := r.selectDbByNo(dbno)

	return db
}

func (r *server) getDbWithConn(conn net.Conn) *database {
	r.lock.Lock()
	defer r.lock.Unlock()

	dbno, ok := r.conn2Dbno[conn]
	if !ok {
		dbno = "0"
	}
	db := r.selectDbByNo(dbno)

	return db
}

func (r *server) selectDbByNo(dbno string) *database {
	db, ok := r.dbno2Database[dbno]
	if !ok {
		db = &database{
			items:     map[string][]byte{},
			deadlines: map[string]time.Time{},
		}
		r.dbno2Database[dbno] = db
	}
	return db
}

func (r *server) Exec(conn redcon.Conn, cmd redcon.Command) {
	db := r.getDbWithConn(conn.NetConn())
	db.lock.Lock()
	defer db.lock.Unlock()

	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		r.UnknownCmd(conn, cmd)
	case "set":
		db.Set(conn, cmd)
	case "get":
		db.Get(conn, cmd)
	case "del":
		db.Del(conn, cmd)
	case "expire":
		db.Expire(conn, cmd)
	}
}

func (r *server) startExpireJob() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			<-ticker.C
			r.expire()
		}
	}()
}

func (r *server) expire() {
	r.lock.Lock()
	dbs := make([]*database, 0, len(r.dbno2Database))
	for _, db := range r.dbno2Database {
		dbs = append(dbs, db)
	}
	r.lock.Unlock()

	for _, db := range dbs {
		db.lock.Lock()
		db.expire()
		db.lock.Unlock()
	}
}
