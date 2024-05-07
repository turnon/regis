package main

import (
	"log"
	"strings"

	"github.com/tidwall/redcon"
	"github.com/turnon/regis/internal"
)

const addr = ":6380"

func main() {
	m := internal.Redis{}
	err := redcon.ListenAndServe(addr, func(conn redcon.Conn, cmd redcon.Command) {
		switch strings.ToLower(string(cmd.Args[0])) {
		default:
			m.UnknownCmd(conn, cmd)
		case "ping":
			m.Ping(conn)
		case "quit":
			m.Quit(conn)
		case "set":
			m.Set(conn, cmd)
		case "get":
			m.Get(conn, cmd)
		case "del":
			m.Del(conn, cmd)
		}
	}, func(conn redcon.Conn) bool {
		// Use this function to accept or deny the connection.
		// log.Printf("accept: %s", conn.RemoteAddr())
		return true
	}, func(conn redcon.Conn, err error) {
		// This is called when the connection has been closed
		// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
	})
	if err != nil {
		log.Fatal(err)
	}
}
