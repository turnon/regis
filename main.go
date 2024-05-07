package main

import (
	"log"

	"github.com/turnon/regis/internal"
)

const addr = ":6380"

func main() {
	if err := internal.ListenAndServe(addr); err != nil {
		log.Fatal(err)
	}
}
