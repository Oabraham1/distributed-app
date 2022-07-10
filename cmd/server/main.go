package main

import (
	"log"

	"github.com/oabraham1/distributed-app/internal/server"
)

const (
	PORT = ":8080"
)

func main() {
	server := server.NewHTTPServer(PORT)
	log.Fatal(server.ListenAndServe())
}
