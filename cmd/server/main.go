package main

import (
	"log"

	"github.com/oabraham1/distributed-app/internal/server"
)

func main() {
	server := server.NewHTTPServer(":8080")
	log.Fatal(server.ListenAndServe())
}