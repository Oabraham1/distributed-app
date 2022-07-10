package main

import (
	"log"

	"github.com/oabraham1/distributed-app/config"
	"github.com/oabraham1/distributed-app/internal/server"
)

func main() {
	server := server.NewHTTPServer(config.PORT)
	log.Fatal(server.ListenAndServe())
}
