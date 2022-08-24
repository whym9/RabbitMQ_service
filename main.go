package main

import (
	"flag"
	"rabbitmq_service/internal/servers"
)

func main() {
	addr := *flag.String("addr", ":5005", "GRPC address")

	servers.Serve(addr)
}
