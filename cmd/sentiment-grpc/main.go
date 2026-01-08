package main

import (
	"log"

	"comment-processing-service/internal/config"
	"comment-processing-service/internal/sentiment"
)

func main() {
	config.LoadEnv()

	if err := sentiment.Run(":50051"); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
