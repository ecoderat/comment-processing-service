package main

import (
	"comment-processing-service/internal/config"
	"comment-processing-service/internal/sentiment"

	"github.com/sirupsen/logrus"
)

func main() {
	config.LoadEnv()

	if err := sentiment.Run(":50051"); err != nil {
		logrus.StandardLogger().WithError(err).Fatal("serve")
	}
}
