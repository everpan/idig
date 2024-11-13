package main

import (
	"github.com/everpan/idig/pkg/config"
	"github.com/gofiber/contrib/fiberzap/v2"
	"github.com/gofiber/fiber/v2"
)

func main() {
	app := fiber.New()
	logger := config.GetLogger()
	app.Use(fiberzap.New(fiberzap.Config{Logger: logger}))
	app.Listen(":9090")
}
