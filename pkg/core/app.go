package core

import (
	"fmt"
	"github.com/everpan/idig/pkg/config"
	"github.com/gofiber/contrib/fiberzap/v2"
	"github.com/gofiber/fiber/v2"
)

func CreateApp() *fiber.App {
	app := fiber.New()
	logger := config.GetLogger()
	app.Use(fiberzap.New(fiberzap.Config{Logger: logger}))
	config.Use(app)
	// logger.Info("main", zap.Any("routes", app.GetRoutes()))
	for _, r := range app.GetRoutes() {
		fmt.Printf("%v\n", r)
	}
	return app
}