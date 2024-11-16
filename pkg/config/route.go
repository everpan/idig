package config

import (
	"github.com/gofiber/fiber/v2"
)

var allRoute []*IDigRoute

func RegisterRouter(routes []*IDigRoute) {
	for _, route := range routes {
		allRoute = append(allRoute, route)
	}
}

func apply(router fiber.Router, routes []*IDigRoute) {
	for _, route := range allRoute {
		if route.Children != nil {
			r := router.Group(route.Path)
			apply(r, route.Children)
		} else {
			if len(route.Method) > 0 {
				router.Add(route.Method, route.Path, func(c *fiber.Ctx) error {
					return IDigHandlerExec(c, route.Handler)
				})
			} else {
				router.Group(route.Path, func(c *fiber.Ctx) error {
					return IDigHandlerExec(c, route.Handler)
				})
			}
		}
	}
}
func Use(app *fiber.App) {
	router := app.Group("/api/v1")
	apply(router, allRoute)
}
