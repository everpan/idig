package main

import (
	"github.com/everpan/idig/pkg/core"
	_ "github.com/everpan/idig/pkg/handler"
)

func main() {
	app := core.CreateApp()
	app.Listen(":9090")
}
