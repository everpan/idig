package main

import (
	"fmt"
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/core"
	_ "github.com/everpan/idig/pkg/event"
	_ "github.com/everpan/idig/pkg/handler"
	"github.com/spf13/viper"
)

var hostPort = ":9090"

func init() {
	viper.SetDefault("server.host-port", hostPort)
	config.RegisterReloadConfigFunc(func() error {
		hostPort = viper.GetString("server.host-port")
		fmt.Printf("reload host port %v\n", hostPort)
		return nil
	})
}

func main() {
	_ = viper.SafeWriteConfigAs("./idig.yaml")
	_ = config.ReloadConfig()
	// 启动之初，将以 tenant.default 的 db信息作为整个系统的信息，进行初始化
	// AppInit()
	app := core.CreateApp()
	_ = app.Listen(hostPort)
}
