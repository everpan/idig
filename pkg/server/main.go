package main

import (
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/core"
	_ "github.com/everpan/idig/pkg/handler"
	"github.com/spf13/viper"
)

func AppInit() {

}

func main() {
	var hostPort = ":9090"
	viper.SetDefault("server.host-port", hostPort)
	_ = viper.SafeWriteConfigAs("./idig.yaml")
	_ = config.ReloadConfig()
	hostPort = viper.GetString("server.host-port")
	// 启动之初，将以 tenant.default 的 db信息作为整个系统的信息，进行初始化
	AppInit()
	app := core.CreateApp()
	_ = app.Listen(":9090")
}
