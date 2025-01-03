package config

import (
	"fmt"
	"github.com/spf13/viper"
)

type ReloadConfigFunc func() error

var reloadConfigFunc []ReloadConfigFunc

func RegisterReloadConfigFunc(fn ReloadConfigFunc) {
	reloadConfigFunc = append(reloadConfigFunc, fn)
}

func ReloadConfig() error {
	err := viper.ReadInConfig()
	if err != nil {
		// return err
		panic(fmt.Errorf("Fatal error config file: %w \n", err))
	}
	for _, f := range reloadConfigFunc {
		if err = f(); err != nil {
			return err
		}
	}
	return nil
}

func init() {
	viper.AutomaticEnv()
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.idig")
	viper.SetConfigName("idig")
	viper.SetConfigType("yaml")
}
