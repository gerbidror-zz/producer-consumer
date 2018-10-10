package settings

import (
	"github.com/spf13/viper"
)

var Conf *viper.Viper

func init() {
	Conf = viper.New()
	Conf.SetConfigType("json")
	Conf.SetConfigName("settings")
	Conf.AddConfigPath("$HOME/go/src/github.com/gerbidror/producer-consumer/conf")
	Conf.AddConfigPath("/go/src/github.com/gerbidror/producer-consumer/conf")
	if err := Conf.ReadInConfig(); err != nil {
		panic(err)
	}
}
