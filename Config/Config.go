package Config

import (
	"flag"
)

type Options struct {
	Host       string
	Port       uint
	LogsPath   string
	Production bool
}

var options *Options

func init() {
	if options == nil {
		options = &Options{}
	}

	flag.StringVar(&options.Host, "host", "127.0.0.1", "server host")
	flag.UintVar(&options.Port, "port", 5000, "server port")
	flag.StringVar(&options.LogsPath, "logsPath", "/opt/aqosta", "path for server logs")
	flag.BoolVar(&options.Production, "prod", false, "production mode for application")

	flag.Parse()
}

func GetOptions() *Options {
	return options
}
