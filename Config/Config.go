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

	flag.StringVar(&options.Host, "host", "", "server host")
	flag.UintVar(&options.Port, "port", 33333, "server port")
	flag.StringVar(&options.LogsPath, "logsPath", "/opt/aqosta/logs", "path for server logs")

	flag.Parse()
}

func GetOptions() *Options {
	return options
}
