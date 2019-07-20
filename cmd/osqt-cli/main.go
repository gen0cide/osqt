package main

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/fatih/color"
	"github.com/mattn/go-colorable"
	"github.com/urfave/cli"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/gen0cide/osqt"
)

var (
	debug      = false
	quiet      = false
	jsonOutput = false
	log        *zap.SugaredLogger
)

func customTime(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(
		fmt.Sprintf("[%s/%s/%s]",
			color.HiCyanString("osqt-cli"),
			color.BlueString("%s", osqt.Version),
			color.WhiteString("%s", t.Format(time.RFC822)),
		),
	)
}

func customCaller(t zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(
		fmt.Sprintf(
			"%s:%s",
			color.HiRedString("%s", t.TrimmedPath()),
			color.RedString("%d", t.Line),
		),
	)
}

func main() {
	aa := zap.NewDevelopmentEncoderConfig()
	aa.EncodeLevel = zapcore.CapitalColorLevelEncoder
	aa.EncodeTime = customTime
	aa.EncodeCaller = customCaller
	bb := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(aa),
		zapcore.AddSync(colorable.NewColorableStdout()),
		zapcore.DebugLevel,
	), zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	log = bb.Sugar().With(
		"ost_version",
		osqt.Version,
	)
	app := cli.NewApp()
	app.Name = "osqt-cli"
	app.Usage = "OSQuery table analysis and toolkit for developers and security engineers."
	app.Version = osqt.Version
	app.Authors = []cli.Author{
		cli.Author{
			Name:  "Alex Levinson",
			Email: "gen0cide.threats@gmail.com",
		},
	}

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:        "debug",
			Destination: &debug,
			Usage:       "Print verbose output to the CLI.",
			EnvVar:      "OSQT_DEBUG",
		},
		cli.BoolFlag{
			Name:        "quiet",
			Destination: &quiet,
			Usage:       "Silence all output except ERROR/FATAL messages.",
			EnvVar:      "OSQT_QUIET",
		},
		cli.BoolFlag{
			Name:        "json",
			Destination: &jsonOutput,
			Usage:       "Output all logging messages as JSON.",
			EnvVar:      "OSQT_JSON_OUTPUT",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:        "export",
			Aliases:     []string{"e"},
			Usage:       "Export a structured schema based on OSQuery spec files.",
			Subcommands: expCommands,
		},
		{
			Name:        "generate",
			Aliases:     []string{"g"},
			Usage:       "Generate various output based on a structured schema.",
			Subcommands: genCommands,
		},
		{
			Name:        "server",
			Aliases:     []string{"s"},
			Usage:       "Runs a local MySQL-compatible server mimicking OSQuery's database.",
			Subcommands: serveCommands,
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	app.Before = func(c *cli.Context) error {
		opts := []zap.Option{}
		lvl := zapcore.InfoLevel
		if c.Bool("debug") == true {
			lvl = zapcore.DebugLevel
			opts = []zap.Option{
				zap.AddCaller(),
				zap.AddStacktrace(zapcore.ErrorLevel),
			}
		}
		if c.Bool("quiet") == true {
			lvl = zapcore.ErrorLevel
		}
		if c.Bool("json") == true {
			aa := zap.NewDevelopmentEncoderConfig()
			bb := zap.New(zapcore.NewCore(
				zapcore.NewJSONEncoder(aa),
				zapcore.AddSync(colorable.NewColorableStdout()),
				lvl,
			), opts...)
			log = bb.Sugar()
			return nil
		}
		aa := zap.NewDevelopmentEncoderConfig()
		aa.EncodeLevel = zapcore.CapitalColorLevelEncoder
		aa.EncodeTime = customTime
		aa.EncodeCaller = customCaller
		bb := zap.New(zapcore.NewCore(
			zapcore.NewConsoleEncoder(aa),
			zapcore.AddSync(colorable.NewColorableStdout()),
			lvl,
		), opts...)
		log = bb.Sugar()
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
