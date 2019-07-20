package main

import (
	"github.com/urfave/cli"
	"golang.org/x/xerrors"
)

var (
	schemaPath  string
	inputQuery  string
	genCommands = []cli.Command{
		{
			Name:  "result-schema",
			Usage: "Creates a structured schema based upon an OSQuery query.",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "query",
					Destination: &inputQuery,
					Usage:       "User defined query to be used in OSQuery",
					EnvVar:      "OSQT_INPUT_QUERY",
				},
				cli.StringFlag{
					Name:        "schema",
					Destination: &schemaPath,
					Usage:       "Path to a previously exported OSQuery schema JSON file.",
					EnvVar:      "OSQT_SCHEMA_PATH",
				},
			},
			Action: genResultSchema,
		},
	}
)

func genResultSchema(c *cli.Context) error {
	if schemaPath == "" {
		return xerrors.New("--schema path was not provided")
	}
	return nil
}
