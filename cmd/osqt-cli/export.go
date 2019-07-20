package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/urfave/cli"
	"golang.org/x/xerrors"
	"gopkg.in/yaml.v3"

	"github.com/gen0cide/osqt"
)

var (
	outputFile   string
	outputFormat string
	specsDir     string
	expCommands  = []cli.Command{
		{
			Name:  "schema",
			Usage: "Exports a structured JSON or YAML file containing the Schema of OSQuery's tables.",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "specs-dir",
					Destination: &specsDir,
					Usage:       "User defined query to be used in OSQuery (required)",
					EnvVar:      "OSQT_SPECS_DIR",
				},
				cli.StringFlag{
					Name:        "output-file",
					Destination: &outputFile,
					Usage:       "Path to write the generated schema file (STDOUT if empty).",
					EnvVar:      "OSQT_OUTPUT_FILE",
				},
				cli.StringFlag{
					Name:        "output-format",
					Destination: &outputFormat,
					Usage:       "Format to write the the generated schema in (options: 'json' or 'yaml').",
					Value:       "json",
					EnvVar:      "OSQT_OUTPUT_FORMAT",
				},
			},
			Action: exportSchema,
		},
	}
)

func isValidDirectory(loc string) error {
	fsinfo, err := os.Stat(loc)
	if err != nil {
		return xerrors.Errorf("not a valid directory: %v", err)
	}
	if !fsinfo.IsDir() {
		return xerrors.New("location is a file and not a directory")
	}
	return nil
}

func exportSchema(c *cli.Context) error {
	if specsDir == "" {
		return xerrors.New("--specs-dir LOCATION was not provided")
	}
	if err := isValidDirectory(specsDir); err != nil {
		return xerrors.Errorf("--specs-dir value was invalid: %v", err)
	}

	parser := osqt.NewParser(log.Named("parser"))

	if err := parser.ParseDirectory(specsDir); err != nil {
		return xerrors.Errorf("error attempting to parse directory: %v", err)
	}

	var data []byte
	var err error

	if outputFormat == "yaml" {
		data, err = yaml.Marshal(parser.Namespaces)
		if err != nil {
			return xerrors.Errorf("error attempting to render tables as YAML: %v", err)
		}
	} else {
		data, err = json.MarshalIndent(parser.Namespaces, "", "  ")
		if err != nil {
			return xerrors.Errorf("error attempting to render tables as JSON: %v", err)
		}
	}

	if outputFile == "" {
		fmt.Printf("%s\n", string(data))
		return nil
	}

	fw, err := os.Create(outputFile)
	if err != nil {
		return xerrors.Errorf("error opening output file for writing data: %v", err)
	}

	defer fw.Close()

	bytesWritten, err := fw.Write(data)
	if err != nil {
		return xerrors.Errorf("error writing output file: %v", err)
	}

	log.Infof("%d table schemas written to %s (%d bytes).", len(parser.Namespaces), outputFile, bytesWritten)

	return nil
}
