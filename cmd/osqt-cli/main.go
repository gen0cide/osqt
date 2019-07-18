package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"go.uber.org/zap"

	"github.com/gen0cide/osqt"
)

var log *zap.SugaredLogger

func main() {
	alog, err := zap.NewDevelopment(zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	if err != nil {
		panic(err)
	}

	log = alog.Sugar().With(
		"ost_version",
		osqt.Version,
	)

	specDir, err := os.Stat(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	if !specDir.IsDir() {
		log.Fatal(errors.New("supplied file is not a directory"))
	}

	psr := osqt.NewParser()

	err = psr.ParseDirectory(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("[*] Dumping\n")
	data, err := json.MarshalIndent(psr.Tables, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", string(data))
}
