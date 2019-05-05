package main

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ankeesler/andb/filestore/metastore"
	log "github.com/sirupsen/logrus"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("usage: %s <storedir>\n", os.Args[0])
		os.Exit(1)
	}

	storedir := os.Args[1]

	log.SetOutput(ioutil.Discard)
	printFile(
		filepath.Join(storedir, "andbmeta.bin"),
		func(file *os.File) error {
			return metastore.New(file).ForEachBlock(
				func(b metastore.Block) error {
					fmt.Printf("%+v\n", b)
					return nil
				},
			)
		},
	)

	printFile(
		filepath.Join(storedir, "andbdata.bin"),
		func(file *os.File) error {
			return nil
		},
	)
}

func printFile(filename string, print func(*os.File) error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		fmt.Printf("error: print file %s: open file: %s", filename, err.Error())
		os.Exit(1)
	}
	defer file.Close()
	fmt.Printf("file: %s\n", filename)

	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Printf("error: print file %s: read file: %s\n", filename, err.Error())
		os.Exit(1)
	}
	fmt.Println(hex.Dump(bytes))

	if err := print(file); err != nil {
		fmt.Printf("error: print file %s: %s\n", filename, err.Error())
		os.Exit(1)
	}

	fmt.Println("")
}
