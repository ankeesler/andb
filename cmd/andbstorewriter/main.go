package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ankeesler/andb/filestore"
	"github.com/ankeesler/andb/filestore/datastore"
	"github.com/ankeesler/andb/filestore/metastore"
	"github.com/ankeesler/andb/memstore"
	log "github.com/sirupsen/logrus"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("usage: %s <storedir> <keycount>\n", os.Args[0])
		os.Exit(1)
	}

	storedir := os.Args[1]
	keycount, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Printf("error: convert keycount: %s\n", err.Error())
		os.Exit(1)
	}

	log.SetOutput(ioutil.Discard)

	dFile, err := os.OpenFile(
		filepath.Join(storedir, "andbdata.bin"),
		os.O_RDWR|os.O_CREATE,
		0600,
	)
	if err != nil {
		fmt.Printf("error: open metastore: %s\n", err.Error())
		os.Exit(1)
	}
	defer dFile.Close()

	mFile, err := os.OpenFile(
		filepath.Join(storedir, "andbmeta.bin"),
		os.O_RDWR|os.O_CREATE,
		0600,
	)
	if err != nil {
		fmt.Printf("error: open metastore: %s\n", err.Error())
		os.Exit(1)
	}
	defer mFile.Close()

	f := filestore.New(
		memstore.New(),
		datastore.New(dFile),
		metastore.New(mFile),
	)

	for i := 0; i < keycount; i++ {
		if err := f.Set(
			fmt.Sprintf("key-%d", i),
			fmt.Sprintf("value-%d", i),
		); err != nil {
			fmt.Printf("error: set: %s", err.Error())
			os.Exit(1)
		}

		if i > 100 && i%100 == 0 {
			fmt.Printf("count: %d\n", i)
			if err := f.Sync(); err != nil {
				fmt.Printf("error: sync: %s", err.Error())
				os.Exit(1)
			}
		}
	}
}
