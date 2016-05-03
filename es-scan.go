package main

import (
	"strings"
	"path/filepath"
	"os"
	"fmt"
	"encoding/json"
	"net/http"
	"io/ioutil"
	"syscall"
	"github.com/codegangsta/cli"
)

type EsFileInfo struct {
	Path string `json:"path"`
	Mtime int64 `json:"mtime"`
	Size int64 `json:"size"`
	Inode uint32 `json:"-"`
}

func send(file EsFileInfo, host string) error {
	filejson, err := json.Marshal(file)
	if err != nil {
		fmt.Println("Marshaling error: ", err)
		return err
	}
	url := fmt.Sprintf("http://%s:9200/filesystem/file/%d", host, file.Inode) 
	resp, err := http.Post(url, "application/json",
		strings.NewReader(string(filejson)))
	if err != nil {
		fmt.Println("Error posting data: ", err)
		return err
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading body", err)
	}
	return nil
}

// Scan a path recursively into a channel
func scan(path string) <-chan EsFileInfo {
	ch := make(chan EsFileInfo)
	go func() {
		defer close(ch)
		filepath.Walk(path, func(path string, f os.FileInfo, error error) error {
			if (f.IsDir()) { // I don't care about directories
				return nil
			}
			if error != nil {
				fmt.Printf("Error getting file: %s\n", error)
			}
			stat, ok := f.Sys().(*syscall.Stat_t) // Type assertion
			if !ok {
				fmt.Println("Error getting file stat")
				return nil
			}
			file := EsFileInfo{
				Path: path,
				Mtime: f.ModTime().Unix(),
				Size: f.Size(),
				Inode: stat.Ino,
			}			
			ch <- file
			return nil
		})
	}()
	return ch
}

// TODO: Use the bulk API
func sendAll(ch <-chan EsFileInfo, host string) error {
	for f := range ch {
		err := send(f, host)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "es-scan"
	app.Usage = "Scan and place basic file information into an elasticsearch server"
	app.Version = "0.1.0"
	app.Action = func(c *cli.Context) error {	
		root := c.Args()[0]
		ch := scan(root)
		err := sendAll(ch, c.String("host"))
		if err != nil {
			fmt.Println("Error found")
		}
		return nil
	}
	app.Flags = []cli.Flag {
		cli.StringFlag{
			Name:        "host",
			Value:       "localhost:9200",
			Usage:       "The ElasticSearch host",
		},
	}
	app.Run(os.Args)
}
