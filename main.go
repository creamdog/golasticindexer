package main

import (
	"encoding/json"
	"github.com/oschwald/geoip2-golang"
	"io/ioutil"
	"log"
	"os"
)

var errLogger *log.Logger = log.New(os.Stderr, "ERROR: ", log.Llongfile|log.Ldate|log.Ltime)

func main() {

	config := loadconfig()

	geoip2Reader, err := geoip2.Open("GeoLite2-City.mmdb")
	if err != nil {
		log.Println(err.Error())
		return
	}

	indexFiles := make(chan *HostLogFile, 4)

	downloadedFilesChannel := make(chan string, 4)

	parser := NewLogFileParser(indexFiles, geoip2Reader, config)

	go parser.Watch(downloadedFilesChannel)

	puller := NewLogFilePuller(downloadedFilesChannel, config)

	go puller.Run()

	indexers := make(chan int, 8)

	upload := func(file *HostLogFile, done chan int) {
		defer func() {
			<-done
			log.Printf("removing: %s", file.Path)
			os.Remove(file.Path)
		}()
		indexer := NewElasticSearchClient(config)
		if err := indexer.Upload(file.Path, file.Index); err != nil {
			errLogger.Printf("failed to upload file %v -> %v, error: %v", file.Path, file.Index, err)
		}
	}

	for {
		select {
		case a := <-indexFiles:
			indexers <- 1
			go upload(a, indexers)
			break
		}
	}

}

func loadconfig() map[string]interface{} {
	content, err := ioutil.ReadFile("config.json")
	if err != nil {
		panic(err)
	}
	var config map[string]interface{}
	json.Unmarshal(content, &config)
	return config
}
