package main

import (
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"time"
)

type LogFilePuller struct {
	auth        aws.Auth
	bucket      string
	marker      string
	prefix      string
	tmpDir      string
	fileChannel chan string
	statefile   string
}

func NewLogFilePuller(fileChannel chan string, config map[string]interface{}) *LogFilePuller {

	access_key := config["source"].(map[string]interface{})["s3"].(map[string]interface{})["access_key"].(string)
	secret_key := config["source"].(map[string]interface{})["s3"].(map[string]interface{})["secret_key"].(string)
	bucket := config["source"].(map[string]interface{})["s3"].(map[string]interface{})["bucket"].(string)
	prefix := config["source"].(map[string]interface{})["s3"].(map[string]interface{})["prefix"].(string)
	tmpdir := config["source"].(map[string]interface{})["tmpdir"].(string)

	os.MkdirAll(tmpdir, 0700)

	return &LogFilePuller{
		auth:        aws.Auth{AccessKey: access_key, SecretKey: secret_key},
		marker:      "",
		prefix:      prefix,
		bucket:      bucket,
		tmpDir:      tmpdir,
		statefile:   "marker.txt",
		fileChannel: fileChannel}
}

func (puller *LogFilePuller) RestoreState() {
	if _, err := os.Stat(puller.statefile); err == nil {
		content, err := ioutil.ReadFile(puller.statefile)
		if err != nil {
			panic(err)
		}
		puller.marker = string(content)
	}
}

func (puller *LogFilePuller) StoreState() {
	f, err := os.OpenFile(puller.statefile, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	if _, err = f.WriteString(puller.marker); err != nil {
		panic(err)
	}
	f.Close()
}

func (puller *LogFilePuller) Run() {

	puller.RestoreState()

	str := time.Now().UTC().Format("2006-01-02")
	t, err := time.Parse("2006-01-02", str)
	if err != nil {
		panic(err)
	}
	sevenDaysAgo := t.UTC().Add(24 * 7 * -time.Hour)

	log.Printf("sevenDaysAgo: %s", sevenDaysAgo.Format(time.RFC3339))

	for {
		puller.StoreState()
		log.Printf("listing files. marker: %s", puller.marker)
		s3client := s3.New(puller.auth, aws.USEast)
		bucket := s3client.Bucket(puller.bucket)
		bucket.ReadTimeout = time.Second * 5
		bucket.ConnectTimeout = time.Second * 2
		result, err := bucket.List(puller.prefix, "", puller.marker, 1000)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		downloaders := make(chan int, 8)
		for _, value := range result.Contents {

			puller.marker = value.Key

			fileTime, _ := time.Parse(time.RFC3339, value.LastModified)
			if fileTime.Unix() < sevenDaysAgo.Unix() {
				log.Printf("skipping file: %s, modified: %s", value.Key, value.LastModified)
				continue
			}

			downloaders <- 1
			go func(done chan int) {
				defer func() {
					<-done
				}()
				file, err := puller.Download(value.Key)
				if err == nil {
					log.Printf("sending file %s to queue. %v", file, len(puller.fileChannel))
					puller.fileChannel <- file
				} else {
					log.Printf("%v", err)
				}
			}(downloaders)
		}

		if len(result.Contents) == 0 {
			log.Printf("no new data, sleeping for 5 minutes")
			time.Sleep(5 * time.Minute)
		}

		log.Printf("result.NextMarker: %s", result.NextMarker)

		if result.IsTruncated && result.NextMarker != puller.marker {
			log.Printf("listing more: %s", result.NextMarker)
			puller.marker = result.NextMarker
		} else {
			//break
		}
	}
}

func (puller *LogFilePuller) Download(key string) (string, error) {

	s3client := s3.New(puller.auth, aws.USEast)
	bucket := s3client.Bucket(puller.bucket)

	reader, err := bucket.GetReader(key)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	localFilePath := path.Join(puller.tmpDir, key)
	if strings.LastIndex(localFilePath, "/") > -1 {
		localFilePath = localFilePath[0:strings.LastIndex(localFilePath, "/")]
	}
	os.MkdirAll(localFilePath, 0700)
	localFilePath = path.Join(puller.tmpDir, key)

	log.Printf("downloading '%s' -> '%s'", key, localFilePath)

	writer, err := os.Create(localFilePath)
	if err != nil {
		return "", err
	}
	defer writer.Close()

	io.Copy(writer, reader)

	fmt.Printf("downloaded '%s'\n", localFilePath)

	return localFilePath, nil
}
