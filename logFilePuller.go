package main

import (
	"encoding/json"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"io"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
	"time"
)

type LogFilePuller struct {
	auth          aws.Auth
	bucket        string
	marker        string
	prefix        string
	tmpDir        string
	fileChannel   chan string
	statefile     string
	lastDate      time.Time
	processedKeys map[string]time.Time
}

func NewLogFilePuller(fileChannel chan string, config map[string]interface{}) *LogFilePuller {

	access_key := config["source"].(map[string]interface{})["s3"].(map[string]interface{})["access_key"].(string)
	secret_key := config["source"].(map[string]interface{})["s3"].(map[string]interface{})["secret_key"].(string)
	bucket := config["source"].(map[string]interface{})["s3"].(map[string]interface{})["bucket"].(string)
	prefix := config["source"].(map[string]interface{})["s3"].(map[string]interface{})["prefix"].(string)
	tmpdir := config["source"].(map[string]interface{})["tmpdir"].(string)

	os.MkdirAll(tmpdir, 0700)

	return &LogFilePuller{
		auth:          aws.Auth{AccessKey: access_key, SecretKey: secret_key},
		marker:        "",
		prefix:        prefix,
		bucket:        bucket,
		tmpDir:        tmpdir,
		statefile:     "marker.txt",
		fileChannel:   fileChannel,
		processedKeys: make(map[string]time.Time)}
}

func (puller *LogFilePuller) RestoreState() {
	if _, err := os.Stat(puller.statefile); err == nil {
		content, err := ioutil.ReadFile(puller.statefile)
		if err != nil {
			panic(err)
		}

		var state map[string]interface{}
		json.Unmarshal([]byte(content), &state)

		tmp, err := time.Parse(time.RFC3339, state["time"].(string))
		if err != nil {
			panic(err)
		}
		puller.lastDate = tmp
		puller.marker = state["marker"].(string)
	} else {
		errLogger.Printf("%v", err)
	}
}

func (puller *LogFilePuller) StoreState() {
	f, err := os.OpenFile(puller.statefile, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	state := map[string]interface{}{
		"marker": puller.marker,
		"time":   puller.lastDate,
	}
	b, _ := json.Marshal(state)
	if _, err = f.WriteString(string(b)); err != nil {
		panic(err)
	}
	f.Close()
}

func (puller *LogFilePuller) Run() {

	puller.RestoreState()

	keyDateRegexp := regexp.MustCompile("^/?nginx/access/(?P<date>[0-9-]+)/.+$")

	for {
		puller.StoreState()
		infoLogger.Printf("listing files. marker: %s", puller.marker)
		s3client := s3.New(puller.auth, aws.USEast)
		bucket := s3client.Bucket(puller.bucket)
		bucket.ReadTimeout = time.Second * 5
		bucket.ConnectTimeout = time.Second * 2
		//result, err := bucket.List(puller.prefix, "", puller.marker, 1000)
		result, err := bucket.List(puller.prefix, "", puller.marker, 1000)
		if err != nil {
			errLogger.Printf("%v", err)
			continue
		}

		downloaders := make(chan int, 8)
		for _, value := range result.Contents {

			puller.marker = value.Key

			keyDateMatch := keyDateRegexp.FindStringSubmatch(value.Key)
			infoLogger.Printf("%v -> %v", value.Key, keyDateMatch)
			result := make(map[string]string)
			for i, name := range keyDateRegexp.SubexpNames() {
				result[name] = keyDateMatch[i]
			}
			keyDate, err := time.Parse("2006-01-02", result["date"])
			if err != nil {
				errLogger.Printf("skipping file: %s, modified: %s, err: %v", value.Key, value.LastModified, err)
				continue
			}

			if keyDate.Unix() < puller.lastDate.AddDate(0, 0, -1).Unix() {
				infoLogger.Printf("skipping file: %s, modified: %s, too old", value.Key, value.LastModified)
				continue
			}

			if keyDate.Unix() > puller.lastDate.Unix() {
				puller.lastDate = keyDate
			}

			if _, exists := puller.processedKeys[value.Key]; exists {
				infoLogger.Printf("skipping file: %s, modified: %s, already processed", value.Key, value.LastModified)
				continue
			} else {
				puller.processedKeys[value.Key] = time.Now()
			}

			/*
				fileTime, _ := time.Parse(time.RFC3339, value.LastModified)
				if fileTime.Unix() < sevenDaysAgo.Unix() {
					infoLogger.Printf("skipping file: %s, modified: %s", value.Key, value.LastModified)
					continue
				}
			*/

			downloaders <- 1
			go func(done chan int, key string, p *LogFilePuller) {
				defer func() {
					<-done
				}()
				file, err := p.Download(key)
				if err == nil {
					infoLogger.Printf("sending file %s to queue. %v", file, len(p.fileChannel))
					p.fileChannel <- file
				} else {
					delete(p.processedKeys, key)
					errLogger.Printf("%v", err)
				}
			}(downloaders, value.Key, puller)
		}

		infoLogger.Printf("result.NextMarker: %s", result.NextMarker)
		infoLogger.Printf("puller.lastDate: %v, puller.marker: %v, puller.processedKeys: %v", puller.lastDate, puller.marker, len(puller.processedKeys))

		puller.marker = ""

		if result.IsTruncated {
			infoLogger.Printf("listing more: %s", result.NextMarker)
			puller.marker = result.NextMarker
		} else {
			infoLogger.Printf("no more data, sleeping for 5 minutes and starting again")
			time.Sleep(5 * time.Minute)
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

	infoLogger.Printf("downloading '%s' -> '%s'", key, localFilePath)

	writer, err := os.Create(localFilePath)
	if err != nil {
		return "", err
	}
	defer writer.Close()

	if num, err := io.Copy(writer, reader); err != nil {
		return "", err
	} else {
		infoLogger.Printf("downloaded '%s', bytes: %v", localFilePath, num)
	}

	return localFilePath, nil
}
