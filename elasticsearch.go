package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var knownindexes = map[string]time.Time{}
var mutex = &sync.Mutex{}

type ElasticSearchClient struct {
	Url       string
	BasicAuth string
	Indexes   map[string]string
}

func NewElasticSearchClient(config map[string]interface{}) *ElasticSearchClient {

	url := config["elasticsearch"].(map[string]interface{})["url"].(string)
	basicAuth := config["elasticsearch"].(map[string]interface{})["basic_auth"].(string)
	return &ElasticSearchClient{Url: url, BasicAuth: basicAuth, Indexes: map[string]string{}}
}

func (eclient *ElasticSearchClient) CreateIndex(index string) error {

	index = strings.ToLower(strings.TrimSpace(index))

	if _, exists := knownindexes[index]; exists {
		return nil
	}
	if exists, err := eclient.Check(index); err != nil {
		return err
	} else if exists {
		return nil
	}

	mutex.Lock()
	defer mutex.Unlock()

	if _, exists := knownindexes[index]; exists {
		return nil
	}
	if exists, err := eclient.Check(index); err != nil {
		return err
	} else if exists {
		return nil
	}

	url := fmt.Sprintf("%s/%s?pretty", eclient.Url, index)

	payload := map[string]interface{}{
		"settings": map[string]interface{}{
			"number_of_shards": 1,
		},
		"mappings": map[string]interface{}{
			"_default_": map[string]interface{}{
				"_id": map[string]interface{}{
					"path": "_id",
				},

				"_timestamp": map[string]interface{}{
					"enabled": true,
					"store":   true,
					"path":    "@timestamp",
					"format":  "YYYY-MM-dd'T'HH:mm:ss'Z'", //2015-04-20T20:05:13Z
				},
				"properties": map[string]interface{}{
					"@timestamp": map[string]interface{}{
						"type":   "date",
						"format": "YYYY-MM-dd'T'HH:mm:ss'Z'",
					},
					"host": map[string]interface{}{
						"type":  "string",
						"index": "not_analyzed",
					},
					"ip": map[string]interface{}{
						"type": "ip",
					},
					"path": map[string]interface{}{
						"type":  "string",
						"index": "not_analyzed",
					},
					"verb": map[string]interface{}{
						"type":  "string",
						"index": "not_analyzed",
					},
					"user_agent": map[string]interface{}{
						"type":  "string",
						"index": "not_analyzed",
					},
					"status": map[string]interface{}{
						"type":       "integer",
						"null_value": 0,
					},
					"request_bytes": map[string]interface{}{
						"type":       "integer",
						"null_value": 0,
					},
					"response_bytes": map[string]interface{}{
						"type":       "integer",
						"null_value": 0,
					},
					"response_time": map[string]interface{}{
						"type":       "integer",
						"null_value": 0,
					},
					"city": map[string]interface{}{
						"type":  "string",
						"index": "not_analyzed",
					},
					"coordinates": map[string]interface{}{
						"type": "geo_point",
					},
					"query": map[string]interface{}{
						"type": "object",
					},
					"country": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"IsoCode": map[string]interface{}{
								"type":  "string",
								"index": "not_analyzed",
							},
							"Name": map[string]interface{}{
								"type":  "string",
								"index": "not_analyzed",
							},
						},
					},
					"continent": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"IsoCode": map[string]interface{}{
								"type":  "string",
								"index": "not_analyzed",
							},
							"Name": map[string]interface{}{
								"type":  "string",
								"index": "not_analyzed",
							},
						},
					},
					"isp": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"Name": map[string]interface{}{
								"type":  "string",
								"index": "not_analyzed",
							},
							"Organization": map[string]interface{}{
								"type":  "string",
								"index": "not_analyzed",
							},
						},
					},
				},
			},
		},
	}

	jsonPayload, _ := json.MarshalIndent(payload, "", "  ")

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonPayload))
	req.Header.Set("Authorization", eclient.BasicAuth)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", fmt.Sprintf("%v", len(jsonPayload)))

	infoLogger.Printf("creating index %s using mapping: %s", index, jsonPayload)

	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		infoLogger.Println("response Body:", string(body))
		return fmt.Errorf("unable to create index: %s -> %s", index, string(body))
	}

	infoLogger.Printf("created index: %s -> %s", index, url)

	knownindexes[index] = time.Now()

	return nil
}

func (eclient *ElasticSearchClient) Check(index string) (bool, error) {

	index = strings.ToLower(strings.TrimSpace(index))

	url := fmt.Sprintf("%s/%s?pretty", eclient.Url, index)
	infoLogger.Println("checking: ", url)

	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", eclient.BasicAuth)

	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 200:
		knownindexes[index] = time.Now()
		infoLogger.Printf("index '%s' exists", index)
		eclient.Indexes[strings.ToLower(strings.TrimSpace(index))] = strings.ToLower(strings.TrimSpace(index))
		return true, nil
		break
	case 404:
		infoLogger.Printf("index '%s' doesn't exist", index)
		break
	default:
		body, _ := ioutil.ReadAll(resp.Body)
		var a map[string]interface{}
		if err := json.Unmarshal(body, &a); err == nil {
			if value, exists := a["status"]; exists {
				if str, isString := value.(int); isString && str == 404 {
					log.Printf("index '%s' doesn't exist", index)
					return false, nil
				}
			}
		}
		errLogger.Println("response Body:", string(body))
		return false, fmt.Errorf("%s", string(body))
		break
	}

	return false, nil
}

func (eclient *ElasticSearchClient) Upload(filename string, index string) error {

	if _, exists := knownindexes[index]; !exists {
		infoLogger.Printf("unknown index %s", index)
		if err := eclient.CreateIndex(index); err != nil {
			panic(err)
		}
	}

	info, _ := os.Stat(filename)

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	url := fmt.Sprintf("%s/%s/accesslogentry/_bulk?pretty", eclient.Url, index)
	infoLogger.Printf("uploading: %s -> %s", filename, url)

	req, err := http.NewRequest("POST", url, file)
	req.Header.Set("Authorization", eclient.BasicAuth)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", fmt.Sprintf("%v", info.Size()))

	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		infoLogger.Println("response Body:", string(body))
		return fmt.Errorf("%s", string(body))
	}

	infoLogger.Printf("finished uploading: %s -> %s", filename, url)

	return nil
}
