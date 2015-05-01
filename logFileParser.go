package main

import (
	"bufio"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"fmt"
	"github.com/oschwald/geoip2-golang"
	"net"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type HostLogFile struct {
	Host    string
	Index   string
	Path    string
	Lines   int
	Created time.Time
	Output  chan *HostLogFile
	Buffer  []string
}

type LogFileParser struct {
	Id           string
	tmpDir       string
	tmpHostFiles map[string]*HostLogFile
	Output       chan *HostLogFile
	GeoipReader  *geoip2.Reader
	config       map[string]interface{}
}

func NewLogFileParser(output chan *HostLogFile, geoipreader *geoip2.Reader, config map[string]interface{}) *LogFileParser {
	tmpdir := config["parser"].(map[string]interface{})["tmpdir"].(string)
	id := uuid.New()
	a := &LogFileParser{tmpDir: tmpdir, tmpHostFiles: map[string]*HostLogFile{}, Output: output, Id: id, GeoipReader: geoipreader, config: config}
	os.MkdirAll(a.tmpDir, 0700)
	return a
}

type ISP struct {
	Name         string
	Organization string
}

type Location struct {
	IsoCode string
	Name    string
}

type IndexableLogFile struct {
	Id            string            `json:"_id,omitempty"`
	Timestamp     string            `json:"@timestamp"`
	Host          string            `json:"host,omitempty"`
	IP            string            `json:"ip,omitempty"`
	Path          string            `json:"path,omitempty"`
	Query         map[string]string `json:"query,omitempty"`
	Verb          string            `json:"verb,omitempty"`
	Status        int               `json:"status"`
	RequestBytes  int               `json:"request_bytes"`
	ResponseBytes int               `json:"response_bytes"`
	ResponseTime  int               `json:"response_time"`
	UserAgent     string            `json:"user_agent,omitempty"`
	City          string            `json:"city, omitempty"`
	Country       Location          `json:"country, omitempty"`
	Continent     Location          `json:"continent, omitempty"`
	Location      interface{}       `json:"location, omitempty"`
	ISP           ISP               `json:"isp, omitempty"`
	Coordinates   string            `json:"coordinates,omitempty"`
}

type RawAccessLogLine struct {
	Host           string `json:"host,omitempty"`
	ForwardedFor   string `json:"http_x_forwarded_for"`
	LocalTime      string `json:"time_local"`
	Request        string `json:"request"`
	StatusCode     string `json:"status"`
	RequestLength  string `json:"request_length"`
	ResponseLength string `json:"bytes_sent"`
	UserAgent      string `json:"user_agent"`
	ReponseTime    string `json:"request_time"`
}

func (line *RawAccessLogLine) ToIndexable(id string, georeader *geoip2.Reader) (*IndexableLogFile, error) {

	timestamp, err := time.Parse("02/Jan/2006:15:04:05 -0700", line.LocalTime)
	if err != nil {
		errLogger.Printf("%v\n", err)
		panic(err)
	}

	s := strings.Split(line.Request, " ")
	verb := ""
	path := ""
	query := ""
	if len(s) > 2 {
		verb = s[0]
		path = s[1]
	} else {
		path = s[0]
	}

	if strings.Index(path, "?") >= 0 {
		query = path[strings.Index(path, "?")+1:]
		path = path[0:strings.Index(path, "?")]
	}

	status, err := strconv.Atoi(line.StatusCode)
	if err != nil {
		return nil, err
	}

	reqBytes, err := strconv.Atoi(line.RequestLength)
	if err != nil {
		return nil, err
	}

	respBytes, err := strconv.Atoi(line.ResponseLength)
	if err != nil {
		return nil, err
	}

	respTimef, err := strconv.ParseFloat(line.ReponseTime, 64)
	if err != nil {
		return nil, err
	}
	respTime := int(respTimef * 1000)

	ips := strings.Split(line.ForwardedFor, ",")

	if len(ips) <= 0 {
		return nil, fmt.Errorf("no ip found: %v", line)
	}

	ip := strings.Trim(ips[len(ips)-1], " ")

	//log.Printf("IP: %s", ip)

	ispinfo, _ := georeader.ISP(net.ParseIP(ip))
	location, _ := georeader.City(net.ParseIP(ip))

	queryMap := parseQueryString(query)

	getOrDefault := func(m map[string]string, key string, def string) string {
		if val, exists := m["en"]; exists {
			return val
		} else {
			return def
		}
	}

	return &IndexableLogFile{
		Id:            id,
		Coordinates:   fmt.Sprintf("%v,%v", location.Location.Latitude, location.Location.Longitude),
		ISP:           ISP{ispinfo.ISP, ispinfo.Organization},
		Continent:     Location{location.Continent.Code, getOrDefault(location.Continent.Names, "en", "unknown")},
		Country:       Location{location.Country.IsoCode, getOrDefault(location.Country.Names, "en", "unknown")},
		City:          getOrDefault(location.City.Names, "en", "unknown"),
		Location:      location.Location,
		Host:          strings.ToLower(line.Host),
		IP:            strings.ToLower(line.ForwardedFor),
		Timestamp:     timestamp.UTC().Format(time.RFC3339),
		Path:          strings.ToLower(path),
		Query:         queryMap,
		Verb:          strings.ToUpper(verb),
		Status:        status,
		RequestBytes:  reqBytes,
		ResponseBytes: respBytes,
		ResponseTime:  respTime,
		UserAgent:     strings.ToLower(line.UserAgent),
	}, nil

}

func (parser *LogFileParser) Watch(fileChannel chan string) {

	threads := make(chan int, 8)
	run := func(file string, done chan int, parser *LogFileParser) {
		p := NewLogFileParser(parser.Output, parser.GeoipReader, parser.config)
		defer func() {
			p.Flush()
			os.Remove(file)
			<-done
		}()
		if err := p.ParseFile(file); err != nil {
			errLogger.Printf("unbale to parse file: %v, error: %v", file, err)
		}
	}

	timeout := make(chan int, 1)

	for {
		go func() {
			time.Sleep(1 * time.Second)
			timeout <- 1
		}()
		select {
		case file := <-fileChannel:
			threads <- 1
			infoLogger.Printf("got file %s", file)
			go run(file, threads, parser)
			break
		case <-timeout:
			break
		}
	}
}

func (parser *LogFileParser) ParseFile(filePath string) error {

	infoLogger.Printf("parsing file %s", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		errLogger.Printf("opening file: %s, error: %v", filePath, err)
		return err
	}
	defer file.Close()

	linenumber := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {

		linenumber++

		if err := scanner.Err(); err != nil {
			errLogger.Printf("reading file: %s, error: %v", filePath, err)
			return err
		}

		line := scanner.Text()

		data, err := parser.ParseLine(line, filePath, linenumber)
		if err != nil {
			errLogger.Printf("parsing line: %s, error: %v", line, err)
			continue
		}

		data.Host = strings.ToLower(strings.TrimSpace(data.Host))

		parser.Store(data)

		//fmt.Println(scanner.Text()) // Println will add back the final '\n'
	}

	return nil
}

func (parser *LogFileParser) Flush() {
	infoLogger.Printf("flushing: %v", parser.Id)
	for _, value := range parser.tmpHostFiles {
		infoLogger.Printf("flushing file '%s'", value.Path)
		value.Flush()
		parser.Output <- value
	}
}

func (parser *LogFileParser) Store(logfile *IndexableLogFile) error {
	jsonBytes, err := json.Marshal(logfile)
	if err != nil {
		return nil
	}

	if v, exists := parser.tmpHostFiles[logfile.Index()]; !exists {
		parser.tmpHostFiles[logfile.Index()] = parser.NewTmpFile(logfile)
	} else if v.Lines > 20000 {
		infoLogger.Printf("purging file '%s'", v.Path)
		v.Flush()
		parser.Output <- v
		parser.tmpHostFiles[logfile.Index()] = parser.NewTmpFile(logfile)
	}

	line1map := map[string]interface{}{
		"index": map[string]interface{}{
			"_id": logfile.Id,
		},
	}
	line1mapbytes, _ := json.Marshal(line1map)
	if err := parser.tmpHostFiles[logfile.Index()].Append(string(line1mapbytes)); err != nil {
		errLogger.Println(err.Error())
		return err
	}
	if err := parser.tmpHostFiles[logfile.Index()].Append(string(jsonBytes)); err != nil {
		errLogger.Println(err.Error())
		return err
	}
	return nil
}

func (appender *HostLogFile) Append(line string) error {
	appender.Buffer = append(appender.Buffer, line)
	appender.Lines++
	if len(appender.Buffer) > 20000 {
		appender.Flush()
	}
	return nil
}

func (appender *HostLogFile) Flush() error {

	infoLogger.Printf("flushing %v lines -> %s", len(appender.Buffer), appender.Path)

	f, err := os.OpenFile(appender.Path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	defer func() {
		appender.Buffer = make([]string, 0)
	}()

	for _, line := range appender.Buffer {
		if _, err = f.WriteString(line + "\n"); err != nil {
			return err
		}
	}

	return nil
}

func (logfile *IndexableLogFile) Index() string {
	logtime, _ := time.Parse(time.RFC3339, logfile.Timestamp)
	index := fmt.Sprintf("accesslogs.%s", logtime.Format("2006.01.02"))
	return index
}

func (parser *LogFileParser) NewTmpFile(logfile *IndexableLogFile) *HostLogFile {
	file := path.Join(parser.tmpDir, fmt.Sprintf("%s_%s_%s_%v.log", strings.ToLower(strings.TrimSpace(logfile.Host)), logfile.Index(), parser.Id, time.Now().Unix()))
	return &HostLogFile{Path: file, Lines: 0, Created: time.Now(), Host: logfile.Host, Index: logfile.Index(), Buffer: []string{}}
}

func (parser *LogFileParser) ParseLine(line string, filename string, linenumber int) (*IndexableLogFile, error) {

	if strings.LastIndex(line, ",") == len(line)-1 {
		line = line[0 : len(line)-1]
	}

	rawLogEntry := &RawAccessLogLine{}
	err := json.Unmarshal([]byte(line), rawLogEntry)

	if err != nil {
		errLogger.Printf("unable to parse json '%s', err: %v", line, err)
		return nil, err
	}

	idRegexp := regexp.MustCompile("[^0-9a-zA-Z]+")
	id := idRegexp.ReplaceAllString(fmt.Sprintf("%v:%v", filename, linenumber), "_")
	converted, err := rawLogEntry.ToIndexable(id, parser.GeoipReader)

	if err != nil {
		errLogger.Printf("%s => %v", line, err)
		return nil, err
	}

	//index(converted)

	return converted, nil
}

func parseQueryString(str string) map[string]string {
	str = strings.Replace(str, "\u0026", "&", -1)
	list := strings.Split(str, "&")
	m := map[string]string{}
	for _, pair := range list {
		if strings.Index(pair, "=") < 0 {
			m[pair] = ""
			continue
		}
		key := pair[0:strings.Index(pair, "=")]
		value, _ := url.QueryUnescape(pair[strings.Index(pair, "=")+1:])
		m[key] = value
	}
	return m
}
