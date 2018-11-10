package downloader

import (
	"archive/zip"
	"bufio"
	"bytes"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	EDGAR_PREFIX = "https://www.sec.gov/Archives/edgar/full-index/"
	SEP = "|"
	MAXRETRY = 5
	MAXRETRIEVALSIZE = 5000
)

type Crawler struct {
	SaveDirectory string
	StartYear int
	EndYear int
}

type HTTPResponse struct {
	url string
	status string
	err error
}

func (c *Crawler) DownloadIndexFiles() {
	log.Println("Started downloading from year:", c.StartYear)
	log.Println("Downloaded files will be saved to:", c.SaveDirectory)

	if _, err := os.Stat(c.SaveDirectory); os.IsNotExist(err) {
		os.Mkdir(c.SaveDirectory, 0700)
	}

	urls := generateURLs(c.StartYear, c.EndYear)
	downloadFiles(urls, c.SaveDirectory)
}

func (c *Crawler) MergeIndexFiles() {
	files, err := ioutil.ReadDir(c.SaveDirectory)
	if err != nil {
		log.Println("Error")
	}

	// create new master index file
	masterFile, err := os.OpenFile(c.SaveDirectory + "/master.tsv", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		log.Println("Error creating master.tsv")
	}
	defer masterFile.Close()

	for _, file := range files {
		path := c.SaveDirectory + "/" + file.Name()
		mergeFiles(path, masterFile)
	}
}

func mergeFiles(filePath string, masterFile *os.File) {
	f, err := os.OpenFile(filePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("open file error: %v", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	w := bufio.NewWriter(masterFile)
	counter := 0
	for sc.Scan() {
		// First 11 lines in all QTR files contain unnecessary data so lets skip them
		if counter > 10 {
			line := sc.Text() + "\n"
			w.Write([]byte(line))
		}
		counter += 1
	}

	w.Flush()

	if err := sc.Err(); err != nil {
		log.Fatalf("scan file error: %v", err)
	}
}

func downloadFiles(urls []string, path string) {
	ch := make(chan HTTPResponse, len(urls))

	for _, URL := range urls {
		go func(URL string, path string, ch chan<- HTTPResponse) {
			downloadFile(URL, path, ch)
		}(URL, path, ch)
	}

	for range urls {
		log.Println(<-ch)
	}
}

func downloadFile(url string, filePath string, ch chan<- HTTPResponse) {
	// create file
	s := strings.Split(url, "/")
	fileName := s[len(s) - 3] + "-" + s[len(s)-2] + ".tsv"
	out, err := os.Create(filePath + "/" + fileName)
	if err != nil {
		ch <- HTTPResponse{url, "", err}
		return
	}
	defer out.Close()

	// get data
	resp, err := http.Get(url)
	if err != nil {
		ch <- HTTPResponse{url, resp.Status, err}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		ch <- HTTPResponse{url, resp.Status, err}
		return
	}

	// open zip file
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		ch <- HTTPResponse{url, resp.Status, err}
		return
	}
	// read zip file
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		ch <- HTTPResponse{url, resp.Status, err}
		return
	}

	// loop through each file inside zip
	for _, zipFile := range zipReader.File {
		if zipFile.Name == "master.idx" {
			unzippedBytes, err := readZipFile(zipFile)
			if err != nil {
				ch <- HTTPResponse{url, resp.Status, err}
				return
			}
			// write data to new file
			_, err = out.Write(unzippedBytes)
			if err != nil {
				ch <- HTTPResponse{url, resp.Status, err}
				return
			}
		}
	}

	ch <- HTTPResponse{url, resp.Status, err}
}

func readZipFile(zf *zip.File) ([]byte, error) {
	f, err := zf.Open()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ioutil.ReadAll(f)
}

func generateURLs(start, end int) (urls []string) {
	currentYear := time.Now().Year()
	currentQuarter := int(math.RoundToEven(float64(time.Now().Month()) / 4 + 1))
	years := createRange(start, end)
	quarters := []string {"QTR1", "QTR2", "QTR3", "QTR4"}

	var endPoints []string
	for i := range years {
		for j := range quarters {
			if years[i] == currentYear && j > (currentQuarter - 1) {
				break
			}
			y := strconv.Itoa(years[i])
			endpoint := EDGAR_PREFIX + y + "/" + quarters[j] + "/master.zip"
			endPoints = append(endPoints, endpoint)
		}
	}
	return endPoints
}

func createRange(min, max int) []int {
	r := make([]int, max-min + 1)
	for i := range r {
		r[i] = min + i
	}
	return r
}