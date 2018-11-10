package main

import (
	"edgar-case/extraction/pkg/downloader"
	"flag"
	"os"
	"path/filepath"
	"time"
)

func main(){

	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	path := filepath.Dir(ex) + "\\indexes"
	currentTime := time.Now()

	startYear := flag.Int("syear", currentTime.Year(),
		"The year from which to start downloading the filing index. Default to current year")

	endYear := flag.Int("eyear", currentTime.Year(),
		"The year to stop (including this year) downloading the filing index. Default to current year")

	dirPtr := flag.String("directory", path,
		"A directory where all the filing index files will be downloaded to. Default to a temporary directory.")

	flag.Parse()

	crawler := downloader.Crawler{
		SaveDirectory: *dirPtr,
		StartYear: *startYear,
		EndYear: *endYear,
	}

	crawler.DownloadIndexFiles()
	crawler.MergeIndexFiles()
}
