package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const batchSize = 10000

var bufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, batchSize*30)
		return &b
	},
}

func checkArgs() int {
	if len(os.Args) != 2 {
		printUsage()
	}
	arg := strings.ReplaceAll(os.Args[1], "_", "")
	numRows, err := strconv.Atoi(arg)
	if err != nil || numRows <= 0 {
		printUsage()
	}
	return numRows
}

func printUsage() {
	fmt.Println("Usage:        go run main.go <positive integer number of records to create>")
	fmt.Println("              You can use underscore notation for large number of records.")
	fmt.Println("              For example:  1_000_000_000 for one billion")
	os.Exit(1)
}

func buildWeatherStationNameList() []string {
	file, err := os.Open("data.csv")
	if err != nil {
		fmt.Printf("Warning: Could not open data.csv (%v). Using default stations.\n", err)
		return []string{"Abha", "Athens", "Bangkok", "Barcelona", "Berlin", "Cairo", "Dakar", "Dallas", "Hanoi", "Houston", "Istanbul", "Jakarta", "Kabul", "Lagos", "London", "Madrid", "Melbourne", "Miami", "Moscow", "Mumbai", "New York", "Paris", "Rome", "Seoul", "Sydney", "Tokyo", "Warsaw"}
	}
	defer file.Close()

	stationSet := make(map[string]struct{})
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "#") {
			continue
		}
		parts := strings.Split(line, ";")
		if len(parts) > 0 && parts[0] != "" {
			stationSet[parts[0]] = struct{}{}
		}
	}

	var stations []string
	for s := range stationSet {
		stations = append(stations, s)
	}
	return stations
}

func convertBytes(num float64) string {
	units := []string{"bytes", "KiB", "MiB", "GiB"}
	for _, unit := range units {
		if num < 1024.0 {
			return fmt.Sprintf("%3.1f %s", num, unit)
		}
		num /= 1024.0
	}
	return fmt.Sprintf("%3.1f TiB", num)
}

func formatElapsedTime(seconds float64) string {
	if seconds < 60 {
		return fmt.Sprintf("%.3f seconds", seconds)
	}
	mins := int(seconds) / 60
	secs := int(seconds) % 60
	if seconds < 3600 {
		return fmt.Sprintf("%d minutes %d seconds", mins, secs)
	}
	hours := mins / 60
	mins = mins % 60
	if mins == 0 {
		return fmt.Sprintf("%d hours %d seconds", hours, secs)
	}
	return fmt.Sprintf("%d hours %d minutes %d seconds", hours, mins, secs)
}

func estimateFileSize(stations []string, numRows int) string {
	var totalBytes int
	for _, s := range stations {
		totalBytes += len(s)
	}
	avgNameBytes := float64(totalBytes) / float64(len(stations))
	avgTempBytes := 4.4002
	avgLineLength := avgNameBytes + avgTempBytes + 2

	humanFileSize := convertBytes(float64(numRows) * avgLineLength)
	return fmt.Sprintf("Estimated max file size is:  %s.", humanFileSize)
}

func appendTemp(b []byte, temp10x int) []byte {
	if temp10x < 0 {
		b = append(b, '-')
		temp10x = -temp10x
	}
	if temp10x >= 100 {
		b = append(b, byte('0'+temp10x/100))
		temp10x %= 100
		b = append(b, byte('0'+temp10x/10))
	} else {
		b = append(b, byte('0'+temp10x/10))
	}
	b = append(b, '.')
	b = append(b, byte('0'+temp10x%10))
	return b
}

func buildTestData(stations []string, numRowsToCreate int) {
	startTime := time.Now()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	stationBytes10k := make([][]byte, 10000)
	for i := 0; i < 10000; i++ {
		stationBytes10k[i] = []byte(stations[rng.Intn(len(stations))])
	}

	chunks := numRowsToCreate / batchSize
	if numRowsToCreate%batchSize != 0 {
		chunks++
	}

	fmt.Println("Building test data...")

	file, err := os.Create("measurements.txt")
	if err != nil {
		fmt.Println("Error creating file:", err)
		os.Exit(1)
	}
	defer file.Close()

	dataChan := make(chan *[]byte, runtime.NumCPU()*2)

	doneChan := make(chan bool)

	go func() {
		writer := bufio.NewWriterSize(file, 4*1024*1024)
		progress := 0
		chunkCount := 0

		for b := range dataChan {
			writer.Write(*b)
			chunkCount++

			currentProgress := (chunkCount * 100) / chunks
			if currentProgress != progress && currentProgress <= 100 {
				progress = currentProgress
				bars := strings.Repeat("=", progress/2)
				fmt.Printf("\r[%-50s] %d%%", bars, progress)
			}

			*b = (*b)[:0]
			bufPool.Put(b)
		}
		writer.Flush()
		fmt.Println()
		doneChan <- true
	}()

	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU()
	chunksPerWorker := chunks / numWorkers

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			localRng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			startChunk := workerID * chunksPerWorker
			endChunk := startChunk + chunksPerWorker
			if workerID == numWorkers-1 {
				endChunk = chunks
			}

			for c := startChunk; c < endChunk; c++ {
				bPtr := bufPool.Get().(*[]byte)
				b := *bPtr

				rows := batchSize
				if c == chunks-1 && numRowsToCreate%batchSize != 0 {
					rows = numRowsToCreate % batchSize
				}

				for i := 0; i < rows; i++ {
					station := stationBytes10k[localRng.Intn(10000)]

					temp := localRng.Intn(1999) - 999

					b = append(b, station...)
					b = append(b, ';')
					b = appendTemp(b, temp)
					b = append(b, '\n')
				}
				*bPtr = b
				dataChan <- bPtr
			}
		}(w)
	}

	wg.Wait()
	close(dataChan)
	<-doneChan

	elapsedTime := time.Since(startTime).Seconds()
	fileInfo, err := file.Stat()
	var humanFileSize string
	if err == nil {
		humanFileSize = convertBytes(float64(fileInfo.Size()))
	}

	fmt.Println("Test data successfully written to measurements.txt")
	fmt.Printf("Actual file size:  %s\n", humanFileSize)
	fmt.Printf("Elapsed time: %s\n", formatElapsedTime(elapsedTime))
}

func main() {
	numRowsToCreate := checkArgs()
	weatherStationNames := buildWeatherStationNameList()
	fmt.Println(estimateFileSize(weatherStationNames, numRowsToCreate))
	buildTestData(weatherStationNames, numRowsToCreate)
	fmt.Println("Test data build complete.")
}
