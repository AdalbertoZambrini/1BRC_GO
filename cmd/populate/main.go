package main

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const batchSize = 10000

type stationProfile struct {
	name     []byte
	mean10x  int
	range10x int
}

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

func buildDefaultStationProfiles() []stationProfile {
	defaultNames := []string{
		"Abha", "Athens", "Bangkok", "Barcelona", "Berlin", "Cairo", "Dakar", "Dallas",
		"Hanoi", "Houston", "Istanbul", "Jakarta", "Kabul", "Lagos", "London", "Madrid",
		"Melbourne", "Miami", "Moscow", "Mumbai", "New York", "Paris", "Rome", "Seoul",
		"Sydney", "Tokyo", "Warsaw",
	}

	profiles := make([]stationProfile, 0, len(defaultNames))
	for i, name := range defaultNames {
		mean10x := 260 - ((i * 15) % 520)
		range10x := 70 + ((i * 9) % 110)
		profiles = append(profiles, stationProfile{
			name:     []byte(name),
			mean10x:  mean10x,
			range10x: range10x,
		})
	}
	return profiles
}

func climateFromLatitude(lat float64) (int, int) {
	absLat := math.Abs(lat)

	meanC := 30.0 - (absLat * 0.5)
	if meanC < -10.0 {
		meanC = -10.0
	}
	if meanC > 32.0 {
		meanC = 32.0
	}

	swingC := 3.0 + (absLat * 0.15)
	if swingC < 3.0 {
		swingC = 3.0
	}
	if swingC > 18.0 {
		swingC = 18.0
	}

	return int(math.Round(meanC * 10.0)), int(math.Round(swingC * 10.0))
}

func buildWeatherStationProfiles() []stationProfile {
	file, err := os.Open("data.csv")
	if err != nil {
		fmt.Printf("Warning: Could not open data.csv (%v). Using default stations.\n", err)
		return buildDefaultStationProfiles()
	}
	defer file.Close()

	stationSet := make(map[string]struct{}, 1024)
	profiles := make([]stationProfile, 0, 1024)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, ";")
		if len(parts) < 2 || parts[0] == "" {
			continue
		}

		if _, exists := stationSet[parts[0]]; exists {
			continue
		}
		lat, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}

		stationSet[parts[0]] = struct{}{}
		mean10x, range10x := climateFromLatitude(lat)
		profiles = append(profiles, stationProfile{
			name:     []byte(parts[0]),
			mean10x:  mean10x,
			range10x: range10x,
		})
	}

	if len(profiles) == 0 {
		fmt.Println("Warning: Could not parse stations from data.csv. Using default stations.")
		return buildDefaultStationProfiles()
	}
	return profiles
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

func estimateFileSize(stations []stationProfile, numRows int) string {
	var totalBytes int
	for _, s := range stations {
		totalBytes += len(s.name)
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

func buildTestData(stations []stationProfile, numRowsToCreate int) {
	startTime := time.Now()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	stationPool10k := make([]stationProfile, 10000)
	for i := 0; i < 10000; i++ {
		stationPool10k[i] = stations[rng.Intn(len(stations))]
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
					station := stationPool10k[localRng.Intn(10000)]

					delta := localRng.Intn(station.range10x*2+1) - station.range10x
					temp := station.mean10x + delta
					if temp < -999 {
						temp = -999
					} else if temp > 999 {
						temp = 999
					}

					b = append(b, station.name...)
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
	weatherStationProfiles := buildWeatherStationProfiles()
	fmt.Println(estimateFileSize(weatherStationProfiles, numRowsToCreate))
	buildTestData(weatherStationProfiles, numRowsToCreate)
	fmt.Println("Test data build complete.")
}
