package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const (
	inputFileName = "measurements.txt"
	readChunkSize = 16 << 20
)

var chunkPool = sync.Pool{
	New: func() any {
		b := make([]byte, readChunkSize+1024)
		return &b
	},
}

type measurement struct {
	min   int32
	max   int32
	sum   int64
	count int64
}

type parseJob struct {
	buf    []byte
	holder *[]byte
}

func main() {
	start := time.Now()

	file, err := os.Open(inputFileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	workerCount := runtime.GOMAXPROCS(0)
	if workerCount < 1 {
		workerCount = 1
	}

	jobs := make(chan parseJob, workerCount*4)
	workerMaps := make([]map[string]*measurement, workerCount)

	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		local := make(map[string]*measurement, 512)
		workerMaps[i] = local

		go func(data map[string]*measurement) {
			defer wg.Done()

			for job := range jobs {
				parseChunk(job.buf, data)
				chunkPool.Put(job.holder)
			}
		}(local)
	}

	if err := dispatchJobs(file, jobs); err != nil {
		panic(err)
	}
	close(jobs)
	wg.Wait()

	merged := mergeMeasurements(workerMaps)
	printResults(merged)
	fmt.Printf("Time to process: %s\n", time.Since(start))
}

func dispatchJobs(file *os.File, jobs chan<- parseJob) error {
	carry := make([]byte, 0, 1024)

	for {
		holder := chunkPool.Get().(*[]byte)
		buf := *holder

		required := len(carry) + readChunkSize
		if cap(buf) < required {
			buf = make([]byte, required)
			*holder = buf
		}
		buf = buf[:required]

		copy(buf, carry)

		n, err := file.Read(buf[len(carry):])
		total := len(carry) + n

		if total == 0 {
			chunkPool.Put(holder)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			continue
		}

		data := buf[:total]
		lastNewline := bytes.LastIndexByte(data, '\n')
		if lastNewline < 0 {
			carry = append(carry[:0], data...)
			chunkPool.Put(holder)
		} else {
			carry = append(carry[:0], data[lastNewline+1:]...)
			jobs <- parseJob{
				buf:    data[:lastNewline+1],
				holder: holder,
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	if len(carry) > 0 {
		holder := chunkPool.Get().(*[]byte)
		buf := *holder

		required := len(carry) + 1
		if cap(buf) < required {
			buf = make([]byte, required)
			*holder = buf
		}

		buf = buf[:required]
		copy(buf, carry)
		buf[len(carry)] = '\n'

		jobs <- parseJob{
			buf:    buf,
			holder: holder,
		}
	}

	return nil
}

func parseChunk(chunk []byte, data map[string]*measurement) {
	for i, n := 0, len(chunk); i < n; {
		nameStart := i
		for i < n && chunk[i] != ';' && chunk[i] != '\n' && chunk[i] != '\r' {
			i++
		}
		if i >= n {
			break
		}
		if chunk[i] != ';' {
			i = skipToNextLine(chunk, i, n)
			continue
		}

		nameBytes := chunk[nameStart:i]
		i++
		if len(nameBytes) == 0 {
			i = skipToNextLine(chunk, i, n)
			continue
		}

		temp, next, ok := parseTemperature(chunk, i, n)
		i = next
		if !ok {
			continue
		}

		name := bytesToString(nameBytes)
		m := data[name]
		if m == nil {
			key := string(nameBytes)
			data[key] = &measurement{
				min:   temp,
				max:   temp,
				sum:   int64(temp),
				count: 1,
			}
			continue
		}

		if temp < m.min {
			m.min = temp
		}
		if temp > m.max {
			m.max = temp
		}
		m.sum += int64(temp)
		m.count++
	}
}

func parseTemperature(chunk []byte, i, n int) (int32, int, bool) {
	negative := false
	if i < n && chunk[i] == '-' {
		negative = true
		i++
	}
	if i >= n {
		return 0, i, false
	}

	var temp int32
	hasIntDigit := false
	for i < n {
		c := chunk[i]
		if c >= '0' && c <= '9' {
			temp = temp*10 + int32(c-'0')
			hasIntDigit = true
			i++
			continue
		}
		if c == '.' {
			i++
			break
		}
		return 0, skipToNextLine(chunk, i, n), false
	}
	if !hasIntDigit || i >= n || chunk[i] < '0' || chunk[i] > '9' {
		return 0, skipToNextLine(chunk, i, n), false
	}

	temp = temp*10 + int32(chunk[i]-'0')
	i++
	if negative {
		temp = -temp
	}

	if i < n {
		switch chunk[i] {
		case '\n':
			i++
		case '\r':
			i++
			if i < n && chunk[i] == '\n' {
				i++
			}
		default:
			i = skipToNextLine(chunk, i, n)
		}
	}

	return temp, i, true
}

func skipToNextLine(chunk []byte, i, n int) int {
	for i < n && chunk[i] != '\n' {
		i++
	}
	if i < n {
		i++
	}
	return i
}

func mergeMeasurements(parts []map[string]*measurement) map[string]measurement {
	merged := make(map[string]measurement, 1024)
	for _, part := range parts {
		for name, mPtr := range part {
			m := *mPtr
			acc, ok := merged[name]
			if !ok {
				merged[name] = m
				continue
			}

			if m.min < acc.min {
				acc.min = m.min
			}
			if m.max > acc.max {
				acc.max = m.max
			}
			acc.sum += m.sum
			acc.count += m.count
			merged[name] = acc
		}
	}
	return merged
}

func printResults(data map[string]measurement) {
	names := make([]string, 0, len(data))
	for name := range data {
		names = append(names, name)
	}
	sort.Strings(names)

	var out strings.Builder
	out.Grow(len(names) * 32)
	out.WriteByte('{')

	for i, name := range names {
		if i > 0 {
			out.WriteString(", ")
		}

		m := data[name]
		avg := (float64(m.sum) / float64(m.count)) / 10.0
		avg = math.Round(avg*10.0) / 10.0
		if avg == 0 {
			avg = 0
		}
		out.WriteString(name)
		out.WriteByte('=')
		out.WriteString(fmt.Sprintf("%.1f/%.1f/%.1f",
			float64(m.min)/10.0,
			avg,
			float64(m.max)/10.0,
		))
	}
	out.WriteByte('}')

	fmt.Println(out.String())
}

func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
