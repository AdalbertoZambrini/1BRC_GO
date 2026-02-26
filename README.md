# 1BRC-GO - 1BRC-Inspired Temperature Aggregator

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-0A7EA4.svg)](LICENSE)
[![Data License: CC BY 4.0](https://img.shields.io/badge/Data%20License-CC%20BY%204.0-4A9B5B.svg)](https://creativecommons.org/licenses/by/4.0/)
[![Go Version](https://img.shields.io/badge/Go-1.25.0-00ADD8?logo=go&logoColor=white)](go.mod)

This repository contains a high-performance Go implementation inspired by the **One Billion Row Challenge (1BRC)**.
It processes a large `measurements.txt` file with weather station readings and computes:

- minimum temperature per station
- average temperature per station
- maximum temperature per station

Output is printed in the classic 1BRC shape:

```text
{StationA=min/avg/max, StationB=min/avg/max, ...}
```

At the end, the program also prints total processing time.

## Requirements

- Go 1.25+ (`go.mod` uses `go 1.25.0`)

## Project Structure

- `main.go`: optimized parser and aggregator
- `cmd/populate/main.go`: dataset generator for `measurements.txt`
- `data.csv`: source of weather station names used by the generator
- `measurements.txt`: input file consumed by `main.go`

## Quick Start

1. Generate a dataset (`measurements.txt`) from the project root:

```bash
go run ./cmd/populate 1_000_000_000
```

2. Run the aggregator:

```bash
go run .
```

3. Check output:
- aggregated results per station (alphabetically sorted)
- final timing line, e.g. `Time to process: 8.7498333s`

## How to Create `measurements.txt` (Step by Step)

1. Open a terminal in the repository root.
2. Run the generator with the number of rows you want:

```bash
go run ./cmd/populate 1_000_000_000
```

3. Wait for generation to complete (it shows progress and elapsed time).
4. Confirm the file was created:

PowerShell:

```powershell
Get-Item .\measurements.txt | Select-Object Length, FullName
```

Bash:

```bash
ls -lh measurements.txt
```

5. Process the generated file:

```bash
go run .
```

## Performance Approach in This Implementation

- chunked file reading (`16 MiB` chunks)
- manual byte-level parsing (avoids `Scanner` + `ParseFloat` in the hot path)
- fixed-point temperatures (tenths as integers)
- worker-parallel aggregation (`GOMAXPROCS`-based)
- memory reuse with `sync.Pool`
- final merge + sorted output formatting

## Local Benchmark

Measured on:

- CPU: Intel Core i7-9700KF @ 3.60GHz
- RAM: 32 GB
- Result: `Time to process: 8.5701223s`

## About the Original 1BRC

The original **One Billion Row Challenge** was created by Gunnar Morling as a Java performance challenge.

- Challenge window: **January 1, 2024 to January 31, 2024**
- Submissions closed: **February 1, 2024**
- Final leaderboard published: **February 4, 2024**

Core task (original challenge):

- input rows in the format `<station>;<temperature>` with one decimal digit
- compute min/mean/max per station
- output sorted alphabetically by station name
- optimize for fastest end-to-end execution

This repository is a Go implementation inspired by that problem format and performance spirit.

## License and Attribution

- Project source code is distributed under the Apache License 2.0 (`LICENSE`).
- This project is inspired by the original 1BRC challenge by Gunnar Morling:
  https://github.com/gunnarmorling/1brc
- `data.csv` is adapted from SimpleMaps world cities data and is licensed under
  Creative Commons Attribution 4.0:
  https://creativecommons.org/licenses/by/4.0/
- Additional attribution details are documented in `NOTICE`.

## References

- 1BRC repository: https://github.com/gunnarmorling/1brc/tree/main
- 1BRC announcement: https://www.morling.dev/blog/one-billion-row-challenge/
- 1BRC final results: https://www.morling.dev/blog/1brc-results-are-in/
