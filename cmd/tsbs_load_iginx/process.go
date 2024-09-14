package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/iznauy/IGinX-client-go/client_v2"
	"github.com/iznauy/IGinX-client-go/rpc"
	"github.com/timescale/tsbs/pkg/targets"
)

// allows for testing
var printFn = fmt.Printf

type processor struct {
	session *client_v2.Session
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func shuffle(nums []int) []int {
	for i := len(nums); i > 0; i-- {
		last := i - 1
		idx := rand.Intn(i)
		nums[last], nums[idx] = nums[idx], nums[last]
	}
	return nums
}

func (p *processor) Init(_ int, _, _ bool) {
	connectionStrings := ""
	numbers := make([]int, 0, len(connectionSocketList))
	for i := 0; i < len(connectionSocketList); i++ {
		numbers = append(numbers, i)
	}
	numbers = shuffle(numbers)
	for i := 0; i < len(numbers); i++ {
		if i > 0 {
			connectionStrings += ","
		}
		connectionStrings += connectionSocketList[numbers[i]]
	}

	settings, err := client_v2.NewSessionSettings(connectionStrings)
	if err != nil {
		log.Fatal(err)
	}

	p.session = client_v2.NewSession(settings)
	if err := p.session.Open(); err != nil {
		log.Fatal(err)
	}
}

func (p *processor) Close(_ bool) {
	if err := p.session.Close(); err != nil {
		log.Fatal(err)
	}
}

func (p *processor) logWithTimeout(doneChan chan int, timeout time.Duration, sqlChan chan string) {
	for {
		var printSQL bool
		select {
		case <-doneChan:
			printSQL = false
		case <-time.After(timeout):
			printSQL = true
		}
		sql := <-sqlChan
		if printSQL {
			lines := strings.Split(sql, "\n")
			lines = lines[0 : len(lines)-1]

			var path []string
			var timestamp int64
			var values [][]interface{}
			var types []rpc.DataType
			i := 0
			for i = 0; i < len(lines); i++ {
				tmp := strings.Split(lines[i], " ")
				tmp[0] = "type=" + tmp[0]
				fir := strings.Split(tmp[0], ",")
				device := ""
				for j := 0; j < len(fir); j++ {
					kv := strings.Split(fir[j], "=")
					device += kv[1]
					device += "."
				}
				timestamp, _ = strconv.ParseInt(tmp[2], 10, 64)
				timestamp /= 1000000
				device = device[0 : len(device)-1]
				device = strings.Replace(device, "-", "_", -1)

				sec := strings.Split(tmp[1], ",")
				for j := 0; j < len(sec); j++ {
					kv := strings.Split(sec[j], "=")
					path = append(path, device+"."+kv[0])
					v, err := strconv.ParseFloat(kv[1], 32)
					if err != nil {
						log.Fatal(err)
					}
					values = append(values, []interface{}{v})
					types = append(types, rpc.DataType_DOUBLE)
				}
			}

			fmt.Println(path)
			fmt.Println(timestamp)
			fmt.Println(values)

			//fmt.Println("try insert again")
			//timestamps := []int64{timestamp}
			//err := p.session.InsertColumnRecords(path, timestamps, values, types)
			//if err != nil {
			//	log.Println(err)
			//	panic(err)
			//}
			//fmt.Println("try insert success")

			//<-doneChan
		}
	}
}

func formatName(name string) string {
	parts := strings.Split(name, "_")
	truck := parts[0]
	index, _ := strconv.Atoi(parts[1])
	return fmt.Sprintf("%s_%04d", truck, index)
}

func parseMeasurementAndValues(measurement string, fields string) ([]string, []float64) {
	var paths []string
	var values []float64

	fir := strings.Split(measurement, ",")
	device := fir[0] + "."
	if !strings.Contains(fir[1], "truck") {
		device += defaultTruck
		device += "."
		fir = fir[1:]
	} else {
		device += formatName(strings.Split(fir[1], "=")[1])
		device += "."
		fir = fir[2:]
	}

	index := 0
	for j := 0; j < len(defaultTagK); j++ {
		if index < len(fir) {
			kv := strings.Split(fir[index], "=")
			if defaultTagK[j] == kv[0] {
				device += strings.Replace(kv[1], ".", "_", -1)
				device += "."
				index++
				continue
			}
		}
		device += defaultTagV[j]
		device += "."
	}
	device = strings.Replace(device, "-", "_", -1)

	sec := strings.Split(fields, ",")
	for j := 0; j < len(sec); j++ {
		kv := strings.Split(sec[j], "=")
		path := device + kv[0]
		path = strings.Replace(path, "-", "_", -1)

		v, err := strconv.ParseFloat(kv[1], 32)
		if err != nil {
			log.Fatal(err)
		}
		paths = append(paths, path)
		values = append(values, v)
	}
	return paths, values
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	beginTime := time.Now().UnixMilli()
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if !doLoad {
		return 0, 0
	}

	lines := strings.Split(batch.buf.String(), "\n")
	lines = lines[0 : len(lines)-1]

	var paths []string
	var timestamps []int64
	var timestampIndices = make(map[int64]int)
	var values [][]interface{}
	var types []rpc.DataType
	var pathIndices = make(map[string]int)

	parts := strings.Split(lines[0], " ")
	fir := strings.Split(parts[0], ",")
	colname := fir[0]

	if colname != "cpu" {
		pathIndices["tags.name"] = len(paths)
		paths = append(paths, "tags.name")
		types = append(types, rpc.DataType_BINARY)

		pathIndices["tags.fleet"] = len(paths)
		paths = append(paths, "tags.fleet")
		types = append(types, rpc.DataType_BINARY)

		pathIndices["tags.driver"] = len(paths)
		paths = append(paths, "tags.driver")
		types = append(types, rpc.DataType_BINARY)

		pathIndices["tags.model"] = len(paths)
		paths = append(paths, "tags.model")
		types = append(types, rpc.DataType_BINARY)

		pathIndices["tags.device_version"] = len(paths)
		paths = append(paths, "tags.device_version")
		types = append(types, rpc.DataType_BINARY)

		pathIndices["tags.fuel_capacity"] = len(paths)
		paths = append(paths, "tags.fuel_capacity")
		types = append(types, rpc.DataType_DOUBLE)

		pathIndices["tags.load_capacity"] = len(paths)
		paths = append(paths, "tags.load_capacity")
		types = append(types, rpc.DataType_DOUBLE)

		pathIndices["tags.nominal_fuel_consumption"] = len(paths)
		paths = append(paths, "tags.nominal_fuel_consumption")
		types = append(types, rpc.DataType_DOUBLE)

		pathIndices["tags.tagid"] = len(paths)
		paths = append(paths, "tags.tagid")
		types = append(types, rpc.DataType_LONG)

		pathIndices["readings.tagid"] = len(paths)
		paths = append(paths, "readings.tagid")
		types = append(types, rpc.DataType_LONG)

		pathIndices["diagnostics.tagid"] = len(paths)
		paths = append(paths, "diagnostics.tagid")
		types = append(types, rpc.DataType_LONG)

		pathIndices["readings.timestamp"] = len(paths)
		paths = append(paths, "readings.timestamp")
		types = append(types, rpc.DataType_LONG)

		pathIndices["diagnostics.timestamp"] = len(paths)
		paths = append(paths, "diagnostics.timestamp")
		types = append(types, rpc.DataType_LONG)

		tagMap := make(map[string]map[string]interface{})

		var performance = []string{"fuel_capacity", "load_capacity", "nominal_fuel_consumption"}

		for _, line := range lines {
			parts := strings.Split(line, " ")
			subPaths, subValues := parseMeasurementAndValues(parts[0], parts[1])

			for i, subPath := range subPaths {
				ifp := false
				items := strings.Split(subPath, ".")
				if _, ok := tagMap[items[1]]; ok {
					for _, p := range performance {
						if p == items[6] {
							tagMap[items[1]][p] = subValues[i]
							ifp = true
							break
						}
					}
				} else {
					timestamp := int64(len(tagMap))
					tagMap[items[1]] = make(map[string]interface{})
					tagMap[items[1]]["tagid"] = timestamp

					timestampIndices[timestamp] = len(timestamps)
					timestamps = append(timestamps, timestamp)

					tagMap[items[1]]["fleet"] = items[2]
					tagMap[items[1]]["driver"] = items[3]
					tagMap[items[1]]["model"] = items[4]
					tagMap[items[1]]["device_version"] = items[5]

					for _, p := range performance {
						if p == items[6] {
							tagMap[items[1]][p] = subValues[i]
							ifp = true
							break
						}
					}
				}
				if !ifp {
					newPath := items[0] + "." + items[6]
					if _, ok := pathIndices[newPath]; !ok {
						pathIndices[newPath] = len(paths)
						paths = append(paths, newPath)
						types = append(types, rpc.DataType_DOUBLE)
					}

					timestamp, _ := strconv.ParseInt(parts[2], 10, 64)
					id := tagMap[items[1]]["tagid"].(int64)
					timestamp += id
					if _, ok := timestampIndices[timestamp]; !ok {
						timestampIndices[timestamp] = len(timestamps)
						timestamps = append(timestamps, timestamp)
					}
				}
			}
		}

		for range paths {
			values = append(values, make([]interface{}, max(len(tagMap), len(timestamps)), max(len(tagMap), len(timestamps))))
		}

		ts := 0
		for key, innerMap := range tagMap {
			secondIndex := 0
			for i := range timestamps {
				if timestamps[i] == int64(ts) {
					secondIndex = i
					break
				}
			}
			path1 := "tags.name"
			firstIndex1 := pathIndices[path1]
			values[firstIndex1][secondIndex] = key

			for innerKey, value := range innerMap {
				path2 := "tags." + innerKey
				firstIndex2 := pathIndices[path2]
				values[firstIndex2][secondIndex] = value
			}
			ts++
		}

		for _, line := range lines {
			parts := strings.Split(line, " ")
			timestamp, _ := strconv.ParseInt(parts[2], 10, 64)
			subPaths, subValues := parseMeasurementAndValues(parts[0], parts[1])
			for i, subPath := range subPaths {
				items := strings.Split(subPath, ".")
				ifp := false
				for _, p := range performance {
					if p == items[6] {
						ifp = true
						break
					}
				}

				secondIndex := 0
				id := tagMap[items[1]]["tagid"].(int64)
				ts := timestamp + id
				for i := range timestamps {
					if timestamps[i] == ts {
						secondIndex = i
						break
					}
				}

				if !ifp {
					path1 := items[0] + "." + items[6]
					firstIndex1 := pathIndices[path1]
					values[firstIndex1][secondIndex] = subValues[i]

					path2 := items[0] + "." + "tagid"
					firstIndex2 := pathIndices[path2]
					values[firstIndex2][secondIndex] = tagMap[items[1]]["tagid"]

					path3 := items[0] + "." + "timestamp"
					firstIndex3 := pathIndices[path3]
					values[firstIndex3][secondIndex] = timestamp
				}
			}
		}
	} else {
		pathIndices["cpu.hostname"] = len(paths)
		paths = append(paths, "cpu.hostname")
		types = append(types, rpc.DataType_BINARY)

		pathIndices["cpu.usage_user"] = len(paths)
		paths = append(paths, "cpu.usage_user")
		types = append(types, rpc.DataType_LONG)

		pathIndices["cpu.usage_system"] = len(paths)
		paths = append(paths, "cpu.usage_system")
		types = append(types, rpc.DataType_LONG)

		pathIndices["cpu.usage_idle"] = len(paths)
		paths = append(paths, "cpu.usage_idle")
		types = append(types, rpc.DataType_LONG)

		pathIndices["cpu.usage_nice"] = len(paths)
		paths = append(paths, "cpu.usage_nice")
		types = append(types, rpc.DataType_LONG)

		pathIndices["cpu.usage_iowait"] = len(paths)
		paths = append(paths, "cpu.usage_iowait")
		types = append(types, rpc.DataType_LONG)

		pathIndices["cpu.usage_irq"] = len(paths)
		paths = append(paths, "cpu.usage_irq")
		types = append(types, rpc.DataType_LONG)

		pathIndices["cpu.usage_softirq"] = len(paths)
		paths = append(paths, "cpu.usage_softirq")
		types = append(types, rpc.DataType_LONG)

		pathIndices["cpu.usage_steal"] = len(paths)
		paths = append(paths, "cpu.usage_steal")
		types = append(types, rpc.DataType_LONG)

		pathIndices["cpu.usage_guest"] = len(paths)
		paths = append(paths, "cpu.usage_guest")
		types = append(types, rpc.DataType_LONG)

		pathIndices["cpu.usage_guest_nice"] = len(paths)
		paths = append(paths, "cpu.usage_guest_nice")
		types = append(types, rpc.DataType_LONG)

		pathIndices["cpu.timestamp"] = len(paths)
		paths = append(paths, "cpu.timestamp")
		types = append(types, rpc.DataType_LONG)

		for _, line := range lines {
			parts := strings.Split(line, " ")
			timestamp, _ := strconv.ParseInt(parts[2], 10, 64)
			host := strings.Split(parts[0], ",")[1]
			id := strings.Split(host, "_")[1]
			tid, _ := strconv.ParseInt(id, 10, 64)
			timestamp += tid
			if _, ok := timestampIndices[timestamp]; !ok {
				timestampIndices[timestamp] = len(timestamps)
				timestamps = append(timestamps, timestamp)
			}
		}

		for range paths {
			values = append(values, make([]interface{}, len(timestamps), len(timestamps)))
		}

		for _, line := range lines {
			parts := strings.Split(line, " ")
			timestamp, _ := strconv.ParseInt(parts[2], 10, 64)
			host := strings.Split(parts[0], ",")[1]
			id := strings.Split(host, "_")[1]
			tid, _ := strconv.ParseInt(id, 10, 64)
			ts := timestamp + tid
			secondIndex := 0
			for i := range timestamps {
				if timestamps[i] == ts {
					secondIndex = i
					break
				}
			}
			path0 := "cpu.hostname"
			firstIndex0 := pathIndices[path0]
			values[firstIndex0][secondIndex] = strings.Split(host, "=")[1]

			path1 := "cpu.timestamp"
			firstIndex1 := pathIndices[path1]
			values[firstIndex1][secondIndex] = timestamp

			items := strings.Split(parts[1], ",")
			for _, item := range items {
				key := strings.Split(item, "=")[0]
				value := strings.Split(item, "=")[1]

				path := "cpu." + key
				firstIndex := pathIndices[path]
				v, _ := strconv.ParseInt(value, 10, 64)
				values[firstIndex][secondIndex] = v
			}
		}
	}

	var err error
	for i := 0; i < 3; i++ {
		err = p.session.InsertNonAlignedColumnRecords(paths, timestamps, values, types, nil)
		if err == nil {
			break
		}
	}
	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	span := time.Now().UnixMilli() - beginTime
	if err != nil {
		log.Printf("[write stats] Span = %dms, Failure: %v\n", span, err)
		return 0, 0
	}
	log.Printf("[write stats] Span = %dms, Success\n", span)
	return metricCnt, uint64(rowCnt)
}
