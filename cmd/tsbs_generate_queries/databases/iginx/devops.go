package iginx

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

// Devops produces Iginx-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

// getSelectAggClauses builds specified aggregate function clauses for
// a set of column idents.
//
// For instance:
//      max(cpu_time) AS max_cpu_time
func (d *Devops) getSelectAggClauses(aggFunc string, idents []string) []string {
	selectAggClauses := make([]string, len(idents))
	for i, ident := range idents {
		selectAggClauses[i] =
			fmt.Sprintf("%[1]s(%[2]s) AS %[1]s_%[2]s", aggFunc, ident)
	}
	return selectAggClauses
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for N random
// hosts
//
// Queries:
// cpu-max-all-1
// cpu-max-all-8
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)
	hosts, err := d.GetRandomHosts(nHosts)
	metrics := devops.GetAllCPUMetrics()
	panicIfErr(err)

	sql := fmt.Sprintf(`{
  		"start_absolute": %d,
  		"end_absolute": %d,
  		"time_zone": "Asia/Kabul",
  		"metrics": [
	`,

		interval.StartUnixMillis(),
		interval.EndUnixMillis())
	i := 0
	for i = 0; i < len(metrics); i++ {
		sql += fmt.Sprintf(`

		{
      		"name": "%s",
      		"aggregators": [
			{
          		"name": "max",
				"tags": {
            		"hostname": [
              			"%s"
            		],
            		"type": [
              			"cpu"
            		]
          		},
          		"sampling": {
            		"value": 1,
            		"unit": "hours"
          		}
        	}]
    	}
	`,
			metrics[i],
			strings.Join(hosts, "\", \""))
		if i != len(metrics)-1 {
			sql += ","
		}
	}
	sql += "]}"
	humanLabel := devops.GetMaxAllLabel("Iginx", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// GroupByTimeAndPrimaryTag selects the AVG of metrics in the group `cpu` per device
// per hour for a day
//
// Queries:
// double-groupby-1
// double-groupby-5
// double-groupby-all
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)

	sql := fmt.Sprintf(`{
  		"start_absolute": %d,
  		"end_absolute": %d,
  		"time_zone": "Asia/Kabul",
  		"metrics": [
	`,
		interval.StartUnixMillis(),
		interval.EndUnixMillis())
	i := 0
	for i = 0; i < len(metrics); i++ {
		sql += fmt.Sprintf(`

		{
      		"name": "%s",
      		"aggregators": [
			{
          		"name": "avg",
				"tags": {
            		"hostname": [
              			"*"
            		],
            		"type": [
              			"cpu"
            		]
          		},
          		"sampling": {
            		"value": 1,
            		"unit": "hours"
          		}
        	}]
    	}
	`,
			metrics[i])
		if i != len(metrics)-1 {
			sql += ","
		}
	}
	sql += "]}"

	humanLabel := devops.GetDoubleGroupByLabel("Iginx", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause,
// that groups by a truncated date, orders by that date, and takes a limit:
//
// Queries:
// groupby-orderby-limit
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)

	metrics := [1]string{"usage_user"}

	sql := fmt.Sprintf(`{
  		"start_relative": {
		"value": "5",
		"unit": "minutes"
	},

  		"end_absolute": %d,
  		"time_zone": "Asia/Kabul",
  		"metrics": [
	`,
		interval.EndUnixMillis())
	i := 0
	for i = 0; i < len(metrics); i++ {
		sql += fmt.Sprintf(`

		{
      		"name": "%s",
      		"aggregators": [
			{
          		"name": "max",
				"tags": {
            		"hostname": [
              			"*"
            		],
            		"type": [
              			"cpu"
            		]
          		},
          		"sampling": {
            		"value": 1,
            		"unit": "minutes"
          		}
        	}]
    	}
	`,
			metrics[i])
		if i != len(metrics)-1 {
			sql += ","
		}
	}
	sql += "]}"

	humanLabel := "Iginx max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// LastPointPerHost finds the last row for every host in the dataset
//
// Queries:
// lastpoint
func (d *Devops) LastPointPerHost(qi query.Query) {
	metrics := devops.GetAllCPUMetrics()

	sql := fmt.Sprintf(`{
  		"start_absolute": 0,
  		"end_absolute": 2000000000000,
  		"time_zone": "Asia/Kabul",
  		"metrics": [
	`)
	i := 0
	for i = 0; i < len(metrics); i++ {
		sql += fmt.Sprintf(`

		{
      		"name": "%s",
      		"aggregators": [
			{
          		"name": "last",
				"tags": {
            		"hostname": [
              			"*"
            		],
            		"type": [
              			"cpu"
            		]
          		},
          		"sampling": {
            		"value": 1,
            		"unit": "years"
          		}
        	}]
    	}
	`,
			metrics[i])
		if i != len(metrics)-1 {
			sql += ","
		}
	}
	sql += "]}"

	humanLabel := "Iginx last row per host"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has
// high usage between a time period for a number of hosts (if 0, it will
// search all hosts)
//
// Queries:
// high-cpu-1
// high-cpu-all
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	sql := ""
	metrics := devops.GetAllCPUMetrics()

	if nHosts > 0 {
		hosts, err := d.GetRandomHosts(nHosts)
		panicIfErr(err)

		sql = fmt.Sprintf(`
		      SELECT *
		      FROM cpu
		      WHERE usage_user > 90.0
		       AND hostname IN ('%s')
		       AND timestamp >= '%s'
		       AND timestamp < '%s'`,
			strings.Join(hosts, "', '"),
			interval.StartString(),
			interval.EndString())

		sql = fmt.Sprintf(`{
  		"start_absolute": %d,
  		"end_absolute": %d,
  		"time_zone": "Asia/Kabul",
  		"metrics": [
	`,
			interval.StartUnixMillis(),
			interval.EndUnixMillis())
		i := 0
		for i = 0; i < len(metrics); i++ {
			sql += fmt.Sprintf(`

		{
      		"name": "%s",
      		"aggregators": [
			{
          		"name": "avg",
				"tags": {
            		"hostname": [
              			"*"
            		],
            		"type": [
              			"cpu"
            		]
          		},
          		"sampling": {
            		"value": 1,
            		"unit": "hours"
          		}
        	}]
    	}
	`,
				metrics[i])
			if i != len(metrics)-1 {
				sql += ","
			}
		}
		sql += "]}"

	} else {
		sql = fmt.Sprintf(`
		      SELECT *
		      FROM cpu
		      WHERE usage_user > 90.0
		       AND timestamp >= '%s'
		       AND timestamp < '%s'`,
			interval.StartString(),
			interval.EndString())

		sql = fmt.Sprintf(`{
  		"start_absolute": %d,
  		"end_absolute": %d,
  		"time_zone": "Asia/Kabul",
  		"metrics": [
	`,
			interval.StartUnixMillis(),
			interval.EndUnixMillis())
		i := 0
		for i = 0; i < len(metrics); i++ {
			sql += fmt.Sprintf(`

		{
      		"name": "%s",
      		"aggregators": [
			{
          		"name": "avg",
				"tags": {
            		"hostname": [
              			"*"
            		],
            		"type": [
              			"cpu"
            		]
          		},
          		"sampling": {
            		"value": 1,
            		"unit": "hours"
          		}
        	}]
    	}
	`,
				metrics[i])
			if i != len(metrics)-1 {
				sql += ","
			}
		}
		sql += "]}"

	}

	humanLabel, err := devops.GetHighCPULabel("Iginx", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// GroupByTime selects the MAX for metrics under 'cpu', per minute for N random
// hosts
//
// Resultsets:
// single-groupby-1-1-12
// single-groupby-1-1-1
// single-groupby-1-8-1
// single-groupby-5-1-12
// single-groupby-5-1-1
// single-groupby-5-8-1
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)

	sql := fmt.Sprintf(`{
  		"start_absolute": %d,
  		"end_absolute": %d,
  		"time_zone": "Asia/Kabul",
  		"metrics": [
	`,
		interval.StartUnixMillis(),
		interval.EndUnixMillis())
	i := 0
	for i = 0; i < len(metrics); i++ {
		sql += fmt.Sprintf(`

		{
      		"name": "%s",
      		"aggregators": [
			{
          		"name": "max",
				"tags": {
            		"hostname": [
              			"%s"
            		],
            		"type": [
              			"cpu"
            		]
          		},
          		"sampling": {
            		"value": 1,
            		"unit": "minutes"
          		}
        	}]
    	}
	`,
			metrics[i],
			strings.Join(hosts, "\", \""))
		if i != len(metrics)-1 {
			sql += ","
		}
	}
	sql += "]}"

	humanLabel := fmt.Sprintf(
		"Iginx %d cpu metric(s), random %4d hosts, random %s by 1m",
		numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}
