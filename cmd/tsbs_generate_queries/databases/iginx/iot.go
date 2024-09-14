package iginx

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/pkg/query"
)

// IoT produces TimescaleDB-specific queries for all the iot query types.
type IoT struct {
	*iot.Core
	*BaseGenerator
}

// NewIoT makes an IoT object ready to generate Queries.
func NewIoT(start, end time.Time, scale int, g *BaseGenerator) *IoT {
	c, err := iot.NewCore(start, end, scale)
	panicIfErr(err)
	return &IoT{
		Core:          c,
		BaseGenerator: g,
	}
}

func (i *IoT) getTrucksWhereWithNames(names []string) string {
	nameClauses := []string{}
	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("tags.name = '%s'", s))
	}

	combinedHostnameClause := strings.Join(nameClauses, " OR ")
	return "(" + combinedHostnameClause + ")"
}

func (i *IoT) getTruckWhereString(nTrucks int) string {
	names, err := i.GetRandomTrucks(nTrucks)
	if err != nil {
		panic(err.Error())
	}
	return i.getTrucksWhereWithNames(names)
}

// LastLocByTruck finds the truck location for nTrucks.
func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	iginxql := fmt.Sprintf(`SELECT tags.name, tags.driver, readings.longitude, readings.latitude 
		FROM tags INNER JOIN readings 
		ON tags.tagid=readings.tagid 
		WHERE %s 
		AND readings.timestamp 
		IN (
			SELECT max(timestamp) 
			FROM readings
		);`,
		i.getTruckWhereString(nTrucks))

	humanLabel := "Iginx last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	iginxql := `SELECT tags.name, tags.driver, readings.longitude, readings.latitude 
		FROM tags INNER JOIN readings 
		ON tags.tagid=readings.tagid 
		WHERE readings.timestamp 
		IN (
			SELECT max(timestamp) 
			FROM readings
		);`
	humanLabel := "Iginx last location per truck"
	humanDesc := humanLabel
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	iginxql := fmt.Sprintf(`SELECT t.name, d.fuel_state 
		FROM tags t 
		INNER JOIN diagnostics d 
		ON t.tagid=d.tagid 
		WHERE d.timestamp 
		IN (
			SELECT max(timestamp) 
			FROM diagnostics
		) 
		AND t.fleet='%s' 
		AND d.fuel_state <0.1;`,
		i.GetRandomFleet())

	humanLabel := "Iginx trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	fleet := i.GetRandomFleet()
	iginxql := fmt.Sprintf(`SELECT tags.name, tags.driver, diagnostics.* 
		FROM tags INNER JOIN diagnostics 
		ON tags.tagid=diagnostics.tagid 
		WHERE diagnostics.timestamp 
		IN (
			SELECT max(timestamp) 
			FROM diagnostics
		) 
		AND tags.fleet='%s'
		AND diagnostics.current_load/tags.load_capacity > 0.9;`,
		fleet)

	humanLabel := "Iginx trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
func (i *IoT) StationaryTrucks(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	start := interval.Start().Unix() * 1000 * 1000 * 1000
	end := interval.End().Unix() * 1000 * 1000 * 1000

	iginxql := fmt.Sprintf(`SELECT t.name, avg(r.velocity) 
		FROM tags t 
		INNER JOIN readings r 
		ON t.tagid=r.tagid 
		WHERE t.fleet='%s' 
		AND r.timestamp>= %d 
		AND r.timestamp<%d 
		GROUP BY t.name 
		HAVING avg(r.velocity)<1;`,
		i.GetRandomFleet(),
		start,
		end)

	humanLabel := "Iginx stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.LongDrivingSessionDuration)
	start := interval.Start().Unix() * 1000 * 1000 * 1000
	end := interval.End().Unix() * 1000 * 1000 * 1000

	iginxql := fmt.Sprintf(`SELECT tags.name, tags.driver, count(tags.tagid) 
		FROM tags INNER JOIN (
			SELECT ten, tagid, avg(velocity) 
			FROM (
				SELECT `+"`"+`timebucket10m(readings.timestamp)`+"`"+` AS ten,`+"`"+`timebucket10m(readings.tagid)`+"`"+` AS tagid,`+"`"+`timebucket10m(readings.velocity)`+"`"+` as velocity 
				FROM (
					SELECT timebucket10m(*) 
					FROM (
						SELECT timestamp, tagid, velocity 
						FROM readings 
						WHERE readings.timestamp >= %d AND readings.timestamp < %d
					)
				)
			) 
			GROUP BY ten, tagid 
			HAVING avg(velocity) > 1 
			ORDER BY ten, tagid
		) 
		ON tags.tagid=tagid 
		WHERE tags.fleet='%s'
		GROUP BY tags.name, tags.driver 
		HAVING count(tags.tagid) > %d;`,
		start,
		end,
		i.GetRandomFleet(),
		tenMinutePeriods(5, iot.LongDrivingSessionDuration))

	humanLabel := "Iginx trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.DailyDrivingDuration)
	start := interval.Start().Unix() * 1000 * 1000 * 1000
	end := interval.End().Unix() * 1000 * 1000 * 1000

	iginxql := fmt.Sprintf(`SELECT tags.name, tags.driver, count(tags.tagid) 
		FROM tags INNER JOIN (
			SELECT ten, tagid, avg(velocity) 
			FROM (
				SELECT `+"`"+`timebucket10m(readings.timestamp)`+"`"+` AS ten,`+"`"+`timebucket10m(readings.tagid)`+"`"+` AS tagid,`+"`"+`timebucket10m(readings.velocity)`+"`"+` AS velocity 
				FROM (
					SELECT timebucket10m(*) 
					FROM (
						SELECT timestamp, tagid, velocity 
						FROM readings 
						WHERE readings.timestamp >= %d AND readings.timestamp < %d
					)
				)
			) 
			GROUP BY ten, tagid 
			HAVING avg(velocity) > 1 
			ORDER BY ten, tagid
		) 
		ON tags.tagid=tagid 
		WHERE tags.fleet='%s'
		GROUP BY tags.name, tags.driver 
		HAVING count(tags.tagid) > %d;`,
		start,
		end,
		i.GetRandomFleet(),
		tenMinutePeriods(35, iot.LongDrivingSessionDuration))

	humanLabel := "Iginx trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: drove more than 10 hours in the last 24 hours", humanLabel)
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	iginxql := `SELECT tags.fleet, avg(readings.fuel_consumption) AS avg_fuel_consumption, avg(tags.nominal_fuel_consumption) AS projected_fuel_consumption 
		FROM (
			SELECT * FROM tags INNER JOIN readings 
			WHERE tags.tagid=readings.tagid 
			AND readings.velocity>1
		) 
		GROUP BY tags.fleet;`

	humanLabel := "Iginx average vs projected fuel consumption per fleet"
	humanDesc := humanLabel
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	iginxql := fmt.Sprintf(`SELECT tags.fleet, tags.name, tags.driver, avg(hours) 
		FROM tags INNER JOIN (
			SELECT day, tagid, div6(hours) AS hours 
			FROM (
				SELECT day, tagid, count(day) AS hours 
				FROM (
					SELECT ` + "`" + `timebucketday(ten)` + "`" + ` AS day,` + "`" + `timebucketday(tagid)` + "`" + ` AS tagid 
					FROM (
						SELECT timebucketday(*) 
						FROM (
							SELECT ten, tagid, avg(velocity) 
							FROM (
								SELECT ` + "`" + `timebucket10m(readings.timestamp)` + "`" + ` AS ten,` + "`" + `timebucket10m(readings.tagid)` + "`" + ` AS tagid,` + "`" + `timebucket10m(readings.velocity)` + "`" + ` AS velocity 
								FROM (
									SELECT timebucket10m(*) 
									FROM (
										SELECT timestamp, tagid, velocity 
										FROM readings
									)
								)
							) 
							GROUP BY ten, tagid 
							HAVING avg(velocity) > 1
						)
					)
				) 
			GROUP BY day, tagid
			)
		) 
		ON tags.tagid=tagid 
		GROUP BY tags.fleet, tags.name, tags.driver;`)

	humanLabel := "Iginx average driver driving duration per day"
	humanDesc := humanLabel
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {

	iginxql := fmt.Sprintf(`SELECT name, day, avg(d) 
		FROM (
			SELECT ` + "`" + `timebucketday(start)` + "`" + ` AS day,` + "`" + `timebucketday(tags.name)` + "`" + ` AS name, ` + "`" + `timebucketday(d)` + "`" + ` AS d 
			FROM (
				SELECT timebucketday(*) 
				FROM (
					SELECT start, tags.name, stop - start AS d 
					FROM tags INNER JOIN (
						SELECT ` + "`" + `startstop(tagid)` + "`" + ` AS tagid, ` + "`" + `startstop(ten)` + "`" + ` AS start,` + "`" + `startstop(stop)` + "`" + ` AS stop,` + "`" + `startstop(driving)` + "`" + ` AS driving 
						FROM (
							SELECT startstop(*) 
							FROM (
								SELECT tagid, ten, avg(velocity) AS driving 
								FROM (
									SELECT ` + "`" + `timebucket10m(readings.timestamp)` + "`" + ` AS ten,` + "`" + `timebucket10m(readings.tagid)` + "`" + ` AS tagid,` + "`" + `timebucket10m(readings.velocity)` + "`" + ` AS velocity 
									FROM (
										SELECT timebucket10m(*) 
										FROM (
											SELECT timestamp, tagid, velocity 
											FROM readings
										)
									)
								) 
								GROUP BY tagid, ten 
								ORDER BY tagid, ten
							)
						)
					WHERE ` + "`" + `startstop(driving)` + "`" + ` > 5
					)
					ON tags.tagid=tagid
				)
			)
		) 
		GROUP BY name, day 
		ORDER BY name, day;`)

	humanLabel := "Iginx average driver driving session without stopping per day"
	humanDesc := humanLabel
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	iginxql := `SELECT fleet, model, load_capacity, avg(load_percentage) 
		FROM (
			SELECT t.fleet AS fleet, t.model AS model, t.load_capacity AS load_capacity, div(avg_load,t.load_capacity) AS load_percentage 
			FROM tags t INNER JOIN (
				SELECT * 
				FROM (
					SELECT tagid AS id, avg(current_load) AS avg_load FROM diagnostics 
					GROUP BY tagid
				)
			) 
			ON t.tagid=id
		) 
		GROUP BY fleet, model, load_capacity;`

	humanLabel := "Iginx average load per truck model per fleet"
	humanDesc := humanLabel
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
func (i *IoT) DailyTruckActivity(qi query.Query) {
	iginxql := fmt.Sprintf(`SELECT tags.fleet, tags.model, day, div144(daily_activity) AS daily_activity 
		FROM (
			SELECT tags.fleet, tags.model, day, sum(ten_mins_per_day) AS daily_activity 
			FROM tags INNER JOIN (
				SELECT day, ten, tagid, avg(sta), count(day) AS ten_mins_per_day 
				FROM (
					SELECT ` + "`" + `timebucketdayten(a)` + "`" + ` AS day,` + "`" + `timebucketdayten(b)` + "`" + ` AS ten,` + "`" + `timebucketdayten(diagnostics.tagid)` + "`" + ` AS tagid,` + "`" + `timebucketdayten(diagnostics.status)` + "`" + ` AS sta 
					FROM (
						SELECT timebucketdayten(*) 
						FROM (
							SELECT timestamp AS a, timestamp AS b, tagid, status 
							FROM diagnostics
						)
					)
				) 
				GROUP BY day, ten, tagid 
				HAVING avg(sta) < 1
			) 
			ON tags.tagid = tagid 
			GROUP BY tags.fleet, tags.model, day 
			ORDER BY day
		);`)

	humanLabel := "Iginx daily truck activity per fleet per model"
	humanDesc := humanLabel
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	iginxql := fmt.Sprintf(`SELECT model, count(*) 
		FROM (
			SELECT tags.model AS model 
			FROM tags INNER JOIN (
				SELECT tagid, ten, broken_down, next_broken_down 
				FROM (
					SELECT ` + "`" + `lead(tagid)` + "`" + ` AS tagid,` + "`" + `lead(ten)` + "`" + ` AS ten,` + "`" + `lead(broken_down)` + "`" + ` AS broken_down,` + "`" + `lead(next_broken_down)` + "`" + ` AS next_broken_down 
					FROM (
						SELECT lead(*) 
						FROM (
							SELECT tagid, ten, ifbreak(s) AS broken_down 
								FROM (
									SELECT ` + "`" + `timebucket10m(diagnostics.timestamp)` + "`" + ` AS ten,` + "`" + `timebucket10m(diagnostics.tagid)` + "`" + ` AS tagid,` + "`" + `timebucket10m(diagnostics.status)` + "`" + ` AS s 
									FROM (
										SELECT timebucket10m(*) 
										FROM (
											SELECT timestamp, tagid, status 
											FROM diagnostics
										)
									)
								) 
							GROUP BY tagid, ten 
							ORDER BY tagid, ten
						)
					)
				) 
				GROUP BY tagid, ten, broken_down, next_broken_down
			) 
			ON tags.tagid = tagid 
			WHERE broken_down = false 
			AND next_broken_down = true
		) 
		GROUP BY model;`)

	humanLabel := "Iginx stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// tenMinutePeriods calculates the number of 10 minute periods that can fit in
// the time duration if we subtract the minutes specified by minutesPerHour value.
// E.g.: 4 hours - 5 minutes per hour = 3 hours and 40 minutes = 22 ten minute periods
func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}
