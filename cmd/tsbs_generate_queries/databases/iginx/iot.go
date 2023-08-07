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
		nameClauses = append(nameClauses, fmt.Sprintf("\"name\" = '%s'", s))
	}

	combinedHostnameClause := strings.Join(nameClauses, " or ")
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
	iginxql := fmt.Sprintf(`SELECT last(longitude), last(latitude)
		FROM readings.%s.*.*.*.*`,
		i.getTruckWhereString(nTrucks))

	humanLabel := "Iginx last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)
	fmt.Printf("query: %s\n", iginxql)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	iginxql := `SELECT last(longitude), last(latitude)
		FROM readings.*.*.*.*.*`
	humanLabel := "Iginx last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	iginxql := fmt.Sprintf(`SELECT truck, last_value(value) AS fuel 
		FROM (
			SELECT transposition(fuel_state)
			FROM diagnostics.*.%s.*.*.*
		)
		GROUP BY truck 
		HAVING last_value(value) < 0.1;`,
		i.GetRandomFleet())

	humanLabel := "Iginx trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	fleet := i.GetRandomFleet()
	iginxql := fmt.Sprintf(`SELECT truck
		FROM (
			SELECT a.truck AS truck, a.curr_load / b.capacity AS rate
			FROM (
				SELECT truck, last_value(value) AS curr_load
				FROM (
					SELECT transposition(current_load)
					FROM diagnostics.*.%s.*.*.*
				)
				GROUP BY truck
			) AS a
			INNER JOIN (
				SELECT truck, last_value(value) AS capacity
				FROM (
					SELECT transposition(load_capacity)
					FROM diagnostics.*.%s.*.*.*
				) 
				GROUP BY truck
			) AS b
			ON a.truck = b.truck
		)
		WHERE rate > 0.9`,
		fleet, fleet)

	humanLabel := "Iginx trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
func (i *IoT) StationaryTrucks(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	start := interval.Start().Unix() * 1000 * 1000 * 1000
	end := interval.End().Unix() * 1000 * 1000 * 1000

	iginxql := fmt.Sprintf(`SELECT truck
		FROM (
			SELECT transposition(*)
			FROM (
				SELECT avg(velocity)
				FROM readings.*.%s.*.*.*
				WHERE key >= %d AND key <= %d
				OVER (RANGE 10m IN [%d,%d])
			)
		)
		GROUP BY truck
		HAVING avg(value) < 1`,
		i.GetRandomFleet(),
		start,
		end,
		start,
		end)

	humanLabel := "Iginx stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.LongDrivingSessionDuration)
	start := interval.Start().Unix() * 1000 * 1000 * 1000
	end := interval.End().Unix() * 1000 * 1000 * 1000

	iginxql := fmt.Sprintf(`SELECT truck
		FROM (
			SELECT transposition(*)
			FROM (
				SELECT avg(velocity)
				FROM readings.*.%s.*.*.*
				OVER (RANGE 10m IN [%d,%d])
			)
		)
		WHERE value > 1
		GROUP BY truck
		HAVING count(value) > %d`,
		i.GetRandomFleet(),
		start,
		end,
		tenMinutePeriods(5, iot.LongDrivingSessionDuration))

	humanLabel := "Iginx trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.DailyDrivingDuration)
	start := interval.Start().Unix() * 1000 * 1000 * 1000
	end := interval.End().Unix() * 1000 * 1000 * 1000

	iginxql := fmt.Sprintf(`SELECT truck
		FROM (
			SELECT transposition(*)
			FROM (
				SELECT avg(velocity)
				FROM readings.*.%s.*.*.*
				OVER (RANGE 10m IN [%d,%d])
			)
		)
		WHERE value > 1
		GROUP BY truck
		HAVING count(value) > %d`,
		i.GetRandomFleet(),
		start,
		end,
		tenMinutePeriods(35, iot.DailyDrivingDuration))

	humanLabel := "Iginx trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: drove more than 10 hours in the last 24 hours", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	iginxql := `SELECT sum(fuel_consumption)
		FROM readings.*.*.*.*.* agg level = 2`

	humanLabel := "Iginx average vs projected fuel consumption per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	start := i.Interval.Start().Unix() * 1000 * 1000 * 1000
	end := i.Interval.End().Unix() * 1000 * 1000 * 1000
	fleet := i.GetRandomFleet()

	iginxql := fmt.Sprintf(`SELECT a.truck as truck, 24 * a.driving_ten_mins / b.ten_mins as hours_daily
		FROM (
			SELECT truck, count(value) AS driving_ten_mins
			FROM(
				SELECT transposition(*)
				FROM (
					SELECT avg(velocity)
					FROM readings.*.%s.*.*.*
					OVER (RANGE 10m IN [%d,%d])
				)
			)
			WHERE value > 1
			GROUP BY truck
		) AS a JOIN (
			SELECT truck, last_value(value) AS ten_mins
			FROM(
				SELECT transposition(*)
				FROM (
					SELECT count(*)
					FROM (
						SELECT avg(velocity)
						FROM readings.*.%s.*.*.*
						OVER (RANGE 10m IN [%d,%d])
					)
				)
			)
			GROUP BY truck
		) AS b
		ON a.truck = b.truck`,
		fleet,
		start,
		end,
		fleet,
		start,
		end)

	humanLabel := "Iginx average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {
	// TODO 需要udsf传入key
	start := i.Interval.Start().Unix() * 1000 * 1000 * 1000
	end := i.Interval.End().Unix() * 1000 * 1000 * 1000
	fleet := i.GetRandomFleet()
	iginxql := fmt.Sprintf(`SELECT *
		FROM(
			SELECT avg_driving_session_div_6(*)
			FROM (
				SELECT AVG(velocity)
				FROM readings.*.%s.*.*.*
				OVER(RANGE 10m IN [%d, %d])
			)
			OVER(RANGE 1d IN [%d, %d])
		)`,
		fleet,
		start,
		end,
		start,
		end)

	humanLabel := "Iginx average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	iginxql := `SELECT avg(*)
		FROM(
			SELECT div_load_cap(*)
			FROM (
				SELECT avg(current_load)
				FROM diagnostics.*.*.*.*.*
			), (
				SELECT load_capacity
				FROM diagnostics.*.*.*.*.*
				LIMIT 1
			)
		)
		AGG level=1,2`

	humanLabel := "Iginx average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// DailyTruckActivity returns the number of hours trucks hAS been active (not out-of-commission) per day per fleet per model.
func (i *IoT) DailyTruckActivity(qi query.Query) {
	start := i.Interval.Start().Unix() * 1000 * 1000 * 1000
	end := i.Interval.End().Unix() * 1000 * 1000 * 1000
	iginxql := fmt.Sprintf(`SELECT div_144(*)
		FROM(
			SELECT sum(*)
			FROM (
				SELECT l_one(*)
				FROM(
					SELECT avg(status)
					FROM diagnostics.*.*.*.*.*
					OVER (RANGE 10m IN [%d, %d])
				)
			)
			OVER (RANGE 1d IN [%d, %d])
			AGG level=2,4
		)`,
		start,
		end,
		start,
		end)

	humanLabel := "Iginx daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iginxql)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	start := i.Interval.Start().Unix() * 1000 * 1000 * 1000
	end := i.Interval.End().Unix() * 1000 * 1000 * 1000

	iginxql := fmt.Sprintf(`SELECT count_up(*)
		FROM(
			SELECT ge_half(*)
			FROM (
				SELECT avg(*)
				FROM(
					SELECT nzero(status)
					FROM diagnostics.*.*.*.*.*
					WHERE key >= %d AND key <= %d
				)
				OVER(RANGE 10m IN [%d, %d])
				AGG level=4
			)
		)`,
		start,
		end,
		start,
		end)

	humanLabel := "Iginx stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

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
