package iginx

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// BaseGenerator contains settings specific for Iginx
type BaseGenerator struct {
}

// GenerateEmptyQuery returns an empty query.Iginx.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewIginx()
}

// fillInQuery fills the query struct with data.
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, sql string) {
	q := qi.(*query.Iginx)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.SqlQuery = []byte(sql)
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}

// NewIoT creates a new iot use case query generator.
func (g *BaseGenerator) NewIoT(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := iot.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	iot := &IoT{
		BaseGenerator: g,
		Core:          core,
	}

	return iot, nil
}
