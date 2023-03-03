package main

import (
	"time"
)

type dbCreator struct {
}

func (d *dbCreator) Init() {
}

func (d *dbCreator) DBExists(dbName string) bool {
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	time.Sleep(time.Second)
	return nil
}

type QueryResponseColumns struct {
	Name string
	Type string
}

type QueryResponse struct {
	Query   string
	Columns []QueryResponseColumns
	Dataset [][]interface{}
	Count   int
	Error   string
}
