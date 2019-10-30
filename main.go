package main

import (
	"fmt"
	"reflect"
)

type table struct {
	ID uint64
}

type tableATableB struct {
	TableA *table
	TableB *table
}

func main() {
	tableA := initTable(16)
	tableB := initTable(32)

	loopConformity := nestedLoopsJoin(tableA, tableB, "ID")
	hashConformity := hashJoin(tableA, tableB, "ID")
	mergeConformity := mergeJoin(tableA, tableB, "ID")
	fmt.Println(len(loopConformity))
	fmt.Println(len(hashConformity))
	fmt.Println(len(mergeConformity))
}

func nestedLoopsJoin(tableA, tableB []table, fieldName string) []tableATableB {
	tablesConformity := make([]tableATableB, 0, len(tableA) / 2)
	for i := 0; i < len(tableA); i++ {
		tableAValue := reflect.ValueOf(tableA[i])
		for j := 0; j < len(tableB); j++ {
			tableBValue := reflect.ValueOf(tableB[j])
			if reflect.DeepEqual(tableAValue.FieldByName(fieldName).Interface(), tableBValue.FieldByName(fieldName).Interface()) {
				tablesConformity = append(tablesConformity, tableATableB{TableA:&tableA[i], TableB:&tableB[j]})
			}
		}
	}
	return tablesConformity
}

func hashJoin(tableA, tableB []table, fieldName string) []tableATableB {
	tablesConformity := make([]tableATableB, 0, len(tableA) / 2)
	tableIdsSet := make(map[interface{}]*table, len(tableA) / 2)
	for i := 0; i < len(tableA); i++ {
		tableIdsSet[reflect.ValueOf(tableA[i]).FieldByName(fieldName).Interface()] = &tableA[i]
	}

	for i := 0; i < len(tableB); i++ {
		if tableIdsSet[reflect.ValueOf(tableB[i]).FieldByName(fieldName).Interface()] != nil {
			tableAI := tableIdsSet[reflect.ValueOf(tableB[i]).FieldByName(fieldName).Interface()]
			tablesConformity = append(tablesConformity, tableATableB{TableA:tableAI, TableB:&tableB[i]})
		}
	}
	return tablesConformity
}


//only works on sorted data
func mergeJoin(tableA, tableB []table, fieldName string) []tableATableB {
	tablesConformity := make([]tableATableB, 0, len(tableA) / 2)
	tableAIterator := 0
	tableBIterator := 0

	for tableAIterator < len(tableA) && tableBIterator < len(tableB) {
		tableAValue := reflect.ValueOf(tableA[tableAIterator]).FieldByName(fieldName)
		tableBValue := reflect.ValueOf(tableA[tableBIterator]).FieldByName(fieldName)
		if tableAValue.Uint() > tableBValue.Uint() {
			tableBIterator++
		} else if tableAValue.Uint() < tableBValue.Uint() {
			tableAIterator++
		} else {
			tablesConformity = append(tablesConformity, tableATableB{TableA:&tableA[tableAIterator], TableB:&tableB[tableBIterator]})
			tableAIterator++
		}
	}
	return tablesConformity
}

func initTable(size int) []table {
	tables := make([]table, 0, size)
	for i := 0; i < cap(tables); i++ {
		tables = append(tables, table{ID:uint64(i)})
	}
	return tables
}