package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func main() {
	fmt.Println("hello world")
}

type OplogEntry struct {
	Op string                 `json:"op"`
	NS string                 `json:"ns"`
	O  map[string]interface{} `json:"o"`
}

func GenerateInsertSQL(oplog string) (string, error) {
	var oplogObj OplogEntry
	if err := json.Unmarshal([]byte(oplog), &oplogObj); err != nil {
		return "", err
	}

	switch oplogObj.Op {
	case "i":
		sql := fmt.Sprintf("INSERT INTO %s", oplogObj.NS)

		columnNames := make([]string, 0, len(oplogObj.O))
		for columnName := range oplogObj.O {
			columnNames = append(columnNames, columnName)
		}

		sort.Strings(columnNames)

		columnValues := make([]string, 0, len(oplogObj.O))
		for _, columnName := range columnNames {
			columnValues = append(columnValues, getColumnValue(oplogObj.O[columnName]))
		}

		sql = fmt.Sprintf("%s (%s) VALUES (%s);", sql, strings.Join(columnNames, ", "), strings.Join(columnValues, ", "))

		return sql, nil
	}

	return "", nil
}

func getColumnValue(value interface{}) string {
	switch value.(type) {
	case int, int8, int16, int32, int64, float32, float64:
		return fmt.Sprintf("%v", value)
	case bool:
		return fmt.Sprintf("%t", value)
	default:
		return fmt.Sprintf("'%v'", value)
	}
}

// // happy path:
// 	oplog -> sql
// // unhappy path:
// 	oplog -> error
