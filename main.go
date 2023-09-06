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
	O2 map[string]interface{} `json:"o2"`
}

func GenerateSQL(oplog string) (string, error) {
	var oplogObj OplogEntry
	if err := json.Unmarshal([]byte(oplog), &oplogObj); err != nil {
		return "", err
	}

	switch oplogObj.Op {
	case "i":
		return generateInsertSQL(oplogObj)
	case "u":
		return generateUpdateSQL(oplogObj)
	case "d":
		return generateDeleteSQL(oplogObj)
	}

	return "", fmt.Errorf("invalid oplog")
}

func generateInsertSQL(oplogObj OplogEntry) (string, error) {
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

func generateUpdateSQL(oplogObj OplogEntry) (string, error) {
	switch oplogObj.Op {
	case "u":
		sql := fmt.Sprintf("UPDATE %s SET", oplogObj.NS)

		diffMap, ok := oplogObj.O["diff"].(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("invalid oplog")
		}

		if setMap, ok := diffMap["u"].(map[string]interface{}); ok {
			columnValues := make([]string, 0, len(setMap))
			for columnName, value := range setMap {
				columnValues = append(columnValues, fmt.Sprintf("%s = %s", columnName, getColumnValue(value)))
			}
			sort.Strings(columnValues)

			sql = fmt.Sprintf("%s %s", sql, strings.Join(columnValues, ", "))
		} else if unsetMap, ok := diffMap["d"].(map[string]interface{}); ok {
			columnValues := make([]string, 0, len(unsetMap))
			for columnName := range unsetMap {
				columnValues = append(columnValues, fmt.Sprintf("%s = NULL", columnName))
			}
			sort.Strings(columnValues)

			sql = fmt.Sprintf("%s %s", sql, strings.Join(columnValues, ", "))
		} else {
			return "", fmt.Errorf("invalid oplog")
		}

		whereColumnValues := make([]string, 0, len(oplogObj.O2))
		for columnName, value := range oplogObj.O2 {
			whereColumnValues = append(whereColumnValues, fmt.Sprintf("%s = %s", columnName, getColumnValue(value)))
		}

		sql = fmt.Sprintf("%s WHERE %s;", sql, strings.Join(whereColumnValues, " AND "))

		return sql, nil
	}

	return "", fmt.Errorf("invalid oplog")
}

func generateDeleteSQL(oplogObj OplogEntry) (string, error) {
	switch oplogObj.Op {
	case "d":
		// DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';
		sql := fmt.Sprintf("DELETE FROM %s WHERE", oplogObj.NS)

		whereColumnValues := make([]string, 0, len(oplogObj.O))
		for columnName, value := range oplogObj.O {
			whereColumnValues = append(whereColumnValues, fmt.Sprintf("%s = %s", columnName, getColumnValue(value)))
		}

		sql = fmt.Sprintf("%s %s;", sql, strings.Join(whereColumnValues, " AND "))

		return sql, nil
	}
	return "", fmt.Errorf("invalid oplog")
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
