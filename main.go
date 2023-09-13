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

func GenerateSQL(oplog string) ([]string, error) {
	sqls := []string{}

	var oplogEntries []OplogEntry
	if err := json.Unmarshal([]byte(oplog), &oplogEntries); err != nil {

		var oplogObj OplogEntry
		if err := json.Unmarshal([]byte(oplog), &oplogObj); err != nil {
			return sqls, err
		}

		oplogEntries = append(oplogEntries, oplogObj)
	}

	cacheMap := make(map[string]bool)
	for _, oplogEntry := range oplogEntries {
		innerSqls, err := generateSQL(oplogEntry, cacheMap)
		if err != nil {
			return []string{}, err
		}
		sqls = append(sqls, innerSqls...)
	}

	return sqls, nil
}

func generateSQL(oplogObj OplogEntry, cacheMap map[string]bool) ([]string, error) {
	sqls := []string{}

	switch oplogObj.Op {
	case "i":
		// Create schema
		nsParts := strings.Split(oplogObj.NS, ".")
		schemaName := nsParts[0]
		if exits := cacheMap[schemaName]; !exits {
			sqls = append(sqls, generateCreateSchemaSQL(schemaName))
			cacheMap[schemaName] = true
		}

		// Create table
		if exits := cacheMap[oplogObj.NS]; !exits {
			sqls = append(sqls, generateCreateTableSQL(oplogObj))
			cacheMap[oplogObj.NS] = true
		}

		sql, err := generateInsertSQL(oplogObj)
		if err != nil {
			return sqls, err
		}
		sqls = append(sqls, sql)
	case "u":
		sql, err := generateUpdateSQL(oplogObj)
		if err != nil {
			return sqls, err
		}
		sqls = append(sqls, sql)
	case "d":
		sql, err := generateDeleteSQL(oplogObj)
		if err != nil {
			return sqls, err
		}
		sqls = append(sqls, sql)
	}

	return sqls, nil
}

func generateCreateSchemaSQL(schemaName string) string {
	return fmt.Sprintf("CREATE SCHEMA %s;", schemaName)
}

func generateCreateTableSQL(oplogObj OplogEntry) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (", oplogObj.NS))
	columnNames := getColumnNames(oplogObj.O)

	sep := ""
	for _, columnName := range columnNames {
		value := oplogObj.O[columnName]
		colDataType := getColumnSQLDataType(columnName, value)

		sb.WriteString(fmt.Sprintf("%s%s %s", sep, columnName, colDataType))
		sep = ", "
	}

	sb.WriteString(");")
	return sb.String()
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

func getColumnNames(data map[string]interface{}) []string {
	columnNames := make([]string, 0, len(data))
	for columnName := range data {
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)
	return columnNames
}

func getColumnSQLDataType(columnName string, value interface{}) string {
	colDataType := ""
	switch value.(type) {
	case int, int8, int16, int32, int64:
		colDataType = "INTEGER"
	case float32, float64:
		colDataType = "FLOAT"
	case bool:
		colDataType = "BOOLEAN"
	default:
		colDataType = "VARCHAR(255)"
	}

	if columnName == "_id" {
		colDataType += " PRIMARY KEY"
	}
	return colDataType
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
