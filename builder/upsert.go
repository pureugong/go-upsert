package builder

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
)

const tagName = "db"
const primaryTagKey = "primary"

type QueryBuilder struct {
	tableName            string
	primaryKeyColumns    []string
	primaryKeyIndex      []int
	nonPrimaryKeyColumns []string
	columns              []string

	// options
	onDuplicateSkip bool
}

func NewQueryBuilder(model interface{}, opts ...func(q QueryBuilder) QueryBuilder) QueryBuilder {
	pkColumns := make([]string, 0)
	primaryKeyIndex := make([]int, 0)
	nonePkColumns := make([]string, 0)
	colums := make([]string, 0)

	t := reflect.TypeOf(model)

	tableName := strings.ToLower(t.Name())

	for i := 0; i < t.NumField(); i++ {
		typeField := t.Field(i)
		tag := typeField.Tag.Get(tagName)
		tagValue := ""
		if strings.Contains(tag, primaryTagKey) {
			tagValue = strings.Split(tag, ",")[0]
			pkColumns = append(pkColumns, tagValue)
			primaryKeyIndex = append(primaryKeyIndex, i)
		} else {
			tagValue = tag
			nonePkColumns = append(nonePkColumns, tagValue)
		}
		colums = append(colums, tagValue)
	}

	qb := QueryBuilder{
		tableName:            tableName,
		primaryKeyColumns:    pkColumns,
		primaryKeyIndex:      primaryKeyIndex,
		nonPrimaryKeyColumns: nonePkColumns,
		columns:              colums,
		onDuplicateSkip:      false,
	}

	for _, opt := range opts {
		qb = opt(qb)
	}
	return qb
}

func WithOnDuplicateSkip() func(q QueryBuilder) QueryBuilder {
	return func(q QueryBuilder) QueryBuilder {
		q.onDuplicateSkip = true
		return q
	}
}

func WithOnDuplicateError() func(q QueryBuilder) QueryBuilder {
	return func(q QueryBuilder) QueryBuilder {
		q.onDuplicateSkip = false
		return q
	}
}

func WithTableName(tableName string) func(q QueryBuilder) QueryBuilder {
	return func(q QueryBuilder) QueryBuilder {
		q.tableName = tableName
		return q
	}
}

func WithPrimaryKeys(primaryKeyColumns []string) func(q QueryBuilder) QueryBuilder {
	return func(q QueryBuilder) QueryBuilder {
		q.primaryKeyColumns = primaryKeyColumns
		return q
	}
}

func WithNonPrimaryKeys(nonPrimaryKeyColumns []string) func(q QueryBuilder) QueryBuilder {
	return func(q QueryBuilder) QueryBuilder {
		q.nonPrimaryKeyColumns = nonPrimaryKeyColumns
		return q
	}
}

func WithColumns(columns []string) func(q QueryBuilder) QueryBuilder {
	return func(q QueryBuilder) QueryBuilder {
		q.columns = columns
		return q
	}
}

func (qb QueryBuilder) UpsertSQL(models interface{}) (sql string, args []interface{}, err error) {

	// type validation
	if models == nil {
		return "", nil, errors.New("nil is not supported")
	}

	rt := reflect.TypeOf(models)

	switch rt.Kind() {
	case reflect.Array:
	case reflect.Slice:
	case reflect.Struct:
		log.Println("ok", rt.Kind())
	default:
		return "", nil, fmt.Errorf("%s is not supported", rt.Kind())
	}

	s := reflect.ValueOf(models)
	if rt.Kind() == reflect.Slice && s.IsNil() {
		return "", nil, errors.New("nil slice is not supported")
	}

	getvaluefunc := qb.getSliceValues
	if reflect.TypeOf(models).Kind() == reflect.Struct {
		getvaluefunc = qb.getStructValues
	}

	values, args, err := getvaluefunc(models)
	if err != nil {
		return "", nil, err
	}

	// insert
	sql = fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s`,
		qb.tableName,
		strings.Join(qb.columns, ", "),
		strings.Join(values, ", "),
	)

	// on conflct
	sql = sql + fmt.Sprintf("\nON CONFLICT (%s) DO UPDATE SET", strings.Join(qb.primaryKeyColumns, ", "))

	// do update
	values = make([]string, 0)
	for _, v := range qb.nonPrimaryKeyColumns {
		values = append(values, fmt.Sprintf(` %s = excluded.%s`, v, v))

	}

	sql = sql + strings.Join(values, `,`)
	return sql, args, nil
}

func (qb QueryBuilder) getStructValues(model interface{}) (values []string, args []interface{}, err error) {
	t := reflect.TypeOf(model)
	o := reflect.ValueOf(model)
	vv := make([]string, 0)
	for i := 0; i < t.NumField(); i++ {
		valueField := o.Field(i)
		value := valueField.Interface()
		if valueField.Kind() == reflect.Ptr && valueField.IsNil() {
			value = nil
		}
		vv = append(vv, "?")
		args = append(args, value)
	}
	values = append(values, fmt.Sprintf("(%s)", strings.Join(vv, ", ")))
	return values, args, nil
}

func (qb QueryBuilder) getSliceValues(models interface{}) (values []string, args []interface{}, err error) {
	s := reflect.ValueOf(models)
	t := s.Index(0).Type()
	// pks
	duplicate := make(map[string]bool)

	for i := 0; i < s.Len(); i++ {
		o := reflect.ValueOf(s.Index(i).Interface())
		vv := make([]string, 0)
		aargs := make([]interface{}, 0)
		pks := make([]string, 0)

		for j := 0; j < t.NumField(); j++ {
			valueField := o.Field(j)
			value := valueField.Interface()
			if valueField.Kind() == reflect.Ptr && valueField.IsNil() {
				value = nil
			}
			vv = append(vv, "?")
			aargs = append(aargs, value)
			if Contains(j, qb.primaryKeyIndex) {
				pks = append(pks, fmt.Sprintf("%s", value))
			}
		}
		pk := strings.Join(pks, "-")
		if found := duplicate[pk]; found {
			if !qb.onDuplicateSkip {
				return nil, nil, errors.New("duplicate record found")
			}
			log.Println("duplicate record found: skipping", pk, aargs)
			continue
		}

		duplicate[pk] = true
		values = append(values, fmt.Sprintf("(%s)", strings.Join(vv, ", ")))
		args = append(args, aargs...)
	}
	return values, args, nil
}

func Contains(v int, list []int) bool {
	for _, l := range list {
		if v == l {
			return true
		}
	}
	return false
}
