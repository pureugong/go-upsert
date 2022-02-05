package builder

import (
	"reflect"
	"testing"
)

func TestNewQueryBuilder(t *testing.T) {
	type Person struct {
		ID   string `db:"id,primary"`
		Name string `db:"name"`
	}

	builder := NewQueryBuilder(Person{})
	if !reflect.DeepEqual(builder.tableName, "person") {
		t.Error("failed")
	}
	if !reflect.DeepEqual(builder.primaryKeyColumns, []string{"id"}) {
		t.Error("failed")
	}
	if !reflect.DeepEqual(builder.primaryKeyIndex, []int{0}) {
		t.Error("failed")
	}
	if !reflect.DeepEqual(builder.nonPrimaryKeyColumns, []string{"name"}) {
		t.Error("failed")
	}
	if !reflect.DeepEqual(builder.columns, []string{"id", "name"}) {
		t.Error("failed")
	}
}

func TestQueryBuilderUpsertSQL(t *testing.T) {
	type Person struct {
		ID   string `db:"id,primary"`
		Name string `db:"name"`
	}
	builder := NewQueryBuilder(Person{})
	tests := []struct {
		models   []Person
		expected string
	}{
		{
			models: []Person{
				{ID: "1001", Name: "Tom"},
				{ID: "1002", Name: "Jerry"},
				{ID: "1003", Name: "Nibbles"},
			},
			expected: `INSERT INTO person (id, name) VALUES (?, ?), (?, ?), (?, ?)
ON CONFLICT (id) DO UPDATE SET name = excluded.name`,
		},
		{
			models: []Person{
				{ID: "1001", Name: "Tom"},
				{ID: "1002", Name: "Jerry"},
				{ID: "1003", Name: "Nibbles"},
				{ID: "1004", Name: "Butch"},
				{ID: "1005", Name: "Quacker"},
			},
			expected: `INSERT INTO person (id, name) VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)
ON CONFLICT (id) DO UPDATE SET name = excluded.name`,
		},
	}

	for _, test := range tests {
		sql, _, err := builder.UpsertSQL(test.models)
		if err != nil {
			t.Error(err)
		}
		if sql != test.expected {
			t.Error(sql)
			t.Error(test.expected)
		}
	}

}

func TestQueryBuilderUpsertSQLDuplicateSkip(t *testing.T) {
	type Person struct {
		ID   string `db:"id,primary"`
		Name string `db:"name"`
	}
	builder := NewQueryBuilder(Person{}, WithOnDuplicateSkip())
	tests := []struct {
		models   []Person
		expected string
	}{
		{
			models: []Person{
				{ID: "1001", Name: "Tom"},
				{ID: "1002", Name: "Jerry"},
				{ID: "1002", Name: "Jerry"},
				{ID: "1002", Name: "Jerry"},
				{ID: "1003", Name: "Nibbles"},
			},
			expected: `INSERT INTO person (id, name) VALUES (?, ?), (?, ?), (?, ?)
ON CONFLICT (id) DO UPDATE SET name = excluded.name`,
		},
		{
			models: []Person{
				{ID: "1001", Name: "Tom"},
				{ID: "1001", Name: "Tom"},
				{ID: "1001", Name: "Tom"},
				{ID: "1001", Name: "Tom"},
				{ID: "1001", Name: "Tom"},
			},
			expected: `INSERT INTO person (id, name) VALUES (?, ?)
ON CONFLICT (id) DO UPDATE SET name = excluded.name`,
		},
	}

	for _, test := range tests {
		sql, _, err := builder.UpsertSQL(test.models)
		if err != nil {
			t.Error(err)
		}
		if sql != test.expected {
			t.Error(sql)
			t.Error(test.expected)
		}
	}
}
