package main

import (
	"log"

	"github.com/pureugong/go-upsert/builder"
)

type Person struct {
	ID   string `db:"id,primary"`
	Name string `db:"name"`
	Age  *int   `db:"age"`
}

func main() {
	builder := builder.NewQueryBuilder(Person{})
	sql, args, err := builder.UpsertSQL(Person{ID: "1001", Name: "pureugong"})
	if err != nil {
		panic(err)
	}
	log.Println(sql)
	log.Println(args)
}
