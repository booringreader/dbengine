package main

import (
	"fmt"
	"os"
	"encoding/json"
)

type Address struct {
	Street string
	City string
	State string
	Pincode json.Number
}

type User struct {
	Name string
	Age json.Number
	Contact string
	Company string
	Address Address
}

func main(){
	dir := "./"

	db, err := New(dir, nil)
	if err != nil fmt.Println("Error:", err)

	// slice(array/collection of structs) of type users
	employees := []User{
		{"John Doe", "25", "1234567890", "OpenAI", Address{"123 Main St", "Anytown", "CA", "12345"}},
		{"Jane Smith", "30", "0987654321", "Facebook", Address{"456 Elm St", "Othertown", "NY", "67890"}},
		{"Jim Beam", "35", "1112223333", "Amazon", Address{"789 Oak St", "Smalltown", "TX", "54321"}},
		{"Jill Johnson", "40", "2223334444", "Microsoft", Address{"321 Pine St", "Bigtown", "WA", "98765"}},
		{"Jack White", "45", "3334445555", "Apple", Address{"654 Maple St", "Largetown", "IL", "43210"}},
		{"Jill Black", "50", "4445556666", "Google", Address{"987 Birch St", "Towntown", "OH", "87654"}},
		{"Jill Green", "55", "5556667777", "TSMC", Address{"1357 Walnut St", "Villagetown", "MI", "76543"}},
		{"Jill Blue", "60", "6667778888", "Amazon", Address{"2468 Oak St", "Hamlet", "IN", "65432"}},
		{"Jill Red", "65", "7778889999", "Microsoft", Address{"3579 Pine St", "Suburb", "PA", "54321"}},
		{"Jill Yellow", "70", "8889990000", "Apple", Address{"4680 Birch St", "Township", "NJ", "43210"}},
		{"Jill Purple", "75", "9990001111", "X.com", Address{"5701 Maple St", "Village", "MO", "32109"}},
		{"Jill Orange", "80", "0001112222", "Nvidia", Address{"6823 Walnut St", "Suburb", "NC", "21098"}},
	}

	for _, value := range employees {
		db.Write("users", value.Name, User{
			Name: value.Name,
			Age: value.Age,
			Contact: value.Contact,
			Company: value.Company,
			Address: value.Address,
		})
	}

	records, err := db.ReadAll("users")
	if err != nil fmt.Println("Error:", err)
	fmt.Println(records)


	/*
	*	`records` is a slice of type `User`
	*	`records` is of type json, so to be able to use it in go,
	*	we need to unmarshal it into a slice of type `User`
	*	unmarshalling will conver the json data into a go struct
	*/

	allusers := []User{}
	for _, f := range records{
		employeeFound := User{}
		if err := json.Unmarshal([]byte(f), &employeeFound); err != nil fmt.Println("Error:", err)

		allusers = append(allusers, employeeFound)
	}
	fmt.Println(allusers)


}