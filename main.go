package main

import (
	"fmt"
	"os"
	"encoding/json"
	"sync"
	"github.com/jcelliott/lumber" //? for logging
	"path/filepath"
	"io/ioutil"
)

const Version = "1.0.0"

type (
	Logger interface{
		Fatal(string, ...interface{})
		Error(string, ...interface{}) 
		Warn(string, ...interface{}) 
		Info(string, ...interface{}) 
		Debug(string, ...interface{}) 
		Trace(string, ...interface{}) 
	}

	dbDriver struct {
		mutex sync.Mutex,
		mutexes map[string]*sync.Mutex,
		dir string,
		log Logger
	}
)

type Options struct{
	Logger
}

func new(dir string, options *Options)(*dbDriver, error) { // accept multiple arguments, return multiple values
dir = filepath.Clean(dir)

opts := Options{}
if options != nil{
	opts := *options
}

if opts.Logger == nil{
	opts.Logger = lumber.NewConsoleLogger((lumber.Info))
}

driver := dbDriver{
	log: opts.Logger,
	mutexes: make(map[string]*sync.Mutex),
	dir: dir
}

//! checking for existing databases (before creating new)
if _, err := os.Stat(dir); err == nil{
	opts.Logger.Debug("Using '%s' (database already exists)\n", dir)
	return &driver, nil
}

opts.Logger.Debug("Creating the database at '%s'...\n", dir)
return &driver, os.MkdirAll(dir, 0755) // chmod +755
}

//! struct method
func (d *dbDriver) Write(collection, resource string, v interface{}) error { // write to db, else return error
	if collection == ""{
		fmt.Errorf("Missing Collection - no place to save record...")
	}
	if resource == ""{
		fmt.Errorf("Missing resource - unable to save record(no name/resource found)..")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()

	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	finalpath := filepath.Join(dir, resource+".json")
	tempPath = filepath + ".tmp"

	b, err := json.MarhsalIndent(v, "", "\t")
	if err != nil{
		return err
	}

	b = append(b, byte('\n'))

	if err := ioutil.WriteFile(tempPath, b, 0644); err != nil{
		return err
	}

	return os.Rename(tempPath, finalpath)
}

//! struct method
func (d *dbDriver) Read(collection, resource string, v interface{}) error{ // read from db, or return error
	
	if collection == ""{
		fmt.Errorf("Missing Collection - no place to extract record...")
	}
	if resource == ""{
		fmt.Errorf("Missing resource - unable to read record(no name/resource found)..")
	}

	record := filepath.Join(d.dir, collection, resource)

	if _, err := stat(record); err != nil{
		return err
	}

	b, err := ioutil.Read(record + ".json")
	if err != nil{
		return err
	}

	return json.Unmarshal(b, &v)
}

//! struct method
func ReadAll()(){ // read from db, and return multiple values

}

//! struct method
func Delete() error{ // if cannot delete, return error

}

//! struct method
func getOrCreateMutex(collection string) *sync.Mutex { // take things and return mutex
	d.mutex.Lock()
	defer d.mutex.Unlock()

	m, ok := mutexes[collection]
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}
	return m
}

func stat(path string)(fi os.FileInfo, err error){
	if fi, err = os.Stat(path); os.IsNotExist(err){
		fi, err = os.Stat(path + ".json")
	}
	return
}

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
	*	unmarshalling will convert the json data into a go struct
	*/

	allusers := []User{}
	for _, f := range records{
		employeeFound := User{}
		if err := json.Unmarshal([]byte(f), &employeeFound); err != nil fmt.Println("Error:", err)

		allusers = append(allusers, employeeFound)
	}
	fmt.Println((allusers))

	// if err:= db.Delete("user", "john"); 
	// err != nil{
	// 	fmt.Println("Error:", err)
	// }

	// if err:= db.DeleteAll("user", "");
	// err != nil{
	// 	fmt.Println("Erro:", err)
	// }

}