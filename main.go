package main

import "fmt"

func main() {
	config, err := LoadConfig("example.yml", nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v", config.Input)
}
