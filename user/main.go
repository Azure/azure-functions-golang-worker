package main

import (
	"log"
	"time"
)

func main() {
	log.Print("Azure Functions Go Worker is not intended to be run directly. Please use the Azure Functions Core Tools to run your function app.")

	for {
		time.Sleep((time.Second * 5))
	}
}
