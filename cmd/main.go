package main

import (
	"fmt"
	"os"
)

func main() {
	key := os.Getenv("KEY")
	shiesh := os.Getenv("SHIESH")

	fmt.Println("THESE ARE THEY::", key, shiesh)
}
