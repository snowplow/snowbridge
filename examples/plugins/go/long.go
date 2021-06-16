package main

import (
	"bufio"
	b64 "encoding/base64"
	"fmt"
	"os"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		inputB64encoded := scanner.Text()

		inputDecoded, _ := b64.StdEncoding.DecodeString(inputB64encoded)

		outputDecoded := "Go Transformation: " + string(inputDecoded)
		outputB64encoded := b64.StdEncoding.EncodeToString([]byte(outputDecoded))

		fmt.Println(outputB64encoded)

		fmt.Fprintln(os.Stderr, "")
	}
}
