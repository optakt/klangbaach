package main

import (
	"log"
	"os"

	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {

	client, err := ethclient.Dial("https://eth-mainnet.g.alchemy.com/v2/UxuHkw-MO02DZ9qYM3usei-qmtlgx8SS")
	if err != nil {
		log.Fatal(err)
	}

	_ = client

	os.Exit(0)
}
