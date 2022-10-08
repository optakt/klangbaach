package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {

	file, err := os.OpenFile("export-Height2Time.csv", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	write := bufio.NewWriter(file)

	eth, err := ethclient.Dial("https://eth-mainnet.g.alchemy.com/v2/UxuHkw-MO02DZ9qYM3usei-qmtlgx8SS")
	if err != nil {
		log.Fatal(err)
	}

	for height := uint64(10019997); height <= 15701829; height++ {

		header, err := eth.HeaderByNumber(context.Background(), big.NewInt(0).SetUint64(height))
		if err != nil {
			log.Fatal(err)
		}

		timestamp := time.Unix(int64(header.Time), 0).UTC()

		_, err = write.WriteString(fmt.Sprintf("\"%d\",\"%s\"\n", height, timestamp.Format(time.RFC3339)))
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("%d => %s\n", height, timestamp.Format(time.RFC3339))
	}

	err = write.Flush()
	if err != nil {
		log.Fatal(err)
	}

	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}

	os.Exit(0)
}
