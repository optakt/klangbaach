package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"golang.org/x/sync/semaphore"
)

type Block struct {
	Height    uint64 `json:"height"`
	Timestamp uint64 `json:"timestamp"`
}

func main() {

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	client := lambda.NewFromConfig(cfg)

	file, err := os.OpenFile("export-Height2Time.csv", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	bufw := bufio.NewWriter(file)
	_, err = bufw.WriteString("height,timestamp\n")
	if err != nil {
		log.Fatal(err)
	}

	start := time.Date(2020, time.May, 7, 0, 0, 0, 0, time.UTC)
	end := time.Date(2022, time.October, 8, 0, 0, 0, 0, time.UTC)

	done := make(chan struct{})
	c := make(chan []Block, 1000)
	go func() {
		for blocks := range c {
			for _, block := range blocks {
				line := fmt.Sprintf("%d,%d\n", block.Height, block.Timestamp)
				_, err = bufw.WriteString(line)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
		close(done)
	}()

	sema := semaphore.NewWeighted(100)
	wg := &sync.WaitGroup{}
	for date := start; date.Before(end); date = date.AddDate(0, 0, 1) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sema.Acquire(context.Background(), 1)
			defer sema.Release(1)
			name := fmt.Sprintf("blockchair_ethereum_blocks_%s.tsv.gz", date.Format("20060102"))
			url := fmt.Sprintf("https://gz.blockchair.com/ethereum/blocks/" + name)
			payload, err := json.Marshal(url)
			if err != nil {
				log.Fatal(err)
			}
			input := lambda.InvokeInput{
				FunctionName: aws.String("worker"),
				Payload:      payload,
			}
			output, err := client.Invoke(context.Background(), &input)
			if err != nil {
				log.Fatal(err)
			}
			if output.FunctionError != nil {
				log.Fatal(*output.FunctionError)
			}
			var blocks []Block
			err = json.Unmarshal(output.Payload, &blocks)
			if err != nil {
				log.Fatal(err)
			}
			c <- blocks
		}()
		time.Sleep(time.Second)
	}
	wg.Wait()
	close(c)
	<-done

	err = bufw.Flush()
	if err != nil {
		log.Fatal(err)
	}

	os.Exit(0)
}
