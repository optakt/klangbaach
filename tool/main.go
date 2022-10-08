package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

func main() {

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	client := lambda.NewFromConfig(cfg)

	start := time.Date(2020, time.May, 7, 0, 0, 0, 0, time.UTC)
	end := time.Date(2022, time.October, 8, 0, 0, 0, 0, time.UTC)

	for date := start; date.Before(end); date = date.AddDate(0, 0, 1) {
		go func() {
			name := fmt.Sprintf("blockchair_ethereum_blocks_%s.tsv.gz", date.Format("20060102"))
			url := fmt.Sprintf("https://gz.blockchair.com/ethereum/blocks/" + name)
			input := lambda.InvokeInput{
				FunctionName: aws.String("worker"),
				Payload:      []byte(url),
			}
			output, err := client.Invoke(context.Background(), &input)
			if err != nil {
				log.Fatal(err)
			}
			err = os.WriteFile(name, output.Payload, 0644)
			if err != nil {
				log.Fatal(err)
			}
		}()
		time.Sleep(time.Second / 50)
	}

	os.Exit(0)
}
