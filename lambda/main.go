package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/valyala/tsvreader"

	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	lambda.Start(handle)
}

type Block struct {
	Height    uint64 `json:"height"`
	Timestamp uint64 `json:"timestamp"`
}

func handle(url string) ([]Block, error) {

	var err error
	var res *http.Response
	for {

		res, err = http.Get(url)
		if err != nil {
			return nil, fmt.Errorf("could not execute HTTP request: %w", err)
		}

		if res.StatusCode == http.StatusOK {
			defer res.Body.Close()
			break
		}

		res.Body.Close()

		if res.StatusCode == http.StatusPaymentRequired {
			time.Sleep(2 * time.Second)
			continue
		}

		return nil, fmt.Errorf("invalid status code (%d)", res.StatusCode)
	}

	gzr, err := gzip.NewReader(res.Body)
	if err != nil {
		return nil, fmt.Errorf("could not create GZIP reader: %w", err)
	}

	data, err := io.ReadAll(gzr)
	if err != nil {
		return nil, fmt.Errorf("could not decode GZIP body: %w", err)
	}

	tsvr := tsvreader.New(bytes.NewReader(data))
	tsvr.Next()
	for tsvr.HasCols() {
		tsvr.SkipCol()
	}

	var blocks []Block
	for tsvr.Next() {

		height := tsvr.Uint64()
		tsvr.SkipCol()
		timestamp := tsvr.DateTime()

		block := Block{
			Height:    height,
			Timestamp: uint64(timestamp.Unix()),
		}
		blocks = append(blocks, block)

		for tsvr.HasCols() {
			tsvr.SkipCol()
		}
	}
	err = tsvr.Error()
	if err != nil && !strings.Contains(err.Error(), "cannot find newline at the end of row") {
		return nil, fmt.Errorf("could not iterate through TSV rows: %w", err)
	}

	return blocks, nil
}
