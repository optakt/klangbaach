package main

import (
	"os"
	"time"

	"github.com/rs/zerolog"

	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {

	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	log.Level(zerolog.DebugLevel)

	log.Info().Msg("starting klangbaach data miner")

	client, err := ethclient.Dial("https://eth-mainnet.g.alchemy.com/v2/UxuHkw-MO02DZ9qYM3usei-qmtlgx8SS")
	if err != nil {
		log.Fatal().Err(err).Msg("could not connect to Ethereum API")
	}

	_ = client

	log.Info().Msg("stopping klangbaach data miner")

	os.Exit(0)
}
