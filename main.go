package main

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"

	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {

	var (
		apiURL   string
		logLevel string
	)

	pflag.StringVarP(&apiURL, "api-url", "a", "https://eth-mainnet.g.alchemy.com/v2/UxuHkw-MO02DZ9qYM3usei-qmtlgx8SS", "Ethereum node JSON RPC API URL")
	pflag.StringVarP(&logLevel, "log-level", "l", "info", "Zerolog logger minimum severity level")

	pflag.Parse()

	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	level, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		log.Fatal().Str("log_level", logLevel).Err(err).Msg("invalid zerolog log level")
	}
	log.Level(level)

	log.Info().Msg("starting klangbaach data miner")

	client, err := ethclient.Dial(apiURL)
	if err != nil {
		log.Fatal().Str("api_url", apiURL).Err(err).Msg("could not connect to Ethereum API")
	}

	_ = client

	log.Info().Msg("stopping klangbaach data miner")

	os.Exit(0)
}
