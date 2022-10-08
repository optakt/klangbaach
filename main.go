package main

import (
	"context"
	"math/big"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {

	var (
		apiURL      string
		logLevel    string
		pairAddress string
		startHeight uint64
		batchSize   uint
	)

	pflag.StringVarP(&apiURL, "api-url", "a", "https://eth-mainnet.g.alchemy.com/v2/UxuHkw-MO02DZ9qYM3usei-qmtlgx8SS", "Ethereum node JSON RPC API URL")
	pflag.StringVarP(&logLevel, "log-level", "l", "info", "Zerolog logger minimum severity level")
	pflag.StringVarP(&pairAddress, "pair-address", "p", "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc", "Ethereum address for Uniswap v2 pair")
	pflag.Uint64VarP(&startHeight, "start-height", "s", 10019997, "start height for parsing Uniswap v2 pair events")
	pflag.UintVarP(&batchSize, "batch-size", "b", 100, "number of blocks to cover per request for log entries")

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

	lastHeight, err := client.BlockNumber(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("could not get last block height")
	}

	for from := startHeight; from < lastHeight; from += uint64(batchSize) {

		if from > lastHeight {
			from = lastHeight
		}

		to := from + uint64(batchSize) - 1

		query := ethereum.FilterQuery{
			FromBlock: big.NewInt(0).SetUint64(from),
			ToBlock:   big.NewInt(0).SetUint64(to),
			Addresses: []common.Address{common.HexToAddress(pairAddress)},
			Topics:    [][]common.Hash{{SigBurn, SigMint, SigSwap, SigSync}},
		}
		logs, err := client.FilterLogs(context.Background(), query)
		if err != nil {
			log.Fatal().Err(err).Msg("could not retrieve filtered log entries")
		}

		log.Info().
			Uint64("from", from).
			Uint64("to", to).
			Int("logs", len(logs)).
			Msg("filtered log entries retrieved")
	}

	log.Info().Msg("stopping klangbaach data miner")

	os.Exit(0)
}
