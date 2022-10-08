package main

import (
	"context"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
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
		firstStable bool
	)

	pflag.StringVarP(&apiURL, "api-url", "a", "https://eth-mainnet.g.alchemy.com/v2/UxuHkw-MO02DZ9qYM3usei-qmtlgx8SS", "Ethereum node JSON RPC API URL")
	pflag.StringVarP(&logLevel, "log-level", "l", "info", "Zerolog logger minimum severity level")
	pflag.StringVarP(&pairAddress, "pair-address", "p", "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc", "Ethereum address for Uniswap v2 pair")
	pflag.Uint64VarP(&startHeight, "start-height", "s", 10019997, "start height for parsing Uniswap v2 pair events")
	pflag.UintVarP(&batchSize, "batch-size", "b", 100, "number of blocks to cover per request for log entries")
	pflag.BoolVarP(&firstStable, "first-stable", "f", false, "whether the first symbol in the pair is the stable")

	pflag.Parse()

	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	level, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		log.Fatal().Str("log_level", logLevel).Err(err).Msg("invalid zerolog log level")
	}
	log.Level(level)

	log.Info().Msg("starting klangbaach data miner")

	pair, err := abi.JSON(strings.NewReader(ABIPair))
	if err != nil {
		log.Fatal().Err(err).Msg("invalid Uniswap Pair ABI")
	}

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
			Topics:    [][]common.Hash{{SigSwap, SigSync}},
		}

		entries, err := client.FilterLogs(context.Background(), query)
		if err != nil {
			log.Fatal().Err(err).Msg("could not retrieve filtered log entries")
		}

		var swap Swap
		var sync Sync
		volumes0 := make(map[uint64]*big.Int)
		volumes1 := make(map[uint64]*big.Int)
		liquidities := make(map[uint64]*big.Int)
		timestamps := make(map[uint64]time.Time)
		for _, entry := range entries {

			timestamps[entry.BlockNumber] = time.Time{}

			switch entry.Topics[0] {

			case SigSwap:

				err := pair.UnpackIntoInterface(&swap, "Swap", entry.Data)
				if err != nil {
					log.Fatal().Err(err).Msg("could not unpack swap event")
				}

				volume0, ok := volumes0[entry.BlockNumber]
				if !ok {
					volume0 = big.NewInt(0)
				}
				volume0.Add(volume0, swap.Amount0In)

				volume1, ok := volumes1[entry.BlockNumber]
				if !ok {
					volume1 = big.NewInt(0)
				}
				volume1.Add(volume1, swap.Amount1In)

				log.Debug().
					Str("amount0in", swap.Amount0In.String()).
					Str("amount1in", swap.Amount1In.String()).
					Str("amount0out", swap.Amount0Out.String()).
					Str("amount1out", swap.Amount1Out.String()).
					Str("volume0", volume0.String()).
					Str("volume1", volume1.String()).
					Msg("swap decoded")

			case SigSync:

				err := pair.UnpackIntoInterface(&sync, "Sync", entry.Data)
				if err != nil {
					log.Fatal().Err(err).Msg("could not unpack sync event")
				}

				liquidity, ok := liquidities[entry.BlockNumber]
				if !ok {
					liquidity = big.NewInt(0)
				}
				liquidity.Mul(sync.Reserve0, sync.Reserve1)
				liquidity.Sqrt(liquidity)

				log.Debug().
					Str("reserve0", sync.Reserve0.String()).
					Str("reserve1", sync.Reserve1.String()).
					Str("liquidity", liquidity.String()).
					Msg("sync decoded")
			}
		}

		for height := range timestamps {

			header, err := client.HeaderByNumber(context.Background(), big.NewInt(0).SetUint64(height))
			if err != nil {
				log.Fatal().Uint64("height", height).Err(err).Msg("could not get header for height")
			}

			timestamps[height] = time.Unix(int64(header.Time), 0)
		}
	}

	log.Info().Msg("stopping klangbaach data miner")

	os.Exit(0)
}
