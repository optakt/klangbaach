package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {

	var (
		ethAPI       string
		logLevel     string
		pairAddress  string
		pairName     string
		startHeight  uint64
		batchSize    uint
		firstStable  bool
		influxAPI    string
		influxToken  string
		influxOrg    string
		influxBucket string
		gasPrices    string
	)

	pflag.StringVarP(&ethAPI, "eth-api", "e", "https://eth-mainnet.g.alchemy.com/v2/UxuHkw-MO02DZ9qYM3usei-qmtlgx8SS", "Ethereum node JSON RPC API URL")
	pflag.StringVarP(&logLevel, "log-level", "l", "info", "Zerolog logger minimum severity level")
	pflag.StringVarP(&pairAddress, "pair-address", "p", "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc", "Ethereum address for Uniswap v2 pair")
	pflag.StringVarP(&pairName, "pair-name", "n", "WETH/USDC", "name of the Uniswap v2 pair")
	pflag.Uint64VarP(&startHeight, "start-height", "s", 10019997, "start height for parsing Uniswap v2 pair events")
	pflag.UintVarP(&batchSize, "batch-size", "b", 100, "number of blocks to cover per request for log entries")
	pflag.BoolVarP(&firstStable, "first-stable", "f", false, "whether the first symbol in the pair is the stable")
	pflag.StringVarP(&influxAPI, "influx-api", "i", "https://eu-central-1-1.aws.cloud2.influxdata.com", "InfluxDB API URL")
	pflag.StringVarP(&influxToken, "influx-token", "t", "3Lq2o0e6-NmfpXK_UQbPqknKgQUbALMdNz86Ojhpm6dXGqGnCuEYGZijTMGhP82uxLfoWiWZRS2Vls0n4dZAjQ==", "InfluxDB authentication token")
	pflag.StringVarP(&influxOrg, "influx-org", "o", "optakt", "InfluxDB organization name")
	pflag.StringVarP(&influxBucket, "influx-bucket", "u", "uniswap", "InfluxDB bucket name")
	pflag.StringVarP(&gasPrices, "gas-prices", "g", "export-AvgGasPrice.csv", "CSV file for average gas price per day")

	pflag.Parse()

	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	level, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		log.Fatal().Str("log_level", logLevel).Err(err).Msg("invalid zerolog log level")
	}
	log = log.Level(level)

	log.Info().Msg("starting klangbaach data miner")

	pair, err := abi.JSON(strings.NewReader(ABIPair))
	if err != nil {
		log.Fatal().Err(err).Msg("invalid Uniswap Pair ABI")
	}

	data, err := os.ReadFile(gasPrices)
	if err != nil {
		log.Fatal().Err(err).Msg("could not read gas prices from file")
	}
	read := csv.NewReader(bytes.NewReader(data))
	records, err := read.ReadAll()
	if err != nil {
		log.Fatal().Err(err).Msg("could not read gas price records")
	}
	prices := make(map[time.Time]uint64, len(records))
	for _, record := range records[1:] {
		day, err := time.Parse("1/2/2006", record[0])
		if err != nil {
			log.Fatal().Err(err).Msg("could not parse gas price day")
		}
		value, err := strconv.ParseUint(record[2], 10, 64)
		if err != nil {
			log.Fatal().Err(err).Msg("could not parse gas price value")
		}
		prices[day] = value
		fmt.Printf("%s - %d\n", day, value)
	}

	eth, err := ethclient.Dial(ethAPI)
	if err != nil {
		log.Fatal().Str("ethAPI", ethAPI).Err(err).Msg("could not connect to Ethereum API")
	}
	lastHeight, err := eth.BlockNumber(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("could not get last block height")
	}

	influx := influxdb2.NewClient(influxAPI, influxToken)
	ok, err := influx.Ready(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("could not connect to InfluxDB API")
	}
	if !ok {
		log.Fatal().Msg("InfluxDB API not ready")
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

		entries, err := eth.FilterLogs(context.Background(), query)
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

			height := entry.BlockNumber
			timestamps[height] = time.Time{}

			switch entry.Topics[0] {

			case SigSwap:

				err := pair.UnpackIntoInterface(&swap, "Swap", entry.Data)
				if err != nil {
					log.Fatal().Err(err).Msg("could not unpack swap event")
				}

				volume0, ok := volumes0[height]
				if !ok {
					volume0 = big.NewInt(0)
					volumes0[height] = volume0
				}
				volume0.Add(volume0, swap.Amount0In)

				volume1, ok := volumes1[height]
				if !ok {
					volume1 = big.NewInt(0)
					volumes1[height] = volume1
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

				liquidity, ok := liquidities[height]
				if !ok {
					liquidity = big.NewInt(0)
					liquidities[height] = liquidity
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

		heights := make([]uint64, 0, len(timestamps))
		for height := range timestamps {

			header, err := eth.HeaderByNumber(context.Background(), big.NewInt(0).SetUint64(height))
			if err != nil {
				log.Fatal().Uint64("height", height).Err(err).Msg("could not get header for height")
			}

			timestamps[height] = time.Unix(int64(header.Time), 0).UTC()
			heights = append(heights, height)
		}

		sort.Slice(heights, func(i int, j int) bool {
			return heights[i] < heights[j]
		})

		points := make([]*write.Point, 0, len(heights))
		for _, height := range heights {

			timestamp := timestamps[height]
			volume0, ok := volumes0[height]
			if !ok {
				volume0 = big.NewInt(0)
			}
			volume1, ok := volumes1[height]
			if !ok {
				volume1 = big.NewInt(0)
			}
			liquidity, ok := liquidities[height]
			if !ok {
				liquidity = big.NewInt(0)
			}

			volume0Float, _ := big.NewFloat(0).SetInt(volume0).Float64()
			volume1Float, _ := big.NewFloat(0).SetInt(volume1).Float64()
			liquidityFloat, _ := big.NewFloat(0).SetInt(liquidity).Float64()

			log.Info().
				Time("timestamp", timestamp).
				Float64("volume0", volume0Float).
				Float64("volume1", volume1Float).
				Float64("liquidity", liquidityFloat).
				Msg("creating datapoint")

			tags := map[string]string{
				"pair": pairName,
			}
			fields := map[string]interface{}{
				"volume0":   volume0Float,
				"volume1":   volume1Float,
				"liquidity": liquidityFloat,
			}

			point := write.NewPoint("ethereum", tags, fields, timestamp)
			points = append(points, point)
		}

		batch := influx.WriteAPIBlocking(influxOrg, influxBucket)
		err = batch.WritePoint(context.Background(), points...)
		if err != nil {
			log.Fatal().Err(err).Msg("could not write influxdb points")
		}
	}

	log.Info().Msg("stopping klangbaach data miner")

	os.Exit(0)
}
