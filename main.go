package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"

	_ "github.com/lib/pq"

	"github.com/influxdata/influxdb-client-go/v2/api/write"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

const (
	measurement = "Uniswap v2"
	chainList   = "chains.json"
)

func main() {

	var (
		logLevel     string
		batchSize    uint
		writeMetrics bool

		pairAddress string
		startHeight uint64

		apiURL string

		influxURL    string
		influxToken  string
		influxOrg    string
		influxBucket string
	)

	pflag.StringVarP(&logLevel, "log-level", "l", "info", "Zerolog logger minimum severity level")
	pflag.BoolVarP(&writeMetrics, "write-metrics", "w", false, "whether to write the datapoints to InfluxDB")
	pflag.UintVarP(&batchSize, "batch-size", "b", 100, "number of blocks to cover per request for log entries")

	pflag.StringVarP(&apiURL, "api-url", "a", "", "JSON RPC API URL")
	pflag.StringVarP(&pairAddress, "pair-address", "p", "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc", "Ethereum address for Uniswap v2 pair")
	pflag.Uint64VarP(&startHeight, "start-height", "s", 10019997, "start height for parsing Uniswap v2 pair events")

	pflag.StringVarP(&influxURL, "influx-url", "i", "https://eu-central-1-1.aws.cloud2.influxdata.com", "InfluxDB API URL")
	pflag.StringVarP(&influxOrg, "influx-org", "o", "optakt", "InfluxDB organization name")
	pflag.StringVarP(&influxBucket, "influx-metrics-bucket", "m", "metrics", "InfluxDB bucket name")
	pflag.StringVarP(&influxToken, "influx-token", "t", "", "InfluxDB authentication token")

	pflag.Parse()

	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }
	log := zerolog.New(os.Stdout).With().Timestamp().Logger()
	level, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		log.Fatal().Str("log_level", logLevel).Err(err).Msg("invalid log level")
	}
	log = log.Level(level)

	pairABI, err := abi.JSON(strings.NewReader(PairMetaData.ABI))
	if err != nil {
		log.Fatal().Err(err).Msg("invalid Uniswap Pair ABI")
	}

	chainsData, err := os.ReadFile(chainList)
	if err != nil {
		log.Fatal().Err(err).Msg("could not read chain list")
	}

	var chains []Chain
	err = json.Unmarshal(chainsData, &chains)
	if err != nil {
		log.Fatal().Err(err).Msg("could not decode chain list")
	}

	chainLookup := make(map[uint64]string)
	for _, chain := range chains {
		chainLookup[chain.ChainID] = chain.Name
	}

	client, err := ethclient.Dial(apiURL)
	if err != nil {
		log.Fatal().Str("api_url", apiURL).Err(err).Msg("could not connect to JSON RPC API")
	}

	chainID, err := client.ChainID(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("could not get chain ID")
	}

	chainName, ok := chainLookup[chainID.Uint64()]
	if !ok {
		log.Fatal().Uint64("chain_id", chainID.Uint64()).Msg("unknown chain ID")
	}

	lastHeight, err := client.BlockNumber(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("could not get last block height")
	}

	pairContract, err := NewPairCaller(common.HexToAddress(pairAddress), client)
	if err != nil {
		log.Fatal().Err(err).Msg("could not bind pair contract")
	}

	address0, err := pairContract.Token0(nil)
	if err != nil {
		log.Fatal().Err(err).Msg("could not get first token address")
	}
	address1, err := pairContract.Token1(nil)
	if err != nil {
		log.Fatal().Err(err).Msg("could not get second token address")
	}

	token0, err := NewERC20Caller(address0, client)
	if err != nil {
		log.Fatal().Err(err).Msg("could not bind first token contract")
	}
	token1, err := NewERC20Caller(address1, client)
	if err != nil {
		log.Fatal().Err(err).Msg("could not bind second token contract")
	}

	symbol0, err := token0.Symbol(nil)
	if err != nil {
		log.Fatal().Err(err).Msg("could not get first token symbol")
	}
	symbol1, err := token1.Symbol(nil)
	if err != nil {
		log.Fatal().Err(err).Msg("could not get second token symbol")
	}

	pairName := symbol0 + "/" + symbol1

	log = log.With().
		Str("bucket", influxBucket).
		Str("measurement", measurement).
		Str("chain_name", chainName).
		Str("pair_name", pairName).
		Logger()

	log.Info().Msg("determined labels for datapoints")

	influx := influxdb2.NewClient(influxURL, influxToken)
	ok, err = influx.Ready(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("could not connect to InfluxDB API")
	}
	if !ok {
		log.Fatal().Msg("InfluxDB API not ready")
	}

	batch := influx.WriteAPI(influxOrg, influxBucket)
	go func() {
		for err := range batch.Errors() {
			log.Fatal().Err(err).Msg("encountered InfluxDB error")
		}
	}()

	for from := startHeight; from < lastHeight; from += uint64(batchSize) {

		if from > lastHeight {
			from = lastHeight
		}

		to := from + uint64(batchSize) - 1

		log = log.With().Uint64("from", from).Uint64("to", to).Logger()

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

		log.Debug().Int("entries", len(entries)).Msg("processing log entries for block range")

		timestamps := make(map[uint64]time.Time)
		reserves0 := make(map[uint64]*big.Int)
		reserves1 := make(map[uint64]*big.Int)
		volumes0 := make(map[uint64]*big.Int)
		volumes1 := make(map[uint64]*big.Int)

		var swap Swap
		var sick Sync
		for _, entry := range entries {

			height := entry.BlockNumber
			timestamps[height] = time.Time{}

			switch entry.Topics[0] {

			case SigSync:

				err := pairABI.UnpackIntoInterface(&sick, "Sync", entry.Data)
				if err != nil {
					log.Fatal().Err(err).Msg("could not unpack sync event")
				}

				reserve0, ok := reserves0[height]
				if !ok {
					reserve0 = big.NewInt(0)
					reserves0[height] = reserve0
				}
				reserve0.Add(reserve0, sick.Reserve0)

				reserve1, ok := reserves1[height]
				if !ok {
					reserve1 = big.NewInt(0)
					reserves1[height] = reserve1
				}
				reserve1.Add(reserve1, sick.Reserve1)

				log.Debug().
					Str("reserve0", sick.Reserve0.String()).
					Str("reserve1", sick.Reserve1.String()).
					Msg("sync decoded")

			case SigSwap:

				err := pairABI.UnpackIntoInterface(&swap, "Swap", entry.Data)
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
					Str("volume0", volume0.String()).
					Str("volume1", volume1.String()).
					Msg("swap decoded")
			}
		}

		heights := make([]uint64, 0, len(timestamps))
		for height := range timestamps {
			heights = append(heights, height)
		}
		sort.Slice(heights, func(i int, j int) bool {
			return heights[i] < heights[j]
		})

		log.Debug().Int("heights", len(heights)).Msg("retrieving timestamps for heights")

		wg := &sync.WaitGroup{}
		for _, height := range heights {
			wg.Add(1)
			go func(height uint64) {
				defer wg.Done()
				header, err := client.HeaderByNumber(context.Background(), big.NewInt(0).SetUint64(height))
				if err != nil {
					log.Fatal().Uint64("height", height).Err(err).Msg("could not get header for height")
				}
				timestamps[height] = time.Unix(int64(header.Time), 0).UTC()
			}(height)
		}
		wg.Wait()

		log.Debug().Int("heights", len(heights)).Msg("writing datapoints for heights")

		for _, height := range heights {

			timestamp := timestamps[height]

			reserve0, ok := reserves0[height]
			if !ok {
				reserve0 = big.NewInt(0)
			}
			reserve1, ok := reserves1[height]
			if !ok {
				reserve1 = big.NewInt(0)
			}

			volume0, ok := volumes0[height]
			if !ok {
				volume0 = big.NewInt(0)
			}
			volume1, ok := volumes1[height]
			if !ok {
				volume1 = big.NewInt(0)
			}

			if writeMetrics {
				tags := map[string]string{
					"chain": chainName,
					"pair":  pairName,
				}
				fields := map[string]interface{}{
					"reserve0": hex.EncodeToString(reserve0.Bytes()),
					"reserve1": hex.EncodeToString(reserve1.Bytes()),
					"volume0":  hex.EncodeToString(volume0.Bytes()),
					"volume1":  hex.EncodeToString(volume1.Bytes()),
				}

				point := write.NewPoint(measurement, tags, fields, timestamp)
				batch.WritePoint(point)
			}

			log.Debug().
				Time("timestamp", timestamp).
				Str("reserve0", reserve0.String()).
				Str("reserve1", reserve1.String()).
				Str("volume0", volume0.String()).
				Str("volume1", volume1.String()).
				Msg("datapoint queued for writing")

		}

		log.Info().Int("entries", len(entries)).Int("heights", len(heights)).Msg("processed log entries for block range")
	}

	batch.Flush()

	log.Info().Msg("stopping klangbaach data miner")

	os.Exit(0)
}
