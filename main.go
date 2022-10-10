package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"io"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
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

var chainAPIs = map[string]string{
	"ethereum": "https://eth-mainnet.g.alchemy.com/v2/",
	"polygon":  "https://polygon-mainnet.g.alchemy.com/v2/",
}

type Mapping struct {
	Height    uint64    `json:"height"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {

	var (
		logLevel    string
		batchSize   uint
		startHeight uint64

		timestampMapping string
		postgresServer   string

		chainName   string
		pairAddress string

		alchemyToken string

		influxAPI    string
		influxToken  string
		influxOrg    string
		influxBucket string
	)

	pflag.StringVarP(&logLevel, "log-level", "l", "info", "Zerolog logger minimum severity level")

	pflag.StringVarP(&timestampMapping, "timestamp-mapping", "t", "timestamps.csv", "CSV for block height to timestamp mapping")
	pflag.StringVarP(&postgresServer, "postgres-server", "g", "host=localhost port=5432 user=postgres password=postgres dbname=klangbaach sslmode=disable", "Postgres server connection string")

	pflag.StringVarP(&alchemyToken, "alchemy-token", "a", "", "Alchemy authentication token")
	pflag.StringVarP(&influxToken, "influx-token", "i", "", "InfluxDB authentication token")

	pflag.StringVarP(&chainName, "chain-name", "c", "ethereum", "name of the blockchain to index Uniswap on")
	pflag.StringVarP(&pairAddress, "pair-address", "p", "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc", "Ethereum address for Uniswap v2 pair")
	pflag.Uint64VarP(&startHeight, "start-height", "s", 10019997, "start height for parsing Uniswap v2 pair events")

	pflag.UintVar(&batchSize, "batch-size", 100, "number of blocks to cover per request for log entries")

	pflag.StringVar(&influxAPI, "influx-api", "https://eu-central-1-1.aws.cloud2.influxdata.com", "InfluxDB API URL")
	pflag.StringVar(&influxOrg, "influx-org", "optakt", "InfluxDB organization name")
	pflag.StringVar(&influxBucket, "influx-bucket", "uniswap", "InfluxDB bucket name")

	pflag.Parse()

	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	level, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		log.Fatal().Str("log_level", logLevel).Err(err).Msg("invalid log level")
	}
	log = log.Level(level)

	log.Info().Msg("starting klangbaach data miner")

	apiURL, ok := chainAPIs[chainName]
	if !ok {
		log.Fatal().Str("chain_name", chainName).Msg("unknow chain name")
	}

	abi, err := abi.JSON(strings.NewReader(PairMetaData.ABI))
	if err != nil {
		log.Fatal().Err(err).Msg("invalid Uniswap Pair ABI")
	}

	db, err := sqlx.Connect("postgres", postgresServer)
	if err != nil {
		log.Fatal().Str("postgres_server", postgresServer).Err(err).Msg("could not connect to Postgres server")
	}

	if timestampMapping != "" {

		log.Info().Msg("initializing block height to timestamp mapping")

		file, err := os.Open(timestampMapping)
		if err != nil {
			log.Fatal().Err(err).Msg("could not open height timestamps")
		}

		csvr := csv.NewReader(file)
		_, err = csvr.Read()
		if err != nil {
			log.Fatal().Err(err).Msg("could not read first line of height to timestamp records")
		}

		var mappings []Mapping
		for {
			record, err := csvr.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			height, err := strconv.ParseUint(record[0], 10, 64)
			if err != nil {
				log.Fatal().Err(err).Msg("could not parse block height")
			}
			unix, err := strconv.ParseUint(record[1], 10, 64)
			if err != nil {
				log.Fatal().Err(err).Msg("could not parse unix timestamp")
			}
			timestamp := time.Unix(int64(unix), 0).UTC()
			mapping := Mapping{
				Height:    height,
				Timestamp: timestamp,
			}
			mappings = append(mappings, mapping)
		}

		log.Debug().Int("mappings", len(mappings)).Msg("block height to timestamp mappings loaded from CSV file")

		err = file.Close()
		if err != nil {
			log.Fatal().Err(err).Msg("could not close mapping file")
		}

		for i := 0; i < len(mappings); i += 1000 {

			j := i + 1000
			if j > len(mappings) {
				j = len(mappings)
			}

			batch := mappings[i:j]

			_, err = db.NamedExec(`INSERT INTO height_to_timestamp (height, timestamp) VALUES (:height, :timestamp) ON CONFLICT DO NOTHING`, batch)
			if err != nil {
				log.Fatal().Err(err).Msg("could not execute batch insertion")
			}

			log.Debug().Int("from", i).Int("to", j).Msg("batch of mappings inserted into database")
		}
	}

	eth, err := ethclient.Dial(apiURL + alchemyToken)
	if err != nil {
		log.Fatal().Str("api_url", apiURL).Err(err).Msg("could not connect to chain API")
	}

	lastHeight, err := eth.BlockNumber(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("could not get last block height")
	}

	pair, err := NewPairCaller(common.HexToAddress(pairAddress), eth)
	if err != nil {
		log.Fatal().Err(err).Msg("could not bind pair contract")
	}

	address0, err := pair.Token0(nil)
	if err != nil {
		log.Fatal().Err(err).Msg("could not get first token address")
	}
	address1, err := pair.Token1(nil)
	if err != nil {
		log.Fatal().Err(err).Msg("could not get second token address")
	}

	token0, err := NewERC20Caller(address0, eth)
	if err != nil {
		log.Fatal().Err(err).Msg("could not bind first token contract")
	}
	token1, err := NewERC20Caller(address1, eth)
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

	influx := influxdb2.NewClient(influxAPI, influxToken)
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

		log.Debug().
			Uint64("from", from).
			Uint64("to", to).
			Int("entries", len(entries)).
			Msg("log entries fetched successfully")

		timestamps := make(map[uint64]time.Time)
		reserves0 := make(map[uint64]*big.Int)
		reserves1 := make(map[uint64]*big.Int)
		volumes0 := make(map[uint64]*big.Int)
		volumes1 := make(map[uint64]*big.Int)

		var swap Swap
		var sync Sync
		for _, entry := range entries {

			height := entry.BlockNumber
			timestamps[height] = time.Time{}

			switch entry.Topics[0] {

			case SigSync:

				err := abi.UnpackIntoInterface(&sync, "Sync", entry.Data)
				if err != nil {
					log.Fatal().Err(err).Msg("could not unpack sync event")
				}

				reserve0, ok := reserves0[height]
				if !ok {
					reserve0 = big.NewInt(0)
					reserves0[height] = reserve0
				}
				reserve0.Add(reserve0, sync.Reserve0)

				reserve1, ok := reserves1[height]
				if !ok {
					reserve1 = big.NewInt(0)
					reserves1[height] = reserve1
				}
				reserve1.Add(reserve1, sync.Reserve1)

				log.Debug().
					Str("reserve0", sync.Reserve0.String()).
					Str("reserve1", sync.Reserve1.String()).
					Msg("sync decoded")

			case SigSwap:

				err := abi.UnpackIntoInterface(&swap, "Swap", entry.Data)
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

		for height := range timestamps {

			var timestamp time.Time
			row := db.QueryRowx("SELECT timestamp FROM height_to_timestamp WHERE height=$1", height)
			err = row.Err()
			if err != nil {
				log.Fatal().Err(err).Msg("could not select rows from database")
			}
			err = row.Scan(&timestamp)
			if err == nil {
				timestamps[height] = timestamp
				continue
			}
			if !errors.Is(err, sql.ErrNoRows) {
				log.Fatal().Err(err).Msg("could not scan row to timestamp")
			}

			log.Warn().Uint64("height", height).Msg("height not found in database, executing API request")

			header, err := eth.HeaderByNumber(context.Background(), big.NewInt(0).SetUint64(height))
			if err != nil {
				log.Fatal().Uint64("height", height).Err(err).Msg("could not get header for height")
			}

			_, err = db.Exec(`INSERT INTO height_to_timestamp (height, timestamp) VALUES ($1, $2)`, height, timestamp)
			if err != nil {
				log.Fatal().Err(err).Msg("could not execute insertion of new timestamp")
			}

			timestamps[height] = time.Unix(int64(header.Time), 0).UTC()
		}

		log.Debug().Int("heights", len(timestamps)).Msg("heights successfully mapped to timestamps")

		heights := make([]uint64, 0, len(timestamps))
		for height := range timestamps {
			heights = append(heights, height)
		}
		sort.Slice(heights, func(i int, j int) bool {
			return heights[i] < heights[j]
		})

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

			log.Debug().
				Time("timestamp", timestamp).
				Str("reserve0", reserve0.String()).
				Str("reserve1", reserve1.String()).
				Str("volume0", volume0.String()).
				Str("volume1", volume1.String()).
				Msg("datapoint queued for writing")

			tags := map[string]string{
				"pair": symbol0 + "/" + symbol1,
			}
			fields := map[string]interface{}{
				"volume0":  hex.EncodeToString(volume0.Bytes()),
				"volume1":  hex.EncodeToString(volume1.Bytes()),
				"reserve0": hex.EncodeToString(reserve0.Bytes()),
				"reserve1": hex.EncodeToString(reserve1.Bytes()),
			}

			point := write.NewPoint("ethereum", tags, fields, timestamp)
			batch.WritePoint(point)
		}

		log.Info().
			Uint64("from", from).
			Uint64("to", to).
			Int("heights", len(heights)).
			Int("entries", len(entries)).
			Msg("successfully processed block batch")
	}

	batch.Flush()

	log.Info().Msg("stopping klangbaach data miner")

	os.Exit(0)
}
