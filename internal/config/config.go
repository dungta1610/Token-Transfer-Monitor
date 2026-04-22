package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	App        AppConfig
	Blockchain BlockchainConfig
	Postgres   PostgresConfig
	RabbitMQ   RabbitMQConfig
	Worker     WorkerConfig
	Shutdown   ShutdownConfig
}

type AppConfig struct {
	Name     string
	Env      string
	Port     int
	LogLevel string
}

type BlockchainConfig struct {
	ChainID            int64
	EVMWSURL           string
	EVMHTTPURL         string
	ConfirmationBlocks int
	TrackedTokenAddrs  []string
	WatchlistAddrs     []string
}

type PostgresConfig struct {
	Host         string
	Port         int
	DB           string
	User         string
	Password     string
	SSLMode      string
	DatabaseURL  string
	MaxOpenConns int
	MinIdleConns int
	MaxIdleTime  time.Duration
}

type RabbitMQConfig struct {
	Host           string
	AMQPPort       int
	ManagementPort int
	User           string
	Password       string
	VHost          string
	AMQPURL        string
	Topology       RabbitMQTopologyConfig
}

type RabbitMQTopologyConfig struct {
	MainExchange       string
	MainExchangeType   string
	RetryExchange      string
	RetryExchangeType  string
	DLXExchange        string
	DLXExchangeType    string
	TransferQueue      string
	TransferRoutingKey string
	TransferRetryQueue string
	TransferRetryKey   string
	TransferDLQ        string
	TransferDLQKey     string
}

type WorkerConfig struct {
	ConsumerTag   string
	PrefetchCount int
	MaxRetryCount int
	RetryDelayMS  int
	IdempotencyOn bool
}

type ShutdownConfig struct {
	Timeout time.Duration
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{
		App: AppConfig{
			Name:     getString("APP_NAME", "token-transfer-monitor"),
			Env:      getString("APP_ENV", "development"),
			Port:     getInt("APP_PORT", 8080),
			LogLevel: getString("LOG_LEVEL", "debug"),
		},
		Blockchain: BlockchainConfig{
			ChainID:            getInt64("CHAIN_ID", 11155111),
			EVMWSURL:           getString("EVM_WS_URL", "ws://localhost:8546"),
			EVMHTTPURL:         getString("EVM_HTTP_URL", "http://localhost:8545"),
			ConfirmationBlocks: getInt("CONFIRMATION_BLOCKS", 0),
			TrackedTokenAddrs:  splitCSV(getString("TRACKED_TOKEN_ADDRESSES", "")),
			WatchlistAddrs:     splitCSV(getString("WATCHLIST_ADDRESSES", "")),
		},
		Postgres: PostgresConfig{
			Host:         getString("POSTGRES_HOST", "localhost"),
			Port:         getInt("POSTGRES_PORT", 5434),
			DB:           getString("POSTGRES_DB", "ttm"),
			User:         getString("POSTGRES_USER", "ttm"),
			Password:     getString("POSTGRES_PASSWORD", "ttm_password"),
			SSLMode:      getString("POSTGRES_SSLMODE", "disable"),
			DatabaseURL:  getString("DATABASE_URL", ""),
			MaxOpenConns: getInt("POSTGRES_MAX_OPEN_CONNS", 20),
			MinIdleConns: getInt("POSTGRES_MIN_IDLE_CONNS", 5),
			MaxIdleTime:  getDurationSeconds("POSTGRES_MAX_IDLE_TIME_SECONDS", 300),
		},
		RabbitMQ: RabbitMQConfig{
			Host:           getString("RABBITMQ_HOST", "localhost"),
			AMQPPort:       getInt("RABBITMQ_AMQP_PORT", 5672),
			ManagementPort: getInt("RABBITMQ_MANAGEMENT_PORT", 15672),
			User:           getString("RABBITMQ_USER", "ttm"),
			Password:       getString("RABBITMQ_PASSWORD", "ttm_password"),
			VHost:          getString("RABBITMQ_VHOST", "/"),
			AMQPURL:        getString("AMQP_URL", ""),
			Topology: RabbitMQTopologyConfig{
				MainExchange:       getString("RABBITMQ_MAIN_EXCHANGE", "token.events"),
				MainExchangeType:   getString("RABBITMQ_MAIN_EXCHANGE_TYPE", "topic"),
				RetryExchange:      getString("RABBITMQ_RETRY_EXCHANGE", "token.retry"),
				RetryExchangeType:  getString("RABBITMQ_RETRY_EXCHANGE_TYPE", "direct"),
				DLXExchange:        getString("RABBITMQ_DLX_EXCHANGE", "token.dlx"),
				DLXExchangeType:    getString("RABBITMQ_DLX_EXCHANGE_TYPE", "direct"),
				TransferQueue:      getString("RABBITMQ_TRANSFER_QUEUE", "token.transfer.process"),
				TransferRoutingKey: getString("RABBITMQ_TRANSFER_ROUTING_KEY", "token.transfer.detected"),
				TransferRetryQueue: getString("RABBITMQ_TRANSFER_RETRY_QUEUE", "token.transfer.process.retry.5s"),
				TransferRetryKey:   getString("RABBITMQ_TRANSFER_RETRY_ROUTING_KEY", "token.transfer.retry"),
				TransferDLQ:        getString("RABBITMQ_TRANSFER_DLQ", "token.transfer.process.dlq"),
				TransferDLQKey:     getString("RABBITMQ_TRANSFER_DLQ_ROUTING_KEY", "token.transfer.dead"),
			},
		},
		Worker: WorkerConfig{
			ConsumerTag:   getString("WORKER_CONSUMER_TAG", "transfer-worker"),
			PrefetchCount: getInt("WORKER_PREFETCH_COUNT", 10),
			MaxRetryCount: getInt("MAX_RETRY_COUNT", 3),
			RetryDelayMS:  getInt("RETRY_DELAY_MS", 5000),
			IdempotencyOn: getBool("IDEMPOTENCY_ENABLED", true),
		},
		Shutdown: ShutdownConfig{
			Timeout: getDurationSeconds("SHUTDOWN_TIMEOUT_SECONDS", 15),
		},
	}

	if cfg.Postgres.DatabaseURL == "" {
		cfg.Postgres.DatabaseURL = fmt.Sprintf(
			"postgres://%s:%s@%s:%d/%s?sslmode=%s",
			cfg.Postgres.User,
			cfg.Postgres.Password,
			cfg.Postgres.Host,
			cfg.Postgres.Port,
			cfg.Postgres.DB,
			cfg.Postgres.SSLMode,
		)
	}

	if cfg.RabbitMQ.AMQPURL == "" {
		cfg.RabbitMQ.AMQPURL = fmt.Sprintf(
			"amqp://%s:%s@%s:%d/%s",
			cfg.RabbitMQ.User,
			cfg.RabbitMQ.Password,
			cfg.RabbitMQ.Host,
			cfg.RabbitMQ.AMQPPort,
			normalizeVHost(cfg.RabbitMQ.VHost),
		)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.Postgres.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is empty")
	}
	if c.RabbitMQ.AMQPURL == "" {
		return fmt.Errorf("AMQP_URL is empty")
	}
	if c.RabbitMQ.Topology.MainExchange == "" {
		return fmt.Errorf("RABBITMQ_MAIN_EXCHANGE is empty")
	}
	if c.RabbitMQ.Topology.TransferQueue == "" {
		return fmt.Errorf("RABBITMQ_TRANSFER_QUEUE is empty")
	}
	if c.Worker.PrefetchCount < 1 {
		return fmt.Errorf("WORKER_PREFETCH_COUNT must be >= 1")
	}
	if c.Worker.MaxRetryCount < 0 {
		return fmt.Errorf("MAX_RETRY_COUNT must be >= 0")
	}
	if c.Worker.RetryDelayMS < 1 {
		return fmt.Errorf("RETRY_DELAY_MS must be >= 1")
	}
	return nil
}

func getString(key, fallback string) string {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback
	}
	return val
}

func getInt(key string, fallback int) int {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return parsed
}

func getInt64(key string, fallback int64) int64 {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func getBool(key string, fallback bool) bool {
	val := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if val == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(val)
	if err != nil {
		return fallback
	}
	return parsed
}

func getDurationSeconds(key string, fallbackSeconds int) time.Duration {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return time.Duration(fallbackSeconds) * time.Second
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return time.Duration(fallbackSeconds) * time.Second
	}
	return time.Duration(parsed) * time.Second
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))

	for _, p := range parts {
		item := strings.TrimSpace(strings.ToLower(p))
		if item == "" {
			continue
		}
		out = append(out, item)
	}

	return out
}

func normalizeVHost(vhost string) string {
	vhost = strings.TrimSpace(vhost)
	if vhost == "" || vhost == "/" {
		return ""
	}
	return strings.TrimPrefix(vhost, "/")
}
