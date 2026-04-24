package blockchain

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

type EVMClient struct {
	wsClient   *ethclient.Client
	httpClient *ethclient.Client
}

func NewEVMClient(ctx context.Context, wsURL string, httpURL string) (*EVMClient, error) {
	if wsURL == "" {
		return nil, fmt.Errorf("EVM_WS_URL is required")
	}

	wsCtx, wsCancel := context.WithTimeout(ctx, 10*time.Second)
	defer wsCancel()

	wsClient, err := ethclient.DialContext(wsCtx, wsURL)
	if err != nil {
		return nil, fmt.Errorf("connect websocket evm client: %w", err)
	}

	var httpClient *ethclient.Client
	if httpURL != "" {
		httpCtx, httpCancel := context.WithTimeout(ctx, 10*time.Second)
		defer httpCancel()

		httpClient, err = ethclient.DialContext(httpCtx, httpURL)
		if err != nil {
			wsClient.Close()
			return nil, fmt.Errorf("connect http evm client: %w", err)
		}
	}

	return &EVMClient{
		wsClient:   wsClient,
		httpClient: httpClient,
	}, nil
}

func (c *EVMClient) WS() *ethclient.Client {
	return c.wsClient
}

func (c *EVMClient) HTTP() *ethclient.Client {
	return c.httpClient
}

func (c *EVMClient) Close() {
	if c == nil {
		return
	}

	if c.wsClient != nil {
		c.wsClient.Close()
	}

	if c.httpClient != nil {
		c.httpClient.Close()
	}
}
