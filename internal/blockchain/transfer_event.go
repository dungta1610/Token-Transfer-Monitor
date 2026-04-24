package blockchain

import (
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"token-transfer-monitor/internal/contract"
)

type ERC20TransferEvent struct {
	ChainID         int64
	ContractAddress common.Address
	From            common.Address
	To              common.Address
	Amount          *big.Int
	TxHash          common.Hash
	LogIndex        uint
	BlockNumber     uint64
	BlockHash       common.Hash
	Removed         bool
	ObservedAt      time.Time
}

func (e ERC20TransferEvent) EventID() string {
	return fmt.Sprintf(
		"%d:%s:%d",
		e.ChainID,
		strings.ToLower(e.TxHash.Hex()),
		e.LogIndex,
	)
}

func (e ERC20TransferEvent) ToMessage() contract.TransferDetectedMessage {
	return contract.TransferDetectedMessage{
		EventID:         e.EventID(),
		EventType:       contract.TransferDetectedEventType,
		ChainID:         e.ChainID,
		ContractAddress: strings.ToLower(e.ContractAddress.Hex()),
		TokenSymbol:     "",
		From:            strings.ToLower(e.From.Hex()),
		To:              strings.ToLower(e.To.Hex()),
		AmountRaw:       e.Amount.String(),
		TxHash:          strings.ToLower(e.TxHash.Hex()),
		LogIndex:        int64(e.LogIndex),
		BlockNumber:     int64(e.BlockNumber),
		BlockHash:       strings.ToLower(e.BlockHash.Hex()),
		ObservedAt:      e.ObservedAt,
	}
}
