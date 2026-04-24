package blockchain

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

var ERC20TransferTopic = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

type TransferDecoder struct {
	chainID int64
}

func NewTransferDecoder(chainID int64) *TransferDecoder {
	return &TransferDecoder{
		chainID: chainID,
	}
}

func (d *TransferDecoder) Decode(log types.Log) (ERC20TransferEvent, error) {
	if len(log.Topics) != 3 {
		return ERC20TransferEvent{}, fmt.Errorf("invalid transfer log topics length: got %d, want 3", len(log.Topics))
	}

	if log.Topics[0] != ERC20TransferTopic {
		return ERC20TransferEvent{}, fmt.Errorf("log topic is not ERC20 Transfer event")
	}

	if len(log.Data) == 0 {
		return ERC20TransferEvent{}, fmt.Errorf("transfer amount data is empty")
	}

	from := common.BytesToAddress(log.Topics[1].Bytes())
	to := common.BytesToAddress(log.Topics[2].Bytes())
	amount := new(big.Int).SetBytes(log.Data)

	return ERC20TransferEvent{
		ChainID:         d.chainID,
		ContractAddress: log.Address,
		From:            from,
		To:              to,
		Amount:          amount,
		TxHash:          log.TxHash,
		LogIndex:        log.Index,
		BlockNumber:     log.BlockNumber,
		BlockHash:       log.BlockHash,
		Removed:         log.Removed,
		ObservedAt:      time.Now().UTC(),
	}, nil
}
