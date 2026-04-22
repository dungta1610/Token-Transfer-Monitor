package contract

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type TransferDetectedMessage struct {
	EventID         string    `json:"eventId"`
	EventType       string    `json:"eventType"`
	ChainID         int64     `json:"chainId"`
	ContractAddress string    `json:"contractAddress"`
	TokenSymbol     string    `json:"tokenSymbol,omitempty"`
	From            string    `json:"from"`
	To              string    `json:"to"`
	AmountRaw       string    `json:"amountRaw"`
	TxHash          string    `json:"txHash"`
	LogIndex        int64     `json:"logIndex"`
	BlockNumber     int64     `json:"blockNumber"`
	BlockHash       string    `json:"blockHash,omitempty"`
	ObservedAt      time.Time `json:"observedAt"`
}

const TransferDetectedEventType = "token.transfer.detected"

func (m *TransferDetectedMessage) Normalize() {
	m.EventID = strings.TrimSpace(m.EventID)
	m.EventType = strings.TrimSpace(m.EventType)
	m.ContractAddress = strings.ToLower(strings.TrimSpace(m.ContractAddress))
	m.TokenSymbol = strings.TrimSpace(m.TokenSymbol)
	m.From = strings.ToLower(strings.TrimSpace(m.From))
	m.To = strings.ToLower(strings.TrimSpace(m.To))
	m.AmountRaw = strings.TrimSpace(m.AmountRaw)
	m.TxHash = strings.ToLower(strings.TrimSpace(m.TxHash))
	m.BlockHash = strings.ToLower(strings.TrimSpace(m.BlockHash))
}

func (m TransferDetectedMessage) Validate() error {
	if strings.TrimSpace(m.EventID) == "" {
		return fmt.Errorf("eventId is required")
	}
	if strings.TrimSpace(m.EventType) == "" {
		return fmt.Errorf("eventType is required")
	}
	if m.EventType != TransferDetectedEventType {
		return fmt.Errorf("unsupported eventType: %s", m.EventType)
	}
	if m.ChainID <= 0 {
		return fmt.Errorf("chainId must be > 0")
	}
	if strings.TrimSpace(m.ContractAddress) == "" {
		return fmt.Errorf("contractAddress is required")
	}
	if strings.TrimSpace(m.From) == "" {
		return fmt.Errorf("from is required")
	}
	if strings.TrimSpace(m.To) == "" {
		return fmt.Errorf("to is required")
	}
	if strings.TrimSpace(m.AmountRaw) == "" {
		return fmt.Errorf("amountRaw is required")
	}
	if strings.TrimSpace(m.TxHash) == "" {
		return fmt.Errorf("txHash is required")
	}
	if m.LogIndex < 0 {
		return fmt.Errorf("logIndex must be >= 0")
	}
	if m.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be > 0")
	}
	if m.ObservedAt.IsZero() {
		return fmt.Errorf("observedAt is required")
	}
	return nil
}

func (m TransferDetectedMessage) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

func ParseTransferDetectedMessage(data []byte) (TransferDetectedMessage, error) {
	var msg TransferDetectedMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return TransferDetectedMessage{}, fmt.Errorf("unmarshal transfer message: %w", err)
	}
	msg.Normalize()
	if err := msg.Validate(); err != nil {
		return TransferDetectedMessage{}, err
	}
	return msg, nil
}

func NewSampleTransferMessage() TransferDetectedMessage {
	now := time.Now().UTC()
	txHash := "0x1111111111111111111111111111111111111111111111111111111111111111"
	logIndex := int64(0)

	return TransferDetectedMessage{
		EventID:         fmt.Sprintf("11155111:%s:%d", txHash, logIndex),
		EventType:       TransferDetectedEventType,
		ChainID:         11155111,
		ContractAddress: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TokenSymbol:     "TST",
		From:            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		To:              "0xcccccccccccccccccccccccccccccccccccccccc",
		AmountRaw:       "1000000000000000000",
		TxHash:          txHash,
		LogIndex:        logIndex,
		BlockNumber:     123456,
		BlockHash:       "0x2222222222222222222222222222222222222222222222222222222222222222",
		ObservedAt:      now,
	}
}
