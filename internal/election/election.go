package election

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	DefaultLeaderKey             = "/masters/leader"
	DefaultMasterLeaseTTLSeconds = 5
)

type LeaderRecord struct {
	MasterID          string    `json:"master_id"`
	HTTPAdvertiseAddr string    `json:"http_advertise_addr"`
	GRPCAdvertiseAddr string    `json:"grpc_advertise_addr"`
	ElectedAt         time.Time `json:"elected_at"`
}

func (r LeaderRecord) Validate() error {
	if strings.TrimSpace(r.MasterID) == "" {
		return fmt.Errorf("master id is required")
	}
	return nil
}

func EncodeLeaderRecord(record LeaderRecord) ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(record)
}

func DecodeLeaderRecord(raw []byte) (LeaderRecord, error) {
	var record LeaderRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return LeaderRecord{}, err
	}
	if err := record.Validate(); err != nil {
		return LeaderRecord{}, err
	}
	record.MasterID = strings.TrimSpace(record.MasterID)
	record.HTTPAdvertiseAddr = strings.TrimSpace(record.HTTPAdvertiseAddr)
	record.GRPCAdvertiseAddr = strings.TrimSpace(record.GRPCAdvertiseAddr)
	record.ElectedAt = record.ElectedAt.UTC()
	return record, nil
}

func NormalizeLeaderKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return DefaultLeaderKey
	}
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}
	return key
}
