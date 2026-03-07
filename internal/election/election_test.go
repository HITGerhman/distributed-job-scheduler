package election

import (
	"testing"
	"time"
)

func TestEncodeDecodeLeaderRecord(t *testing.T) {
	t.Parallel()

	record := LeaderRecord{
		MasterID:          "master-1",
		HTTPAdvertiseAddr: "master-1:8080",
		GRPCAdvertiseAddr: "master-1:50052",
		ElectedAt:         time.Date(2026, 3, 7, 2, 0, 0, 0, time.FixedZone("UTC+8", 8*3600)),
	}

	raw, err := EncodeLeaderRecord(record)
	if err != nil {
		t.Fatalf("EncodeLeaderRecord() error = %v", err)
	}

	decoded, err := DecodeLeaderRecord(raw)
	if err != nil {
		t.Fatalf("DecodeLeaderRecord() error = %v", err)
	}

	if decoded.MasterID != record.MasterID {
		t.Fatalf("decoded.MasterID = %q, want %q", decoded.MasterID, record.MasterID)
	}
	if decoded.HTTPAdvertiseAddr != record.HTTPAdvertiseAddr {
		t.Fatalf("decoded.HTTPAdvertiseAddr = %q, want %q", decoded.HTTPAdvertiseAddr, record.HTTPAdvertiseAddr)
	}
	if decoded.GRPCAdvertiseAddr != record.GRPCAdvertiseAddr {
		t.Fatalf("decoded.GRPCAdvertiseAddr = %q, want %q", decoded.GRPCAdvertiseAddr, record.GRPCAdvertiseAddr)
	}
	if decoded.ElectedAt.Location() != time.UTC {
		t.Fatalf("decoded.ElectedAt location = %v, want UTC", decoded.ElectedAt.Location())
	}
}

func TestNormalizeLeaderKey(t *testing.T) {
	t.Parallel()

	if got := NormalizeLeaderKey("masters/leader"); got != "/masters/leader" {
		t.Fatalf("NormalizeLeaderKey() = %q, want %q", got, "/masters/leader")
	}
	if got := NormalizeLeaderKey(""); got != DefaultLeaderKey {
		t.Fatalf("NormalizeLeaderKey(empty) = %q, want %q", got, DefaultLeaderKey)
	}
}
