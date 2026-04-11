package grpc

import (
	"encoding/json"
	"sync"

	"google.golang.org/grpc/encoding"
)

const JSONCodecName = "json"

var registerJSONCodecOnce sync.Once

type jsonCodec struct{}

func RegisterJSONCodec() {
	registerJSONCodecOnce.Do(func() {
		encoding.RegisterCodec(jsonCodec{})
	})
}

func (jsonCodec) Name() string {
	return JSONCodecName
}

func (jsonCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
