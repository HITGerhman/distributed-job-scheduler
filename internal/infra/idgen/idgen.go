package idgen

import "sync/atomic"

type Generator struct {
	value uint64
}

func NewGenerator(initial uint64) *Generator {
	return &Generator{value: initial}
}

func (g *Generator) Next() uint64 {
	return atomic.AddUint64(&g.value, 1)
}
