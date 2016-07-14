package kernel_test

import (
	"testing"

	"github.com/odeke-em/rosedatastore/kernel"
)

func TestKernel(t *testing.T) {
	k := kernel.New()

	// Insert in different goroutines
	testCases := [...]struct {
		id         uint64
		ttl1, ttl2 int64
	}{
		0: {id: ^uint64(0), ttl1: 1082, ttl2: 10},
		1: {id: 10, ttl1: 82, ttl2: -1},
	}

	for _, tt := range testCases {
		k.SetTTL(tt.id, tt.ttl1)
	}

	for i, tt := range testCases {
		for j := 0; j < 10; j++ {
			ttl, err := k.TTL(tt.id)
			if err != nil {
				t.Errorf("ttl1-#%d-round%d: gotErr=%v wantErr=nil", i, j+1, err)
			}
			if got, want := ttl, tt.ttl1; got != want {
				t.Errorf("ttl1-#%d-round%d: gotTTL=%v wantTTL=%v", i, j+1, got, want)
			}
		}
	}

	for _, tt := range testCases {
		k.SetTTL(tt.id, tt.ttl2)
	}

	for i, tt := range testCases {
		for j := 0; j < 10; j++ {
			ttl, err := k.TTL(tt.id)
			if err != nil {
				t.Errorf("ttl2-#%d-round%d: gotErr=%v wantErr=nil", i, j+1, err)
			}
			if got, want := ttl, tt.ttl2; got != want {
				t.Errorf("ttl2-#%d-round%d: gotTTL=%v wantTTL=%v", i, j+1, got, want)
			}
		}
	}
}
