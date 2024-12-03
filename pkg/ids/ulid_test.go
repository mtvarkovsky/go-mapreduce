package ids

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestULIDIDGenerator_GetID_MapTask(t *testing.T) {
	gen, err := NewULIDIDGenerator()
	assert.NoError(t, err)
	id, err := gen.GetID(context.Background(), MapTask)
	assert.NoError(t, err)
	assert.Equal(t, "MapTask", id[:7])
	assert.Len(t, id, 33)
}

func TestULIDIDGenerator_GetID_ReduceTask(t *testing.T) {
	gen, err := NewULIDIDGenerator()
	assert.NoError(t, err)
	id, err := gen.GetID(context.Background(), ReduceTask)
	assert.NoError(t, err)
	assert.Equal(t, "ReduceTask", id[:10])
	assert.Len(t, id, 36)
}
