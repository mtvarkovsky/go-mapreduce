package ids

import (
	"context"
	"fmt"
	"github.com/oklog/ulid/v2"
	"math/rand"
	"time"
)

type (
	ulidIDGenerator struct {
		entropy *rand.Rand
	}
)

func NewULIDIDGenerator() (IDGenerator, error) {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &ulidIDGenerator{
		entropy: entropy,
	}, nil
}

func (ig *ulidIDGenerator) GetID(ctx context.Context, entityType EntityType) (string, error) {
	id, err := ulid.New(ulid.Timestamp(time.Now()), ig.entropy)
	if err != nil {
		return "", fmt.Errorf("can't generate id: %w", err)
	}
	return entityType.String() + id.String(), nil
}
