package ids

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/oklog/ulid/v2"
)

type (
	ULIDIDGenerator struct {
		entropy *rand.Rand
	}
)

func NewULIDIDGenerator() (*ULIDIDGenerator, error) {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &ULIDIDGenerator{
		entropy: entropy,
	}, nil
}

func (ig *ULIDIDGenerator) GetID(ctx context.Context, entityType EntityType) (string, error) {
	id, err := ulid.New(ulid.Timestamp(time.Now()), ig.entropy)
	if err != nil {
		return "", fmt.Errorf("can't generate id: %w", err)
	}
	return entityType.String() + id.String(), nil
}
