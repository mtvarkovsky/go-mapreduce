package repository

import (
	"fmt"
	"strings"
	"time"

	"github.com/mtvarkovsky/go-mapreduce/pkg/mapreduce"
)

type (
	Filter struct {
		IDs            []string
		Statuses       []mapreduce.TaskStatus
		Limit          *uint32
		Offset         *uint32
		OrderBy        *OrderField
		OrderDirection *OrderDirection
		InProgressFor  *time.Duration
	}

	OrderField     int
	OrderDirection int

	UpdateFields struct {
		Status        *mapreduce.TaskStatus
		RescheduledAt *time.Time
	}
)

const (
	OrderByCreatedAt OrderField = iota
)

func (of OrderField) String() string {
	return [...]string{
		"OrderByCreatedAt",
	}[of]
}

const (
	Ascending OrderDirection = iota
	Descending
)

func (od OrderDirection) String() string {
	return [...]string{
		"Ascending",
		"Descending",
	}[od]
}

func (f Filter) String() string {
	str := strings.Builder{}
	str.WriteString(fmt.Sprintf("Filter{IDs=%s, Statuses=%s", f.IDs, f.Statuses))
	if f.Offset != nil {
		str.WriteString(fmt.Sprintf(", Offset=%d", *f.Offset))
	}
	if f.Limit != nil {
		str.WriteString(fmt.Sprintf(", Limit=%d", *f.Limit))
	}
	if f.OrderBy != nil {
		str.WriteString(fmt.Sprintf(", OrderBy=%s", *f.OrderBy))
	}
	if f.OrderDirection != nil {
		str.WriteString(fmt.Sprintf(", OrderDirection=%s", *f.OrderDirection))
	} else {
		str.WriteString(fmt.Sprintf(", OrderDirection=%s", Ascending))
	}
	if f.InProgressFor != nil {
		str.WriteString(fmt.Sprintf(", InProgressFor=%s", *f.InProgressFor))
	}
	str.WriteString("}")
	return str.String()
}
