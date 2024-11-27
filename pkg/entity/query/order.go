package query

import (
	"errors"
	"fmt"
	"github.com/goccy/go-json"
	"strings"
)

type Order struct {
	Col    string `json:"col"`
	Option string `json:"opt,omitempty"`
}

func parseOrder(data []byte) ([]*Order, error) {
	if data == nil {
		return nil, nil
	}
	var orders []*Order
	err := json.Unmarshal(data, &orders)
	if err != nil {
		return nil, err
	}
	for _, o1 := range orders {
		if o1.Option == "" {
			o1.Option = "ASC"
		} else {
			o1.Option = strings.ToUpper(o1.Option)
		}
		err = o1.Verify()
		if err != nil {
			return nil, err
		}
	}
	return orders, nil
}

func (o *Order) Verify() error {
	if o == nil {
		return errors.New("order is nil")
	}
	if o.Col == "" {
		return errors.New("order col is required")
	}
	if o.Option != "DESC" && o.Option != "ASC" {
		return errors.New("order option must be 'D' or 'asc'")
	}
	return nil
}

func (o *Order) String() string {
	return fmt.Sprintf("%s %s", o.Col, o.Option)
}
