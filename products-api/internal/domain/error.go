package domain

import "errors"

var (
	ErrProductNotFound  = errors.New("product not found for the specified market")
	ErrInvalidProductID = errors.New("product id is required")
	ErrInvalidMarket    = errors.New("market must be one of MX, CO, PE")
)
