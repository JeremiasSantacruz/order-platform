package domain

import "errors"

var (
	ErrProductNotFound  = errors.New("product not found for the specified market")
	ErrInvalidProductID = errors.New("product id is required")
	ErrInvalidMarket    = errors.New("market parameter is required")
)
