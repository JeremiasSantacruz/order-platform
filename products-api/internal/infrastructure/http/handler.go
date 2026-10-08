package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"products-api/internal/application"
	"products-api/internal/domain"
)

type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type ProductHandler struct {
	useCase *application.GetProductUseCase
}

func NewProductHandler(useCase *application.GetProductUseCase) *ProductHandler {
	return &ProductHandler{useCase: useCase}
}

func (h *ProductHandler) GetProduct(w http.ResponseWriter, r *http.Request) {
	productID := r.PathValue("productId")
	market := r.URL.Query().Get("market")

	product, err := h.useCase.Execute(r.Context(), productID, market)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrInvalidProductID), errors.Is(err, domain.ErrInvalidMarket):
			writeJSONError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, domain.ErrProductNotFound):
			writeJSONError(w, http.StatusNotFound, err.Error())
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			writeJSONError(w, http.StatusGatewayTimeout, "client timeout or request canceled")
		default:
			writeJSONError(w, http.StatusInternalServerError, "internal server error")
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(product)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponse{
		Code:    status,
		Message: message,
	})
}
