package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func NewRouter(handler *Handler) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/health", handler.Health)

	r.Get("/transfers", handler.ListTransfers)
	r.Get("/transfers/{eventID}", handler.GetTransferByEventID)
	r.Get("/wallets/{address}/transfers", handler.ListTransfersByWallet)

	r.Get("/tokens", handler.ListTokens)
	r.Get("/watchlist", handler.ListWatchlist)

	return r
}
