package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	pgrepo "token-transfer-monitor/internal/repository/postgres"
)

type Handler struct {
	queryRepo *pgrepo.QueryRepo
	startedAt time.Time
}

func NewHandler(queryRepo *pgrepo.QueryRepo) *Handler {
	return &Handler{
		queryRepo: queryRepo,
		startedAt: time.Now().UTC(),
	}
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"service":   "token-transfer-monitor-api",
		"startedAt": h.startedAt,
		"now":       time.Now().UTC(),
	})
}

func (h *Handler) ListTransfers(w http.ResponseWriter, r *http.Request) {
	limit := getIntQuery(r, "limit", 50)
	offset := getIntQuery(r, "offset", 0)

	items, err := h.queryRepo.ListTransfers(r.Context(), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data":   items,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *Handler) GetTransferByEventID(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(chi.URLParam(r, "eventID"))
	if eventID == "" {
		writeError(w, http.StatusBadRequest, "eventID is required")
		return
	}

	item, err := h.queryRepo.GetTransferByEventID(r.Context(), eventID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if item == nil {
		writeError(w, http.StatusNotFound, "transfer not found")
		return
	}

	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) ListTransfersByWallet(w http.ResponseWriter, r *http.Request) {
	address := strings.TrimSpace(chi.URLParam(r, "address"))
	if address == "" {
		writeError(w, http.StatusBadRequest, "wallet address is required")
		return
	}

	limit := getIntQuery(r, "limit", 50)
	offset := getIntQuery(r, "offset", 0)

	items, err := h.queryRepo.ListTransfersByWallet(r.Context(), address, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data":   items,
		"wallet": strings.ToLower(address),
		"limit":  limit,
		"offset": offset,
	})
}

func (h *Handler) ListTokens(w http.ResponseWriter, r *http.Request) {
	items, err := h.queryRepo.ListTrackedTokens(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": items,
	})
}

func (h *Handler) ListWatchlist(w http.ResponseWriter, r *http.Request) {
	items, err := h.queryRepo.ListWalletWatchlist(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data": items,
	})
}

func getIntQuery(r *http.Request, key string, fallback int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}

	if value < 0 {
		return fallback
	}

	if key == "limit" {
		if value == 0 {
			return fallback
		}
		if value > 200 {
			return 200
		}
	}

	return value
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"error": message,
	})
}
