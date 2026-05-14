package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	pgrepo "token-transfer-monitor/internal/repository/postgres"
)

type Handler struct {
	queryRepo        *pgrepo.QueryRepo
	trackedTokenRepo *pgrepo.TrackedTokenRepo
	watchlistRepo    *pgrepo.WalletWatchlistRepo
	startedAt        time.Time
}

func NewHandler(
	queryRepo *pgrepo.QueryRepo,
	trackedTokenRepo *pgrepo.TrackedTokenRepo,
	watchlistRepo *pgrepo.WalletWatchlistRepo,
) *Handler {
	return &Handler{
		queryRepo:        queryRepo,
		trackedTokenRepo: trackedTokenRepo,
		watchlistRepo:    watchlistRepo,
		startedAt:        time.Now().UTC(),
	}
}

type createTrackedTokenRequest struct {
	ChainID         int64  `json:"chainId"`
	ContractAddress string `json:"contractAddress"`
	Symbol          string `json:"symbol"`
	Decimals        int    `json:"decimals"`
	IsActive        *bool  `json:"isActive,omitempty"`
}

type updateTrackedTokenRequest struct {
	Symbol   *string `json:"symbol,omitempty"`
	Decimals *int    `json:"decimals,omitempty"`
	IsActive *bool   `json:"isActive,omitempty"`
}

type createWalletWatchlistRequest struct {
	Address  string `json:"address"`
	Label    string `json:"label"`
	IsActive *bool  `json:"isActive,omitempty"`
}

type updateWalletWatchlistRequest struct {
	Label    *string `json:"label,omitempty"`
	IsActive *bool   `json:"isActive,omitempty"`
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

func (h *Handler) CreateToken(w http.ResponseWriter, r *http.Request) {
	var req createTrackedTokenRequest

	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	req.ContractAddress = strings.ToLower(strings.TrimSpace(req.ContractAddress))
	req.Symbol = strings.TrimSpace(req.Symbol)

	if req.ChainID <= 0 {
		writeError(w, http.StatusBadRequest, "chainId must be > 0")
		return
	}

	if !common.IsHexAddress(req.ContractAddress) {
		writeError(w, http.StatusBadRequest, "contractAddress must be a valid EVM address")
		return
	}

	if req.Decimals < 0 || req.Decimals > 255 {
		writeError(w, http.StatusBadRequest, "decimals must be between 0 and 255")
		return
	}

	isActive := true
	if req.IsActive != nil {
		isActive = *req.IsActive
	}

	item, err := h.trackedTokenRepo.Create(r.Context(), pgrepo.CreateTrackedTokenParams{
		ChainID:         req.ChainID,
		ContractAddress: req.ContractAddress,
		Symbol:          req.Symbol,
		Decimals:        req.Decimals,
		IsActive:        isActive,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, item)
}

func (h *Handler) UpdateToken(w http.ResponseWriter, r *http.Request) {
	id, ok := parseIDParam(w, r, "id")
	if !ok {
		return
	}

	var req updateTrackedTokenRequest

	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.Symbol == nil && req.Decimals == nil && req.IsActive == nil {
		writeError(w, http.StatusBadRequest, "at least one field is required")
		return
	}

	if req.Decimals != nil && (*req.Decimals < 0 || *req.Decimals > 255) {
		writeError(w, http.StatusBadRequest, "decimals must be between 0 and 255")
		return
	}

	if req.Symbol != nil {
		normalized := strings.TrimSpace(*req.Symbol)
		req.Symbol = &normalized
	}

	item, err := h.trackedTokenRepo.Update(r.Context(), id, pgrepo.UpdateTrackedTokenParams{
		Symbol:   req.Symbol,
		Decimals: req.Decimals,
		IsActive: req.IsActive,
	})
	if err != nil {
		if isNoRows(err) {
			writeError(w, http.StatusNotFound, "tracked token not found")
			return
		}

		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) DeleteToken(w http.ResponseWriter, r *http.Request) {
	id, ok := parseIDParam(w, r, "id")
	if !ok {
		return
	}

	item, err := h.trackedTokenRepo.Deactivate(r.Context(), id)
	if err != nil {
		if isNoRows(err) {
			writeError(w, http.StatusNotFound, "tracked token not found")
			return
		}

		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, item)
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

func (h *Handler) CreateWatchlist(w http.ResponseWriter, r *http.Request) {
	var req createWalletWatchlistRequest

	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	req.Address = strings.ToLower(strings.TrimSpace(req.Address))
	req.Label = strings.TrimSpace(req.Label)

	if !common.IsHexAddress(req.Address) {
		writeError(w, http.StatusBadRequest, "address must be a valid EVM address")
		return
	}

	isActive := true
	if req.IsActive != nil {
		isActive = *req.IsActive
	}

	item, err := h.watchlistRepo.Create(r.Context(), pgrepo.CreateWalletWatchlistParams{
		Address:  req.Address,
		Label:    req.Label,
		IsActive: isActive,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, item)
}

func (h *Handler) UpdateWatchlist(w http.ResponseWriter, r *http.Request) {
	id, ok := parseIDParam(w, r, "id")
	if !ok {
		return
	}

	var req updateWalletWatchlistRequest

	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.Label == nil && req.IsActive == nil {
		writeError(w, http.StatusBadRequest, "at least one field is required")
		return
	}

	if req.Label != nil {
		normalized := strings.TrimSpace(*req.Label)
		req.Label = &normalized
	}

	item, err := h.watchlistRepo.Update(r.Context(), id, pgrepo.UpdateWalletWatchlistParams{
		Label:    req.Label,
		IsActive: req.IsActive,
	})
	if err != nil {
		if isNoRows(err) {
			writeError(w, http.StatusNotFound, "watchlist item not found")
			return
		}

		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) DeleteWatchlist(w http.ResponseWriter, r *http.Request) {
	id, ok := parseIDParam(w, r, "id")
	if !ok {
		return
	}

	item, err := h.watchlistRepo.Deactivate(r.Context(), id)
	if err != nil {
		if isNoRows(err) {
			writeError(w, http.StatusNotFound, "watchlist item not found")
			return
		}

		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, item)
}

func parseIDParam(w http.ResponseWriter, r *http.Request, key string) (int64, bool) {
	raw := strings.TrimSpace(chi.URLParam(r, key))
	if raw == "" {
		writeError(w, http.StatusBadRequest, key+" is required")
		return 0, false
	}

	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, key+" must be a positive integer")
		return 0, false
	}

	return id, true
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

func readJSON(r *http.Request, dst any) error {
	defer r.Body.Close()

	decoder := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(dst); err != nil {
		return err
	}

	return nil
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

func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}
