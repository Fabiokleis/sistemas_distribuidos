package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, req)

		slog.Info("request", "method", req.Method, "path", req.URL.Path, "took", time.Since(start))
	})
}

func (gc *GatewayController) SSEHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	clientID := uuid.New().String()
	client := gc.Service.RegisterSSEClient(clientID)
	defer gc.Service.UnregisterSSEClient(clientID)

	fmt.Fprintf(w, "event: connected\ndata: {\"client_id\":\"%s\"}\n\n", clientID)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	ctx := req.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-client.ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.EventType, string(event.Data))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}
}

func (*GatewayController) Opa(w http.ResponseWriter, req *http.Request) {
	slog.Debug("[*] ms-gateway opa!")
	w.Write([]byte("opa!"))
}

func (gc *GatewayController) RegisterPromotion(w http.ResponseWriter, req *http.Request) {
	var promo Promotion

	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&promo); err != nil {
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err := gc.Validate.Struct(promo); err != nil {
		var validationErrors validator.ValidationErrors
		errors.As(err, &validationErrors)

		errorsResponse := make(map[string]string)
		for _, fieldErr := range validationErrors {
			errorsResponse[fieldErr.Field()] = fmt.Sprintf("failed validation on rule: %s", fieldErr.Tag())
		}

		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"errors": errorsResponse})
		return
	}

	if err := gc.Service.PublishPromotion(promo.Category, promo.Description, promo.StoreEmail); err != nil {
		http.Error(w, "failed to publish promotion", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	slog.Debug("[*] ms-gateway published promotion ", promo)
}

func (gc *GatewayController) HandleVote(w http.ResponseWriter, req *http.Request) {
	log.Println("[*] ms-gateway handle vote!")

	query := req.URL.Query()

	id := req.PathValue("id")
	intension, _ := strconv.Atoi(query.Get("intension"))

	params := struct {
		Id        string `validate:"required,uuid"`
		Intension int    `validate:"required,oneof=1 -1"`
	}{
		Id:        id,
		Intension: intension,
	}
	if err := gc.Validate.Struct(params); err != nil {
		var validationErrors validator.ValidationErrors
		errors.As(err, &validationErrors)

		errorsResponse := make(map[string]string)
		for _, fieldErr := range validationErrors {
			errorsResponse[fieldErr.Field()] = fmt.Sprintf("failed validation on rule: %s", fieldErr.Tag())
		}

		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"errors": errorsResponse})
		return
	}

	if err := gc.Service.HandleVote(params.Id, int32(params.Intension)); err != nil {
		http.Error(w, "failed to apply vote", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	slog.Debug("[*] ms-gateway apply vote on promotion ", params)
}

func (gc *GatewayController) SubscribeCategory(w http.ResponseWriter, req *http.Request) {
	slog.Debug("[*] ms-gateway subscribe category")

	query := req.URL.Query()

	params := struct {
		ClientID string `validate:"required,uuid"`
		Category string `validate:"required,min=3,max=50"`
	}{
		ClientID: query.Get("client_id"),
		Category: query.Get("category"),
	}

	if err := gc.Validate.Struct(params); err != nil {
		var validationErrors validator.ValidationErrors
		errors.As(err, &validationErrors)

		errorsResponse := make(map[string]string)
		for _, fieldErr := range validationErrors {
			errorsResponse[fieldErr.Field()] = fmt.Sprintf("failed validation on rule: %s", fieldErr.Tag())
		}

		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"errors": errorsResponse})
		return
	}

	if err := gc.Service.SubscribeConsumer(params.ClientID, params.Category); err != nil {
		http.Error(w, "failed to subscribe category", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	slog.Debug("[*] ms-gateway subscribed", "client_id", params.ClientID, "category", params.Category)
}

func (gc *GatewayController) UnsubscribeCategory(w http.ResponseWriter, req *http.Request) {
	slog.Debug("[*] ms-gateway unsubscribe category")

	query := req.URL.Query()

	params := struct {
		ClientID string `validate:"required,uuid"`
		Category string `validate:"required,min=3,max=50"`
	}{
		ClientID: query.Get("client_id"),
		Category: query.Get("category"),
	}

	if err := gc.Validate.Struct(params); err != nil {
		var validationErrors validator.ValidationErrors
		errors.As(err, &validationErrors)

		errorsResponse := make(map[string]string)
		for _, fieldErr := range validationErrors {
			errorsResponse[fieldErr.Field()] = fmt.Sprintf("failed validation on rule: %s", fieldErr.Tag())
		}

		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"errors": errorsResponse})
		return
	}

	if err := gc.Service.UnsubscribeConsumer(params.ClientID, params.Category); err != nil {
		http.Error(w, "failed to unsubscribe category", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	slog.Debug("[*] ms-gateway unsubscribed", "client_id", params.ClientID, "category", params.Category)
}

func (gc *GatewayController) GetPromotion(w http.ResponseWriter, req *http.Request) {
	slog.Debug("[*] ms-gateway get promotion!")

	promoId := req.PathValue("id")

	if promoId == "" {
		http.Error(w, "provide promotion id", http.StatusBadRequest)
		return
	}

	err := gc.Validate.Var(promoId, "required,uuid4")
	if err != nil {
		http.Error(w, "invalid promotion uuid", http.StatusBadRequest)
		return
	}

	promo := gc.Service.GetPromotion(promoId)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(promo); err != nil {
		http.Error(w, "internal Server Error", http.StatusInternalServerError)
		return
	}
}

func (gc *GatewayController) ListPromotions(w http.ResponseWriter, req *http.Request) {
	promos := gc.Service.ListPromotions()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(promos); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

func (r Router) Serve(gs GatewayService) {
	gc := GatewayController{
		Validate: validator.New(),
		Service:  gs,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /sse", gc.SSEHandler)
	mux.HandleFunc("GET /opa", gc.Opa)
	mux.HandleFunc("GET /promotions", gc.ListPromotions)
	mux.HandleFunc("GET /promotions/{id}", gc.GetPromotion)
	mux.HandleFunc("POST /promotions", gc.RegisterPromotion)
	mux.HandleFunc("POST /promotions/{id}/vote", gc.HandleVote)
	mux.HandleFunc("POST /promotions/subscribe", gc.SubscribeCategory)
	mux.HandleFunc("POST /promotions/unsubscribe", gc.UnsubscribeCategory)

	mux.Handle("/", http.FileServer(http.Dir("static/")))

	handler := loggingMiddleware(mux)

	slog.Info("ms-gateway listening", "addr", ":8123")

	err := http.ListenAndServe(":8123", handler)
	if err != nil {
		log.Fatalf("failed start listening: %v", err)
	}
}
