package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-playground/validator/v10"
)

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, req)

		log.Printf("[*] ms-gateway %s %s | took: %v", req.Method, req.URL.Path, time.Since(start))
	})
}

func (*GatewayController) Opa(w http.ResponseWriter, req *http.Request) {
	log.Println("[*] ms-gateway opa!")
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

	if err := gc.Validate.Struct(promo); err != nil {
		var validationErrors validator.ValidationErrors
		errors.As(err, &validationErrors)

		errorsResponse := make(map[string]string)
		for _, fieldErr := range validationErrors {
			errorsResponse[fieldErr.Field()] = fmt.Sprintf("failed validation on rule: %s", fieldErr.Tag())
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"errors": errorsResponse})
		return
	}

	gc.Service.PublishPromotion(promo.Category, promo.Description)

	log.Println("[*] ms-gateway published promotion ", promo)
}

func (gc *GatewayController) GetPromotion(w http.ResponseWriter, req *http.Request) {
	log.Println("[*] ms-gateway get promotion !")

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
	mux.HandleFunc("GET /opa", gc.Opa)
	mux.HandleFunc("GET /promotions", gc.ListPromotions)
	mux.HandleFunc("GET /promotions/{id}", gc.GetPromotion)
	mux.HandleFunc("POST /promotions", gc.RegisterPromotion)
	mux.Handle("/", http.FileServer(http.Dir("static/")))

	handler := loggingMiddleware(mux)

	log.Printf("[*] ms-gateway listening on %v ...", ":8123")

	err := http.ListenAndServe(":8123", handler)
	if err != nil {
		log.Fatalf("failed start listening: %v", err)
	}
}
