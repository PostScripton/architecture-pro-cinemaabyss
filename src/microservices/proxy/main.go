package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type config struct {
	port                   string
	monolithURL            string
	moviesServiceURL       string
	eventsServiceURL       string
	gradualMigration       bool
	moviesMigrationPercent int
}

func loadConfig() config {
	gradual := strings.ToLower(os.Getenv("GRADUAL_MIGRATION")) == "true"
	percent, _ := strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT"))

	return config{
		port:                   getEnv("PORT", "8000"),
		monolithURL:            getEnv("MONOLITH_URL", "http://localhost:8080"),
		moviesServiceURL:       getEnv("MOVIES_SERVICE_URL", "http://localhost:8081"),
		eventsServiceURL:       getEnv("EVENTS_SERVICE_URL", "http://localhost:8082"),
		gradualMigration:       gradual,
		moviesMigrationPercent: percent,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func newReverseProxy(target string) *httputil.ReverseProxy {
	u, err := url.Parse(target)
	if err != nil {
		log.Fatalf("invalid URL %s: %v", target, err)
	}
	return httputil.NewSingleHostReverseProxy(u)
}

func main() {
	cfg := loadConfig()

	monolithProxy := newReverseProxy(cfg.monolithURL)
	moviesProxy := newReverseProxy(cfg.moviesServiceURL)

	routeMovies := func(w http.ResponseWriter, r *http.Request) {
		if cfg.gradualMigration && rand.Intn(100) <= cfg.moviesMigrationPercent {
			log.Printf("[proxy] %s %s -> movies-service (%d%%)", r.Method, r.URL.Path, cfg.moviesMigrationPercent)
			moviesProxy.ServeHTTP(w, r)
		} else {
			log.Printf("[proxy] %s %s -> monolith", r.Method, r.URL.Path)
			monolithProxy.ServeHTTP(w, r)
		}
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"status": true})
	})

	http.HandleFunc("/api/movies", routeMovies)
	http.HandleFunc("/api/movies/", routeMovies)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[proxy] %s %s -> monolith", r.Method, r.URL.Path)
		monolithProxy.ServeHTTP(w, r)
	})

	addr := ":" + cfg.port
	log.Printf("proxy-service listening on %s (gradual=%v, movies=%d%%)", addr, cfg.gradualMigration, cfg.moviesMigrationPercent)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}
