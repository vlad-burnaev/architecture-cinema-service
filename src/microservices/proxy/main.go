package main

import (
	"bytes"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port                   string
	MonolithURL            string
	MoviesServiceURL       string
	EventsServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
}

var config Config

func main() {
	// Initialize random seed for traffic distribution
	rand.Seed(time.Now().UnixNano())

	// Load configuration from environment variables
	loadConfig()

	// Setup HTTP routes
	http.HandleFunc("/", proxyHandler)

	// Start server
	log.Printf("Starting Proxy Service (API Gateway) on port %s", config.Port)
	log.Printf("Gradual Migration: %v", config.GradualMigration)
	log.Printf("Movies Migration Percent: %d%%", config.MoviesMigrationPercent)
	log.Printf("Monolith URL: %s", config.MonolithURL)
	log.Printf("Movies Service URL: %s", config.MoviesServiceURL)
	log.Printf("Events Service URL: %s", config.EventsServiceURL)

	log.Fatal(http.ListenAndServe(":"+config.Port, nil))
}

func loadConfig() {
	config.Port = getEnv("PORT", "8000")
	config.MonolithURL = getEnv("MONOLITH_URL", "http://monolith:8080")
	config.MoviesServiceURL = getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081")
	config.EventsServiceURL = getEnv("EVENTS_SERVICE_URL", "http://events-service:8082")

	gradualMigration := getEnv("GRADUAL_MIGRATION", "true")
	config.GradualMigration = gradualMigration == "true"

	migrationPercentStr := getEnv("MOVIES_MIGRATION_PERCENT", "50")
	migrationPercent, err := strconv.Atoi(migrationPercentStr)
	if err != nil {
		log.Printf("Invalid MOVIES_MIGRATION_PERCENT value, using default 50: %v", err)
		migrationPercent = 50
	}
	config.MoviesMigrationPercent = migrationPercent
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	// Log incoming request
	log.Printf("[%s] %s %s", r.Method, r.URL.Path, r.RemoteAddr)

	// Determine target service based on path and migration strategy
	targetURL := determineTargetService(r.URL.Path)

	// Forward request to target service
	proxyRequest(w, r, targetURL)
}

func determineTargetService(path string) string {
	// Route /api/movies requests based on Strangler Fig pattern
	if strings.HasPrefix(path, "/api/movies") {
		if config.GradualMigration {
			// Probabilistic routing based on migration percentage
			if shouldRouteToNewService() {
				log.Printf("Routing to Movies Service (new): %s", path)
				return config.MoviesServiceURL
			}
			log.Printf("Routing to Monolith (legacy): %s", path)
			return config.MonolithURL
		}
		// If migration is disabled, route to monolith
		log.Printf("Migration disabled, routing to Monolith: %s", path)
		return config.MonolithURL
	}

	// Route /api/events requests to Events Service
	if strings.HasPrefix(path, "/api/events") {
		log.Printf("Routing to Events Service: %s", path)
		return config.EventsServiceURL
	}

	// All other requests go to Monolith (users, payments, subscriptions, health)
	log.Printf("Routing to Monolith: %s", path)
	return config.MonolithURL
}

func shouldRouteToNewService() bool {
	// Generate random number between 0-99
	randomValue := rand.Intn(100)
	// Route to new service if random value is less than migration percentage
	return randomValue < config.MoviesMigrationPercent
}

func proxyRequest(w http.ResponseWriter, r *http.Request, targetURL string) {
	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, "Error reading request", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	// Create new request to target service
	targetReq, err := http.NewRequest(r.Method, targetURL+r.URL.Path+"?"+r.URL.RawQuery, bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Error creating target request: %v", err)
		http.Error(w, "Error creating target request", http.StatusInternalServerError)
		return
	}

	// Copy headers from original request
	for name, values := range r.Header {
		for _, value := range values {
			targetReq.Header.Add(name, value)
		}
	}

	// Add proxy headers
	targetReq.Header.Set("X-Forwarded-For", r.RemoteAddr)
	targetReq.Header.Set("X-Forwarded-Proto", "http")
	targetReq.Header.Set("X-Proxy-By", "CinemaAbyss-Proxy")

	// Send request to target service
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	resp, err := client.Do(targetReq)
	if err != nil {
		log.Printf("Error forwarding request to %s: %v", targetURL, err)
		http.Error(w, "Error forwarding request", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body: %v", err)
		http.Error(w, "Error reading response", http.StatusInternalServerError)
		return
	}

	// Copy response headers
	for name, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}

	// Add custom header to indicate which service handled the request
	if strings.Contains(targetURL, "movies-service") {
		w.Header().Set("X-Served-By", "movies-service")
	} else if strings.Contains(targetURL, "monolith") {
		w.Header().Set("X-Served-By", "monolith")
	} else if strings.Contains(targetURL, "events-service") {
		w.Header().Set("X-Served-By", "events-service")
	}

	// Write response status and body
	w.WriteHeader(resp.StatusCode)
	w.Write(respBody)

	log.Printf("[%s] %s -> %d (served by: %s)", r.Method, r.URL.Path, resp.StatusCode, w.Header().Get("X-Served-By"))
}
