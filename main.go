package main

import (
	"Bars-Collector/config"
	"context"
	"log"
	"os"
	"time"

	"Bars-Collector/service"
	"golang.org/x/time/rate"
)

func main() {
	// Initialize the Candlestick service
	baseURL := "https://www.okx.com/api/v5/market/history-candles"
	timeout := 10 * time.Second
	candlestickService := service.NewCandlestickService(baseURL, timeout)

	// Load configuration from the config.yaml file
	cfg, err := config.LoadConfig("db_config.yml")
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Generate PostgreSQL connection string
	connStr := config.GetPostgresConnectionString(&cfg.Database)

	dbService, err := service.NewDatabaseService(connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Validate command-line arguments
	if len(os.Args) < 2 || len(os.Args) > 3 {
		log.Fatalf("Usage: go run main.go <instID> <bar>")
	}
	instID := os.Args[1]
	bar := "1m" // Default interval
	if len(os.Args) == 3 {
		bar = os.Args[2]
	}
	limit := 300

	// Call InitializeDatabase with the  ID
	err = dbService.InitializeDatabase(instID)
	if err != nil {
		log.Fatalf("Failed to initialize database for instrument %s: %v", instID, err)
	}

	// Initialize rate limiter
	limiter := rate.NewLimiter(10, 1) // 10 requests per second

	// Start fetching and storing data
	err = fetchAndStoreCandlesticksRecursive(instID, bar, limit, candlestickService, dbService, "", limiter)
	if err != nil {
		log.Fatalf("Error during data collection: %v", err)
	}

	log.Println("All data fetching completed!")
}

// Recursive function to fetch and store candlesticks
func fetchAndStoreCandlesticksRecursive(instID, bar string, limit int, candlestickService *service.CandlestickService, dbService *service.DatabaseService, after string, limiter *rate.Limiter) error {
	// Enforce rate limit
	if err := limiter.Wait(context.Background()); err != nil {
		return err
	}

	// Fetch candlestick data
	candlesticks, nextAfter, err := candlestickService.FetchCandlesticks(instID, bar, limit, after)
	if err != nil {
		return err
	}

	// Stop recursion if no data is returned
	if len(candlesticks) == 0 {
		log.Println("No more data to fetch.")
		return nil
	}

	// Store candlestick data
	err = dbService.StoreCandlesticks(instID, candlesticks)
	if err != nil {
		return err
	}

	// Log progress and make the next recursive call
	log.Printf("Fetched and stored %d candlesticks. Fetching next batch with after=%s", len(candlesticks), nextAfter)
	return fetchAndStoreCandlesticksRecursive(instID, bar, limit, candlestickService, dbService, nextAfter, limiter)
}
