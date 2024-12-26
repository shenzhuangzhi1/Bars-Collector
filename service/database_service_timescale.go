package service

import (
	"database/sql"
	"fmt"
	"strconv"

	"Bars-Collector/utils"
	_ "github.com/lib/pq" // PostgreSQL driver
)

type DatabaseService struct {
	DB *sql.DB
}

// InitializeDatabase ensures the candlesticks table exists and is set up correctly as a hypertable.
func (s *DatabaseService) InitializeDatabase(instID string) error {
	queryCreateTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS candlesticks_%s
		(
			time TIMESTAMPTZ    NOT NULL PRIMARY KEY,
			open	  DOUBLE    PRECISION NOT NULL,
			high	  DOUBLE    PRECISION NOT NULL,
			low	      DOUBLE       PRECISION NOT NULL,
			close  	 DOUBLE     PRECISION NOT NULL,
			volume	DOUBLE      PRECISION NOT NULL
		);
	`, utils.ReplaceHyphenWithUnderscore(instID))

	queryCreateHypertable := fmt.Sprintf(`
		SELECT create_hypertable('candlesticks_%s', by_range('time'), if_not_exists => TRUE);
	`, utils.ReplaceHyphenWithUnderscore(instID))

	// Execute create table query
	_, err := s.DB.Exec(queryCreateTable)
	if err != nil {
		return fmt.Errorf("failed to create candlesticks table: %v", err)
	}

	// Execute create hypertable query
	_, err = s.DB.Exec(queryCreateHypertable)
	if err != nil {
		return fmt.Errorf("failed to create hypertable: %v", err)
	}

	return nil
}

// NewDatabaseService initializes the database connection.
func NewDatabaseService(connStr string) (*DatabaseService, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the database: %v", err)
	}
	return &DatabaseService{
		DB: db,
	}, nil
}

// StoreCandlesticks stores a batch of candlestick data into the database.
func (s *DatabaseService) StoreCandlesticks(instID string, candlesticks []Candlestick) error {
	query := fmt.Sprintf(`
		INSERT INTO candlesticks_%s (time, open, high, low, close, volume)
		VALUES (to_timestamp($1::bigint / 1000), $2, $3, $4, $5, $6)
		ON CONFLICT (time) DO NOTHING
	`, utils.ReplaceHyphenWithUnderscore(instID))

	// Prepare for batch insert
	tx, err := s.DB.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Insert each candlestick
	for _, candle := range candlesticks {
		_, err = stmt.Exec(
			candle.Timestamp,
			parseAsFloat(candle.Open),
			parseAsFloat(candle.High),
			parseAsFloat(candle.Low),
			parseAsFloat(candle.Close),
			parseAsFloat(candle.Volume),
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute insert: %v", err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// parseAsFloat is a utility function that converts a string to a float64
func parseAsFloat(value string) float64 {
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0.0
	}
	return f
}
