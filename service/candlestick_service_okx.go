package service

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

type Candlestick struct {
	Timestamp string
	Open      string
	High      string
	Low       string
	Close     string
	Volume    string
}

type Response struct {
	Code string     `json:"code"`
	Msg  string     `json:"msg"`
	Data [][]string `json:"data"`
}

type CandlestickService struct {
	BaseURL string
	Timeout time.Duration
}

func NewCandlestickService(baseURL string, timeout time.Duration) *CandlestickService {
	return &CandlestickService{
		BaseURL: baseURL,
		Timeout: timeout,
	}
}

// FetchCandlesticks retrieves candlestick data from the API using the `after` parameter.
func (s *CandlestickService) FetchCandlesticks(instID, bar string, limit int, after string) ([]Candlestick, string, error) {
	// Prepare query parameters
	params := url.Values{}
	params.Add("instId", instID)
	params.Add("bar", bar)
	params.Add("limit", fmt.Sprintf("%d", limit))
	if after != "" {
		params.Add("after", after)
	}

	// Construct request URL
	reqURL := fmt.Sprintf("%s?%s", s.BaseURL, params.Encode())

	// Create HTTP client with a timeout
	client := &http.Client{Timeout: s.Timeout}

	// Perform GET request
	resp, err := client.Get(reqURL)
	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch data: %v", err)
	}
	defer resp.Body.Close()

	// Check HTTP response status
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("unexpected status: %s", resp.Status)
	}

	// Parse the API response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read response: %v", err)
	}

	var apiResponse Response
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return nil, "", fmt.Errorf("failed to parse JSON: %v", err)
	}

	// Check API error code
	if apiResponse.Code != "0" {
		return nil, "", fmt.Errorf("API error: %s", apiResponse.Msg)
	}

	// Convert the API data into structured candlestick objects
	var candlesticks []Candlestick
	for _, entry := range apiResponse.Data {
		candlesticks = append(candlesticks, Candlestick{
			Timestamp: entry[0],
			Open:      entry[1],
			High:      entry[2],
			Low:       entry[3],
			Close:     entry[4],
			Volume:    entry[5],
		})
	}

	// Return the data and the oldest timestamp for the next `before` parameter
	oldestTimestamp := ""
	if len(candlesticks) > 0 {
		oldestTimestamp = candlesticks[len(candlesticks)-1].Timestamp
	}

	return candlesticks, oldestTimestamp, nil
}
