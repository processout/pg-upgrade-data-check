package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v4"
)

type BeforeReplication struct {
	config    *Config
	beforeOut string
	conn      *pgx.Conn
}

func NewBeforeReplication(config *Config, beforeOut string, conn *pgx.Conn) *BeforeReplication {
	return &BeforeReplication{
		config:    config,
		beforeOut: beforeOut,
		conn:      conn,
	}
}

func (c *BeforeReplication) Run(ctx context.Context) error {
	log.Println("🏃 Starting pre-upgrade stage...")
	// Check if the collection file does not exist, error otherwise
	if _, err := os.Stat(c.beforeOut); err == nil {
		return fmt.Errorf("file already exists: %s", c.beforeOut)
	}

	cr := make(map[string]collectResult)

	var result int
	for tname, data := range c.config.Tables {
		query := prepareCollectQuery(tname, data)
		log.Printf("⬇️ Fetching data for %s", tname)

		err := c.conn.QueryRow(ctx, query).Scan(&result)
		if err != nil {
			log.Printf("💥 No rows found for table: %s, please double check", tname)
			result = 0
		}

		cr[tname] = collectResult{StartID: result}
	}

	jsonByte, err := json.MarshalIndent(cr, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshalling json: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(c.beforeOut), os.ModePerm); err != nil {
		return err
	}

	return os.WriteFile(c.beforeOut, jsonByte, 0600)
}
