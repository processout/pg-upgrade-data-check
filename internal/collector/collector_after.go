package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v4"
)

type AfterReplication struct {
	config    *Config
	beforeOut string
	afterOut  string
	conn      *pgx.Conn
}

func NewAfterReplication(config *Config, beforeOut, afterOut string, conn *pgx.Conn) *AfterReplication {
	return &AfterReplication{
		config:    config,
		beforeOut: beforeOut,
		afterOut:  afterOut,
		conn:      conn,
	}
}

func (c *AfterReplication) Run(ctx context.Context) error {
	log.Println("🏃 Starting post-upgrade stage...")
	if _, err := os.Stat(c.afterOut); err == nil {
		return fmt.Errorf("file already exists: %s", c.afterOut)
	}

	if _, err := os.Stat(c.beforeOut); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("file is missing: %s", c.beforeOut)
	}

	var cr map[string]collectResult
	preContent, err := os.ReadFile(c.beforeOut)
	if err != nil {
		return fmt.Errorf("error reading file %s: %w", c.beforeOut, err)
	}

	err = json.Unmarshal(preContent, &cr)
	if err != nil {
		return fmt.Errorf("error unmarshalling file %s: %w", c.beforeOut, err)
	}

	var result int
	for tname, data := range c.config.Tables {
		query := prepareCollectQuery(tname, data)
		log.Printf("⬇️ Fetching data for %s", tname)

		err = c.conn.QueryRow(ctx, query).Scan(&result)
		if err != nil {
			log.Printf("💥 No rows found for table: %s, please double check", tname)
			result = 0
		}

		item := cr[tname]
		item.StopID = result
		cr[tname] = item
	}

	jsonByte, err := json.MarshalIndent(cr, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshalling json: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(c.afterOut), os.ModePerm); err != nil {
		return err
	}

	return os.WriteFile(c.afterOut, jsonByte, 0600)
}
