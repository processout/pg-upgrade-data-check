package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"

	"github.com/jackc/pgx/v4"
)

type Compare struct {
	config     *Config
	afterFile  string
	sourceConn *pgx.Conn
	targetConn *pgx.Conn
}

func NewCompare(config *Config, afterFile string, sourceConn, targetConn *pgx.Conn) *Compare {
	return &Compare{
		config:     config,
		afterFile:  afterFile,
		sourceConn: sourceConn,
		targetConn: targetConn,
	}
}

//nolint:funlen // very complex logic to refactor
func (c *Compare) Run(ctx context.Context) error {
	log.Println("🏃 Comparing...")
	if _, err := os.Stat(c.afterFile); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("file is missing: %s", c.afterFile)
	}

	var cr map[string]collectResult
	stag2Content, err := os.ReadFile(c.afterFile)
	if err != nil {
		return fmt.Errorf("error reading file %s: %w", c.afterFile, err)
	}

	if err := json.Unmarshal(stag2Content, &cr); err != nil {
		return fmt.Errorf("error unmarshalling file %s: %w", c.afterFile, err)
	}

	var sid int
	var cid int
	var shash string
	var chash string
	var issues int
	var rows int

	for tname, data := range c.config.Tables {
		issues = 0
		rows = 0
		log.Printf("🔍 Comparing table %s", tname)

		if cr[tname].StartID == 0 && cr[tname].StopID == 0 {
			log.Printf("  ⏭️ Skipping %v as start == stop == 0, please double check this table", tname)
			continue
		}

		start := cr[tname].StartID
		nextStart := start
		var stop int

		for {
			// Make sure we compare not more than 1000 rows at once
			if cr[tname].StopID-nextStart > 1000 {
				start = nextStart
				nextStart = start + 1000
				stop = start + 1000
			} else {
				stop = cr[tname].StopID
			}

			query := prepareCompareQuery(tname, data, start, stop)
			srows, err := c.sourceConn.Query(ctx, query)
			if err != nil {
				return err
			}
			defer srows.Close()

			crows, err := c.targetConn.Query(ctx, query)
			if err != nil {
				return err
			}
			defer crows.Close()

			for srows.Next() {
				rows++
				crows.Next()
				err := srows.Scan(&sid, &shash)
				if err != nil {
					return err
				}
				err = crows.Scan(&cid, &chash)

				if err != nil {
					log.Printf("  💥 Comparison failed at id %d for table %s", sid, tname)
					issues++
					nextStart = int(math.Max(float64(sid), float64(cid))) + 1

					break
				}

				if cid != sid || shash != chash {
					log.Printf("  💥 Comparison failed at id %d for table %s based on hash", sid, tname)
					log.Printf("  💥 sid: %d (%s), cid: %d (%s) for table %s based on hash", sid, shash, cid, chash, tname)
					issues++
					nextStart = int(math.Max(float64(sid), float64(cid))) + 1

					break
				}
			}
			crows.Close()
			srows.Close()

			if stop == cr[tname].StopID {
				break
			}
		}

		if issues == 0 {
			log.Printf("  ✅️ No issues found for table %s after comparing %d rows", tname, rows)
			continue
		}

		log.Printf("  💥 %d issues found for table %s", issues, tname)
	}

	return nil
}
