package main

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/jackc/pgx/v4"
	"github.com/processout/pg-upgrade-data-check/internal/collector"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
)

func before() *cli.Command {
	return &cli.Command{
		Name:  "before",
		Usage: "get start ids from source db",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "before-replication-output",
				Aliases: []string{"b"},
				Value:   "data/before_ids.json",
				Usage:   "output destination for before replication IDs",
			},
			&cli.StringFlag{
				Name:    "after-replication-output",
				Aliases: []string{"a"},
				Value:   "data/after_ids.json",
				Usage:   "stage 2 file",
			},
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Value:   "config.yaml",
				Usage:   "where the config is situated",
			},
			&cli.StringFlag{
				Name:  "source-url",
				Value: "postgres://postgres@localhost:6001/postgres",
				Usage: "source database url",
			},
		},
		Action: func(c *cli.Context) error {
			ctx := c.Context

			conf, err := getConfig(c.String("config"))
			if err != nil {
				return fmt.Errorf("unable to get config: %w", err)
			}

			connSource, err := pgx.Connect(ctx, c.String("source-url"))
			if err != nil {
				return fmt.Errorf("unable to connect to source database: %w", err)
			}
			defer connSource.Close(ctx)

			return collector.
				NewBeforeReplication(conf,
					c.String("before-replication-output"),
					connSource,
				).
				Run(ctx)
		},
	}
}

func after() *cli.Command {
	return &cli.Command{
		Name:  "after",
		Usage: "get end ids from source db",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "before-replication-output",
				Aliases: []string{"b"},
				Value:   "data/before_ids.json",
				Usage:   "output destination for before replication IDs",
			},
			&cli.StringFlag{
				Name:    "after-replication-output",
				Aliases: []string{"a"},
				Value:   "data/after_ids.json",
				Usage:   "output destination for after replication IDs",
			},
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Value:   "config.yaml",
				Usage:   "where the config is situated",
			},
			&cli.StringFlag{
				Name:  "source-url",
				Value: "postgres://postgres@localhost:6001/postgres",
				Usage: "source database url",
			},
		},
		Action: func(c *cli.Context) error {
			ctx := c.Context

			conf, err := getConfig(c.String("config"))
			if err != nil {
				return fmt.Errorf("unable to get config: %w", err)
			}

			connSource, err := pgx.Connect(ctx, c.String("source-url"))
			if err != nil {
				return fmt.Errorf("unable to connect to source database: %w", err)
			}
			defer connSource.Close(ctx)

			return collector.
				NewAfterReplication(conf,
					c.String("before-replication-output"),
					c.String("after-replication-output"),
					connSource,
				).
				Run(ctx)
		},
	}
}

func compare() *cli.Command {
	return &cli.Command{
		Name:  "compare",
		Usage: "compare rows between start and end ids in source and destination databases",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "after-replication-output",
				Aliases: []string{"a"},
				Value:   "data/after_ids.json",
				Usage:   "output destination for after replication IDs",
			},
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Value:   "config.yaml",
				Usage:   "where the config is situated",
			},
			&cli.StringFlag{
				Name:  "source-url",
				Value: "postgres://postgres@localhost:6000/postgres",
				Usage: "source database url",
			},
			&cli.StringFlag{
				Name:  "target-url",
				Value: "postgres://postgres@localhost:6001/postgres",
				Usage: "target database url",
			},
		},
		Action: func(c *cli.Context) error {
			ctx := c.Context

			conf, err := getConfig(c.String("config"))
			if err != nil {
				return fmt.Errorf("unable to get config: %w", err)
			}

			connSource, err := pgx.Connect(ctx, c.String("source-url"))
			if err != nil {
				return fmt.Errorf("unable to connect to source database: %w", err)
			}
			defer connSource.Close(ctx)

			connTarget, err := pgx.Connect(ctx, c.String("target-url"))
			if err != nil {
				return fmt.Errorf("unable to connect to target database: %w", err)
			}
			defer connTarget.Close(ctx)

			return collector.
				NewCompare(conf, c.String("after-replication-output"), connSource, connTarget).
				Run(ctx)
		},
	}
}

func main() {
	app := &cli.App{
		Commands: []*cli.Command{before(), after(), compare()},
	}

	if err := app.Run(os.Args); err != nil {
		color.Red("The application has failed")
		color.Red(err.Error())
		os.Exit(1)
	}
}

func getConfig(configPath string) (*collector.Config, error) {
	conf := &collector.Config{}
	yamlFile, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(yamlFile, conf); err != nil {
		return nil, err
	}

	return conf, nil
}
