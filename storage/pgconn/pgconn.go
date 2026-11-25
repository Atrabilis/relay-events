// Package pgconn opens a PostgreSQL pool from a Config whose credentials come from environment variables.
package pgconn

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config is the connection (by environment variable name, never the values themselves) and target table.
type Config struct {
	HostEnv     string `yaml:"host_env"`
	PortEnv     string `yaml:"port_env"`
	UserEnv     string `yaml:"user_env"`
	PasswordEnv string `yaml:"password_env"`
	DatabaseEnv string `yaml:"database_env"`
	Schema      string `yaml:"schema"`
	Table       string `yaml:"table"`
}

var ident = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// Connect resolves the environment variables named in cfg, checks the table name and pings the database. It
// returns the pool and the schema-qualified table name. It does not create tables.
func Connect(ctx context.Context, name string, cfg Config) (*pgxpool.Pool, string, error) {
	env := func(key string) (string, error) {
		v := strings.TrimSpace(os.Getenv(key))
		if v == "" {
			return "", fmt.Errorf("output %q: environment variable %s is empty", name, key)
		}
		return v, nil
	}
	host, err := env(cfg.HostEnv)
	if err != nil {
		return nil, "", err
	}
	port, err := env(cfg.PortEnv)
	if err != nil {
		return nil, "", err
	}
	user, err := env(cfg.UserEnv)
	if err != nil {
		return nil, "", err
	}
	database, err := env(cfg.DatabaseEnv)
	if err != nil {
		return nil, "", err
	}
	password := os.Getenv(cfg.PasswordEnv)
	if password == "" {
		return nil, "", fmt.Errorf("output %q: environment variable %s is empty", name, cfg.PasswordEnv)
	}
	schema, table := strings.ToLower(cfg.Schema), strings.ToLower(cfg.Table)
	if !ident.MatchString(schema) || !ident.MatchString(table) {
		return nil, "", fmt.Errorf("output %q: schema and table must be plain lower-case identifiers", name)
	}

	conn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		url.QueryEscape(user), url.QueryEscape(password), host, port, url.QueryEscape(database))
	pool, err := pgxpool.New(ctx, conn)
	if err != nil {
		return nil, "", fmt.Errorf("output %q: %w", name, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, "", fmt.Errorf("output %q: database not reachable: %w", name, err)
	}
	return pool, schema + "." + table, nil
}
