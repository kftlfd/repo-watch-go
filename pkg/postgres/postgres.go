package postgres

import (
	"database/sql"

	_ "github.com/lib/pq"
)

func Connect(url string) (*sql.DB, error) {
	conn, err := sql.Open("postgres", url)
	return conn, err
}

func CreateDbUrl(baseDbUrl, dbName string) string {
	return baseDbUrl + "/" + dbName + "?sslmode=disable"
}
