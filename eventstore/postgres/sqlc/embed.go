package sqlc

import (
	"embed"
)

const Path = "schema"

//go:embed schema/*.sql
var SchemaSQLs embed.FS
