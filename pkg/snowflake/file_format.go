package snowflake

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

type FileFormatType string

const (
	CSV     FileFormatType = "csv" // TODO: Add support for this
	Parquet FileFormatType = "parquet"
)

type Compression string

const (
	AUTO   Compression = "auto"
	LZO    Compression = "lzo"
	SNAPPY Compression = "snappy"
	NONE   Compression = "none"
)

// FileFormatBuilder abstracts the creation of SQL queries for a Snowflake file format
type FileFormatBuilder struct {
	name   string
	db     string
	schema string
}

// QualifiedName prepends the db and schema and escapes everything nicely
func (fb *FileFormatBuilder) QualifiedName() string {
	var n strings.Builder

	n.WriteString(fmt.Sprintf(`"%v"."%v"."%v"`, fb.db, fb.schema, fb.name))

	return n.String()
}

// FileFormat returns a pointer to a Builder that abstracts the DDL operations for a stage.
//
// Supported DDL operations are:
//   - DESCRIBE FILE FORMAT
//
// [Snowflake Reference](https://docs.snowflake.com/en/sql-reference/ddl-stage.html#file-format-management)
func FileFormat(name, db, schema string) *FileFormatBuilder {
	return &FileFormatBuilder{
		name:   name,
		db:     db,
		schema: schema,
	}
}

// Describe returns the SQL query that will describe a file format.
func (fb *FileFormatBuilder) Describe() string {
	return fmt.Sprintf(`DESCRIBE FILE FORMAT %v`, fb.QualifiedName())
}

// Show returns the SQL query that will show a file format.
func (fb *FileFormatBuilder) Show() string {
	return fmt.Sprintf(`SHOW STAGES LIKE '%v' IN DATABASE "%v"`, fb.name, fb.db)
}

type fileFormatMetadata struct {
	Name         *string `db:"name"`
	DatabaseName *string `db:"database_name"`
	SchemaName   *string `db:"schema_name"`
	Comment      *string `db:"comment"`
}

// ScanFileFormatShow scans the given SQL row and returns the parsed file format metadata.
func ScanFileFormatShow(row *sqlx.Row) (*fileFormatMetadata, error) {
	metadata := &fileFormatMetadata{}
	err := row.StructScan(metadata)
	return metadata, err
}

type fileFormatData struct {
	Type         FileFormatType
	Compression  Compression
	BinaryAsText bool
	TrimSpace    bool
	NullIf       []string
}

type descFileFormatRow struct {
	Property        string `db:"property"`
	PropertyType    string `db:"property_type"`
	PropertyValue   string `db:"property_value"`
	PropertyDefault string `db:"property_default"`
}

// DescFileFormat queries the file format with a describe and returns the file format data.
func DescFileFormat(db *sql.DB, query string) (*fileFormatData, error) {
	rows, err := Query(db, query)
	if err != nil {
		return &fileFormatData{}, err
	}
	defer rows.Close()

	result := &fileFormatData{}
	for rows.Next() {
		row := &descFileFormatRow{}
		if err := rows.StructScan(row); err != nil {
			return &fileFormatData{}, err
		}

		switch row.Property {
		case "TYPE":
			result.Type = FileFormatType(row.PropertyValue)
		case "COMPRESSION":
			result.Compression = Compression(row.PropertyValue)
		case "BINARY_AS_TEXT":
			v, err := strconv.ParseBool(row.PropertyValue)
			if err != nil {
				return &fileFormatData{}, err
			}
			result.BinaryAsText = v
		case "TRIM_SPACE":
			v, err := strconv.ParseBool(row.PropertyValue)
			if err != nil {
				return &fileFormatData{}, err
			}
			result.TrimSpace = v
		case "NULL_IF":
			strs := strings.Trim(row.PropertyValue, "[\"]")
			result.NullIf = strings.Split(strs, ",")
		}
	}

	return result, nil
}
