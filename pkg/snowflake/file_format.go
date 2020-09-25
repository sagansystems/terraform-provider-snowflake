package snowflake

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

// FileFormatBuilder abstracts the creation of SQL queries for a Snowflake file format
type FileFormatBuilder struct {
	name           string
	db             string
	schema         string
	fileFormatType string
	comment        string
	compression    string
	binaryAsText   bool
	trimSpace      bool
	nullIf         []string
}

// QualifiedName prepends the db and schema and escapes everything nicely
func (fb *FileFormatBuilder) QualifiedName() string {
	var n strings.Builder

	n.WriteString(fmt.Sprintf(`"%v"."%v"."%v"`, fb.db, fb.schema, fb.name))

	return n.String()
}

// WithType adds a type to the FileFormatBuilder
func (fb *FileFormatBuilder) WithType(t string) *FileFormatBuilder {
	fb.fileFormatType = t
	return fb
}

// WithComment adds a comment to the FileFormatBuilder
func (fb *FileFormatBuilder) WithComment(comment string) *FileFormatBuilder {
	fb.comment = comment
	return fb
}

// WithCompression adds a compression to the FileFormatBuilder
func (fb *FileFormatBuilder) WithCompression(compression string) *FileFormatBuilder {
	fb.compression = compression
	return fb
}

// WithBinaryAsText adds binary as text to the FileFormatBuilder
func (fb *FileFormatBuilder) WithBinaryAsText(binaryAsText bool) *FileFormatBuilder {
	fb.binaryAsText = binaryAsText
	return fb
}

// WithTrimSpace adds trim space to the FileFormatBuilder
func (fb *FileFormatBuilder) WithTrimSpace(trimSpace bool) *FileFormatBuilder {
	fb.trimSpace = trimSpace
	return fb
}

// WithNullIf adds null if to the FileFormatBuilder
func (fb *FileFormatBuilder) WithNullIf(nullIf []string) *FileFormatBuilder {
	fb.nullIf = nullIf
	return fb
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

// Create returns the SQL query that will create a new file format.
func (fb *FileFormatBuilder) Create() string {
	builder := strings.Builder{}
	builder.WriteString(`CREATE FILE FORMAT `)
	builder.WriteString(fb.QualifiedName())

	if fb.fileFormatType != "" {
		builder.WriteString(fmt.Sprintf(` TYPE = "%v"`, fb.fileFormatType))
	}

	if fb.comment != "" {
		builder.WriteString(fmt.Sprintf(` COMMENT = "%v"`, EscapeString(fb.comment)))
	}

	if fb.compression != "" {
		builder.WriteString(fmt.Sprintf(` COMPRESSION = "%v"`, fb.compression))
	}

	builder.WriteString(fmt.Sprintf(` BINARY_AS_TEXT = %v`, fb.binaryAsText))

	builder.WriteString(fmt.Sprintf(` TRIM_SPACE = %v`, fb.trimSpace))

	if len(fb.nullIf) > 0 {
		nulls := make([]string, len(fb.nullIf))
		for i, n := range fb.nullIf {
			nulls[i] = fmt.Sprintf(`'%v'`, EscapeString(n))
		}
		builder.WriteString(fmt.Sprintf(` NULL_IF = (%v)`, strings.Join(nulls, ",")))
	}

	return builder.String()
}

// ChangeComment returns the SQL query that will update the comment on the file format.
func (fb *FileFormatBuilder) ChangeComment(comment string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET COMMENT = '%v'`, fb.QualifiedName(), comment)
}

// ChangeComment returns the SQL query that will update the compression on the file format.
func (fb *FileFormatBuilder) ChangeCompression(compression string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET COMPRESSION = '%v'`, fb.QualifiedName(), compression)
}

// ChangeComment returns the SQL query that will update binary as text on the file format.
func (fb *FileFormatBuilder) ChangeBinaryAsText(binaryAsText bool) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET BINARY_AS_TEXT = %v`, fb.QualifiedName(), binaryAsText)
}

// ChangeComment returns the SQL query that will update trim space on the file format.
func (fb *FileFormatBuilder) ChangeTrimSpace(trimSpace bool) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET TRIM_SPACE = %v`, fb.QualifiedName(), trimSpace)
}

// ChangeComment returns the SQL query that will update null if on the file format.
func (fb *FileFormatBuilder) ChangeNullIf(nullIf []string) string {
	nulls := make([]string, len(nullIf))
	for i, n := range nullIf {
		nulls[i] = fmt.Sprintf(`'%v'`, EscapeString(n))
	}
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET NULL_IF = (%v)`, fb.QualifiedName(), strings.Join(nulls, ","))
}

// Drop returns the SQL query that will drop a file format.
func (fb *FileFormatBuilder) Drop() string {
	return fmt.Sprintf(`DROP FILE FORMAT %v`, fb.QualifiedName())
}

// Describe returns the SQL query that will describe a file format.
func (fb *FileFormatBuilder) Describe() string {
	return fmt.Sprintf(`DESCRIBE FILE FORMAT %v`, fb.QualifiedName())
}

// Show returns the SQL query that will show a file format.
func (fb *FileFormatBuilder) Show() string {
	return fmt.Sprintf(`SHOW FILE FORMATS LIKE '%v' IN DATABASE "%v"`, fb.name, fb.db)
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
	Type         string
	Compression  string
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
			result.Type = row.PropertyValue
		case "COMPRESSION":
			result.Compression = row.PropertyValue
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
			if len(strs) < 1 {
				result.NullIf = nil
			} else {
				nulls := strings.Split(strs, ",")
				result.NullIf = make([]string, len(nulls))
				for i, str := range nulls {
					result.NullIf[i] = UnescapeString(strings.TrimSpace(str))
				}
			}
		}
	}

	return result, nil
}
