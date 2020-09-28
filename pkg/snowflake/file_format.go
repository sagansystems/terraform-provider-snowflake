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
	name                          string
	db                            string
	schema                        string
	fileFormatType                string
	comment                       string
	compression                   string
	setBinaryAsText               bool
	binaryAsText                  bool
	setTrimSpace                  bool
	trimSpace                     bool
	nullIf                        []string
	recordDelimiter               string
	fieldDelimiter                string
	fileExtension                 string
	setSkipHeader                 bool
	skipHeader                    int
	setSkipBlankLines             bool
	skipBlankLines                bool
	dateFormat                    string
	timeFormat                    string
	timestampFormat               string
	binaryFormat                  string
	escape                        string
	escapeUnenclosedField         string
	fieldOptionallyEnclosedBy     string
	setErrorOnColumnCountMismatch bool
	errorOnColumnCountMismatch    bool
	setReplaceInvalidCharacters   bool
	replaceInvalidCharacters      bool
	setValidateUtf8               bool
	validateUtf8                  bool
	setEmptyFieldAsNull           bool
	emptyFieldAsNull              bool
	setSkipByteOrderMark          bool
	skipByteOrderMark             bool
	encoding                      string
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
	fb.setBinaryAsText = true
	fb.binaryAsText = binaryAsText
	return fb
}

// WithTrimSpace adds trim space to the FileFormatBuilder
func (fb *FileFormatBuilder) WithTrimSpace(trimSpace bool) *FileFormatBuilder {
	fb.setTrimSpace = true
	fb.trimSpace = trimSpace
	return fb
}

// WithNullIf adds null if to the FileFormatBuilder
func (fb *FileFormatBuilder) WithNullIf(nullIf []string) *FileFormatBuilder {
	fb.nullIf = nullIf
	return fb
}

// WithRecordDelimiter adds a record delimiter to the FileFormatBuilder
func (fb *FileFormatBuilder) WithRecordDelimiter(recordDelimiter string) *FileFormatBuilder {
	fb.recordDelimiter = recordDelimiter
	return fb
}

// WithFieldDelimiter adds a field delimiter to the FileFormatBuilder
func (fb *FileFormatBuilder) WithFieldDelimiter(fieldDelimiter string) *FileFormatBuilder {
	fb.fieldDelimiter = fieldDelimiter
	return fb
}

// WithFileExtension adds a file extension to the FileFormatBuilder
func (fb *FileFormatBuilder) WithFileExtension(fileExtension string) *FileFormatBuilder {
	fb.fileExtension = fileExtension
	return fb
}

// WithSkipHeader adds skip header to the FileFormatBuilder
func (fb *FileFormatBuilder) WithSkipHeader(skipHeader int) *FileFormatBuilder {
	fb.setSkipHeader = true
	fb.skipHeader = skipHeader
	return fb
}

// WithSkipBlankLines adds skip blank lines to the FileFormatBuilder
func (fb *FileFormatBuilder) WithSkipBlankLines(skipBlankLines bool) *FileFormatBuilder {
	fb.setSkipBlankLines = true
	fb.skipBlankLines = skipBlankLines
	return fb
}

// WithDateFormat adds a date format to the FileFormatBuilder
func (fb *FileFormatBuilder) WithDateFormat(dateFormat string) *FileFormatBuilder {
	fb.dateFormat = dateFormat
	return fb
}

// WithTimeFormat adds a time format to the FileFormatBuilder
func (fb *FileFormatBuilder) WithTimeFormat(timeFormat string) *FileFormatBuilder {
	fb.timeFormat = timeFormat
	return fb
}

// WithTimestampFormat adds a timestamp format to the FileFormatBuilder
func (fb *FileFormatBuilder) WithTimestampFormat(timestampFormat string) *FileFormatBuilder {
	fb.timestampFormat = timestampFormat
	return fb
}

// WithBinaryFormat adds a binary format to the FileFormatBuilder
func (fb *FileFormatBuilder) WithBinaryFormat(binaryFormat string) *FileFormatBuilder {
	fb.binaryFormat = binaryFormat
	return fb
}

// WithEscape adds escape to the FileFormatBuilder
func (fb *FileFormatBuilder) WithEscape(escape string) *FileFormatBuilder {
	fb.escape = escape
	return fb
}

// WithEscapeUnenclosedField adds escape unenclosed field to the FileFormatBuilder
func (fb *FileFormatBuilder) WithEscapeUnenclosedField(escapeUnenclosedField string) *FileFormatBuilder {
	fb.escapeUnenclosedField = escapeUnenclosedField
	return fb
}

// WithFieldOptionallyEnclosedBy adds field optionally enclosed by to the FileFormatBuilder
func (fb *FileFormatBuilder) WithFieldOptionallyEnclosedBy(fieldOptionallyEnclosedBy string) *FileFormatBuilder {
	fb.fieldOptionallyEnclosedBy = fieldOptionallyEnclosedBy
	return fb
}

// WithErrorOnColumnCountMismatch adds error on column counter mismatch to the FileFormatBuilder
func (fb *FileFormatBuilder) WithErrorOnColumnCountMismatch(errorOnColumnCountMismatch bool) *FileFormatBuilder {
	fb.setErrorOnColumnCountMismatch = true
	fb.errorOnColumnCountMismatch = errorOnColumnCountMismatch
	return fb
}

// WithReplaceInvalidCharacters adds replace invalid characters to the FileFormatBuilder
func (fb *FileFormatBuilder) WithReplaceInvalidCharacters(replaceInvalidCharacters bool) *FileFormatBuilder {
	fb.setReplaceInvalidCharacters = true
	fb.replaceInvalidCharacters = replaceInvalidCharacters
	return fb
}

// WithValidateUtf8 adds validate UTF8 to the FileFormatBuilder
func (fb *FileFormatBuilder) WithValidateUtf8(validateUtf8 bool) *FileFormatBuilder {
	fb.setValidateUtf8 = true
	fb.validateUtf8 = validateUtf8
	return fb
}

// WithEmptyFieldAsNull adds empty field as null to the FileFormatBuilder
func (fb *FileFormatBuilder) WithEmptyFieldAsNull(emptyFieldAsNull bool) *FileFormatBuilder {
	fb.setEmptyFieldAsNull = true
	fb.emptyFieldAsNull = emptyFieldAsNull
	return fb
}

// WithSkipByteOrderMark adds skip byte order mark to the FileFormatBuilder
func (fb *FileFormatBuilder) WithSkipByteOrderMark(skipByteOrderMark bool) *FileFormatBuilder {
	fb.setSkipByteOrderMark = true
	fb.skipByteOrderMark = skipByteOrderMark
	return fb
}

// WithEncoding adds encoding to the FileFormatBuilder
func (fb *FileFormatBuilder) WithEncoding(encoding string) *FileFormatBuilder {
	fb.encoding = encoding
	return fb
}

// FileFormat returns a pointer to a Builder that abstracts the DDL operations for a stage.
//
// Supported DDL operations are:
//   - CREATE FILE FORMAT
//   - ALTER FILE FORMAT
//   - DROP FILE FORMAT
//   - DESCRIBE FILE FORMAT
//   - SHOW FILE FORMAT
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

	if fb.setBinaryAsText {
		builder.WriteString(fmt.Sprintf(` BINARY_AS_TEXT = %v`, fb.binaryAsText))
	}

	if fb.setTrimSpace {
		builder.WriteString(fmt.Sprintf(` TRIM_SPACE = %v`, fb.trimSpace))
	}

	if len(fb.nullIf) > 0 {
		nulls := make([]string, len(fb.nullIf))
		for i, n := range fb.nullIf {
			nulls[i] = fmt.Sprintf(`'%v'`, EscapeString(n))
		}
		builder.WriteString(fmt.Sprintf(` NULL_IF = (%v)`, strings.Join(nulls, ",")))
	}

	if fb.recordDelimiter != "" {
		builder.WriteString(fmt.Sprintf(` RECORD_DELIMITER = "%v"`, EscapeString(fb.recordDelimiter)))
	}

	if fb.fieldDelimiter != "" {
		builder.WriteString(fmt.Sprintf(` FIELD_DELIMITER = "%v"`, EscapeString(fb.fieldDelimiter)))
	}

	if fb.fileExtension != "" {
		builder.WriteString(fmt.Sprintf(` FILE_EXTENSION = "%v"`, EscapeString(fb.fileExtension)))
	}

	if fb.setSkipHeader {
		builder.WriteString(fmt.Sprintf(` SKIP_HEADER = %v`, fb.skipHeader))
	}

	if fb.setSkipBlankLines {
		builder.WriteString(fmt.Sprintf(` SKIP_BLANK_LINES = %v`, fb.skipBlankLines))
	}

	if fb.dateFormat != "" {
		builder.WriteString(fmt.Sprintf(` DATE_FORMAT = "%v"`, EscapeString(fb.dateFormat)))
	}

	if fb.timeFormat != "" {
		builder.WriteString(fmt.Sprintf(` TIME_FORMAT = "%v"`, EscapeString(fb.timeFormat)))
	}

	if fb.timestampFormat != "" {
		builder.WriteString(fmt.Sprintf(` TIMESTAMP_FORMAT = "%v"`, EscapeString(fb.timestampFormat)))
	}

	if fb.binaryFormat != "" {
		builder.WriteString(fmt.Sprintf(` BINARY_FORMAT = %v`, fb.binaryFormat))
	}

	if fb.escape != "" {
		builder.WriteString(fmt.Sprintf(` ESCAPE = "%v"`, EscapeString(fb.escape)))
	}

	if fb.escapeUnenclosedField != "" {
		builder.WriteString(fmt.Sprintf(` ESCAPE_UNENCLOSED_FIELD = "%v"`, EscapeString(fb.escapeUnenclosedField)))
	}

	if fb.fieldOptionallyEnclosedBy != "" {
		builder.WriteString(fmt.Sprintf(` FIELD_OPTIONALLY_ENCLOSED_BY = "%v"`, EscapeString(fb.fieldOptionallyEnclosedBy)))
	}

	if fb.setErrorOnColumnCountMismatch {
		builder.WriteString(fmt.Sprintf(` ERROR_ON_COLUMN_COUNT_MISMATCH = %v`, fb.errorOnColumnCountMismatch))
	}

	if fb.setReplaceInvalidCharacters {
		builder.WriteString(fmt.Sprintf(` REPLACE_INVALID_CHARACTERS = %v`, fb.replaceInvalidCharacters))
	}

	if fb.setValidateUtf8 {
		builder.WriteString(fmt.Sprintf(` VALIDATE_UTF8 = %v`, fb.validateUtf8))
	}

	if fb.setEmptyFieldAsNull {
		builder.WriteString(fmt.Sprintf(` EMPTY_FIELD_AS_NULL = %v`, fb.emptyFieldAsNull))
	}

	if fb.setSkipByteOrderMark {
		builder.WriteString(fmt.Sprintf(` SKIP_BYTE_ORDER_MARK = %v`, fb.skipByteOrderMark))
	}

	if fb.encoding != "" {
		builder.WriteString(fmt.Sprintf(` ENCODING = "%v"`, EscapeString(fb.encoding)))
	}

	return builder.String()
}

// ChangeComment returns the SQL query that will update the comment on the file format.
func (fb *FileFormatBuilder) ChangeComment(comment string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET COMMENT = "%v"`, fb.QualifiedName(), EscapeString(comment))
}

// ChangeComment returns the SQL query that will update the compression on the file format.
func (fb *FileFormatBuilder) ChangeCompression(compression string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET COMPRESSION = "%v"`, fb.QualifiedName(), compression)
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

// ChangeRecordDelimiter returns the SQL query that will update record delimiter on the file format.
func (fb *FileFormatBuilder) ChangeRecordDelimiter(recordDelimiter string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET RECORD_DELIMITER = "%v"`, fb.QualifiedName(), EscapeString(recordDelimiter))
}

// ChangeFieldDelimiter returns the SQL query that will update field delimiter on the file format.
func (fb *FileFormatBuilder) ChangeFieldDelimiter(fieldDelimiter string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET FIELD_DELIMITER = "%v"`, fb.QualifiedName(), EscapeString(fieldDelimiter))
}

// ChangeFileExtension returns the SQL query that will update file extension on the file format.
func (fb *FileFormatBuilder) ChangeFileExtension(fileExtension string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET FILE_EXTENSION = "%v"`, fb.QualifiedName(), EscapeString(fileExtension))
}

// ChangeSkipHeader returns the SQL query that will update skip header on the file format.
func (fb *FileFormatBuilder) ChangeSkipHeader(skipHeader int) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET SKIP_HEADER = %v`, fb.QualifiedName(), skipHeader)
}

// ChangeSkipBlankLines returns the SQL query that will update skip blank lines on the file format.
func (fb *FileFormatBuilder) ChangeSkipBlankLines(skipBlankLines bool) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET SKIP_BLANK_LINES = %v`, fb.QualifiedName(), skipBlankLines)
}

// ChangeDateFormat returns the SQL query that will update dateformat on the file format.
func (fb *FileFormatBuilder) ChangeDateFormat(dateFormat string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET DATE_FORMAT = "%v"`, fb.QualifiedName(), EscapeString(dateFormat))
}

// ChangeTimeFormat returns the SQL query that will update time format on the file format.
func (fb *FileFormatBuilder) ChangeTimeFormat(timeFormat string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET TIME_FORMAT = "%v"`, fb.QualifiedName(), EscapeString(timeFormat))
}

// ChangeTimestampFormat returns the SQL query that will update timestamp format on the file format.
func (fb *FileFormatBuilder) ChangeTimestampFormat(timestampFormat string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET TIMESTAMP_FORMAT = "%v"`, fb.QualifiedName(), EscapeString(timestampFormat))
}

// ChangeBinaryFormat returns the SQL query that will update binary format on the file format.
func (fb *FileFormatBuilder) ChangeBinaryFormat(binaryFormat string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET BINARY_FORMAT = %v`, fb.QualifiedName(), binaryFormat)
}

// ChangeEscape returns the SQL query that will update escape on the file format.
func (fb *FileFormatBuilder) ChangeEscape(escape string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET ESCAPE = "%v"`, fb.QualifiedName(), EscapeString(escape))
}

// ChangeEscapeUnenclosedField returns the SQL query that will update escape unenclosed field on the file format.
func (fb *FileFormatBuilder) ChangeEscapeUnenclosedField(escapeUnenclosedField string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET ESCAPE_UNENCLOSED_FIELD = "%v"`, fb.QualifiedName(), EscapeString(escapeUnenclosedField))
}

// ChangeFieldOptionallyEnclosedBy returns the SQL query that will update field optionally enclosed by on the file format.
func (fb *FileFormatBuilder) ChangeFieldOptionallyEnclosedBy(fieldOptionallyEnclosedBy string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET FIELD_OPTIONALLY_ENCLOSED_BY = "%v"`, fb.QualifiedName(), EscapeString(fieldOptionallyEnclosedBy))
}

// ChangeErrorOnColumnCountMismatch returns the SQL query that will update error on column count mismatch on the file format.
func (fb *FileFormatBuilder) ChangeErrorOnColumnCountMismatch(errorOnColumnCountMismatch bool) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET ERROR_ON_COLUMN_COUNT_MISMATCH = %v`, fb.QualifiedName(), errorOnColumnCountMismatch)
}

// ChangeReplaceInvalidCharacters returns the SQL query that will update replace invalid characters on the file format.
func (fb *FileFormatBuilder) ChangeReplaceInvalidCharacters(replaceInvalidCharacters bool) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET REPLACE_INVALID_CHARACTERS = %v`, fb.QualifiedName(), replaceInvalidCharacters)
}

// ChangeValidateUtf8 returns the SQL query that will update validate utf8 on the file format.
func (fb *FileFormatBuilder) ChangeValidateUtf8(validateUtf8 bool) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET VALIDATE_UTF8 = %v`, fb.QualifiedName(), validateUtf8)
}

// ChangeEmptyFieldAsNull returns the SQL query that will update empty field as null on the file format.
func (fb *FileFormatBuilder) ChangeEmptyFieldAsNull(emptyFieldAsNull bool) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET EMPTY_FIELD_AS_NULL = %v`, fb.QualifiedName(), emptyFieldAsNull)
}

// ChangeSkipByteOrderMark returns the SQL query that will update skip byte order mark on the file format.
func (fb *FileFormatBuilder) ChangeSkipByteOrderMark(skipByteOrderMark bool) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET SKIP_BYTE_ORDER_MARK = %v`, fb.QualifiedName(), skipByteOrderMark)
}

// ChangeEncoding returns the SQL query that will update encoding on the file format.
func (fb *FileFormatBuilder) ChangeEncoding(encoding string) string {
	return fmt.Sprintf(`ALTER FILE FORMAT %v SET ENCODING = "%v"`, fb.QualifiedName(), EscapeString(encoding))
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
	Type                       string
	Compression                string
	BinaryAsText               bool
	TrimSpace                  bool
	NullIf                     []string
	RecordDelimiter            string
	FieldDelimiter             string
	FileExtension              string
	SkipHeader                 int
	SkipBlankLines             bool
	DateFormat                 string
	TimeFormat                 string
	TimestampFormat            string
	BinaryFormat               string
	Escape                     string
	EscapeUnenclosedField      string
	FieldOptionallyEnclosedBy  string
	ErrorOnColumnCountMismatch bool
	ReplaceInvalidCharacters   bool
	ValidateUtf8               bool
	EmptyFieldAsNull           bool
	SkipByteOrderMark          bool
	Encoding                   string
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
		case "RECORD_DELIMITER":
			result.RecordDelimiter = row.PropertyValue
		case "FIELD_DELIMITER":
			result.FieldDelimiter = row.PropertyValue
		case "FILE_EXTENSION":
			result.FileExtension = row.PropertyValue
		case "SKIP_HEADER":
			v, err := strconv.Atoi(row.PropertyValue)
			if err != nil {
				return &fileFormatData{}, err
			}
			result.SkipHeader = v
		case "SKIP_BLANK_LINES":
			v, err := strconv.ParseBool(row.PropertyValue)
			if err != nil {
				return &fileFormatData{}, err
			}
			result.SkipBlankLines = v
		case "DATE_FORMAT":
			result.DateFormat = row.PropertyValue
		case "TIME_FORMAT":
			result.TimeFormat = row.PropertyValue
		case "TIMESTAMP_FORMAT":
			result.TimestampFormat = row.PropertyValue
		case "BINARY_FORMAT":
			result.BinaryFormat = row.PropertyValue
		case "ESCAPE":
			result.Escape = row.PropertyValue
		case "ESCAPE_UNENCLOSED_FIELD":
			result.EscapeUnenclosedField = row.PropertyValue
		case "FIELD_OPTIONALLY_ENCLOSED_BY":
			result.FieldOptionallyEnclosedBy = row.PropertyValue
		case "ERROR_ON_COLUMN_COUNT_MISMATCH":
			v, err := strconv.ParseBool(row.PropertyValue)
			if err != nil {
				return &fileFormatData{}, err
			}
			result.ErrorOnColumnCountMismatch = v
		case "REPLACE_INVALID_CHARACTERS":
			v, err := strconv.ParseBool(row.PropertyValue)
			if err != nil {
				return &fileFormatData{}, err
			}
			result.ReplaceInvalidCharacters = v
		case "VALIDATE_UTF8":
			v, err := strconv.ParseBool(row.PropertyValue)
			if err != nil {
				return &fileFormatData{}, err
			}
			result.ValidateUtf8 = v
		case "EMPTY_FIELD_AS_NULL":
			v, err := strconv.ParseBool(row.PropertyValue)
			if err != nil {
				return &fileFormatData{}, err
			}
			result.EmptyFieldAsNull = v
		case "SKIP_BYTE_ORDER_MARK":
			v, err := strconv.ParseBool(row.PropertyValue)
			if err != nil {
				return &fileFormatData{}, err
			}
			result.SkipByteOrderMark = v
		case "ENCODING":
			result.Encoding = row.PropertyValue
		}
	}

	return result, nil
}
