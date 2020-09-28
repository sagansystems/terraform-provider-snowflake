package snowflake_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/snowflake"
)

const (
	fileFormatName             = "test_file_format"
	databaseName               = "test_db"
	schemaName                 = "test_schema"
	fileFormatType             = "parquet"
	comment                    = "This is a test"
	compression                = "lzo"
	binaryAsText               = true
	trimSpace                  = false
	recordDelimiter            = `\n`
	fieldDelimiter             = "."
	fileExtension              = ".csv.lzo"
	skipHeader                 = 1
	skipBlankLines             = false
	dateFormat                 = "auto"
	timeFormat                 = "auto"
	timestampFormat            = "auto"
	binaryFormat               = "HEX"
	escape                     = "none"
	escapeUnenclosedField      = `\`
	fieldOptionallyEnclosedBy  = "none"
	errorOnColumnCountMismatch = true
	replaceInvalidCharacters   = false
	validateUtf8               = true
	emptyFieldAsNull           = true
	skipByteOrderMark          = true
	encoding                   = "UTF8"
)

func TestFileFormatCreate(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)

	r.Equal(fmt.Sprintf(`"%v"."%v"."%v"`, databaseName, schemaName, fileFormatName), ff.QualifiedName())

	query := fmt.Sprintf(
		`CREATE FILE FORMAT "%v"."%v"."%v"`,
		databaseName, schemaName, fileFormatName,
	)
	r.Equal(query, ff.Create())

	ff.WithType(fileFormatType)
	query += fmt.Sprintf(` TYPE = "%v"`, fileFormatType)
	r.Equal(query, ff.Create())

	ff.WithComment(comment)
	query += fmt.Sprintf(` COMMENT = "%v"`, snowflake.EscapeString(comment))
	r.Equal(query, ff.Create())

	ff.WithCompression(compression)
	query += fmt.Sprintf(` COMPRESSION = "%v"`, compression)
	r.Equal(query, ff.Create())

	ff.WithBinaryAsText(binaryAsText)
	query += fmt.Sprintf(` BINARY_AS_TEXT = %v`, binaryAsText)
	r.Equal(query, ff.Create())

	ff.WithTrimSpace(trimSpace)
	query += fmt.Sprintf(` TRIM_SPACE = %v`, trimSpace)
	r.Equal(query, ff.Create())

	ff.WithNullIf([]string{`\N`, "NULL", ""})
	query += fmt.Sprintf(` NULL_IF = ('\\N','NULL','')`)
	r.Equal(query, ff.Create())

	ff.WithRecordDelimiter(recordDelimiter)
	query += fmt.Sprintf(` RECORD_DELIMITER = "%v"`, snowflake.EscapeString(recordDelimiter))
	r.Equal(query, ff.Create())

	ff.WithFieldDelimiter(fieldDelimiter)
	query += fmt.Sprintf(` FIELD_DELIMITER = "%v"`, snowflake.EscapeString(fieldDelimiter))
	r.Equal(query, ff.Create())

	ff.WithFileExtension(fileExtension)
	query += fmt.Sprintf(` FILE_EXTENSION = "%v"`, snowflake.EscapeString(fileExtension))
	r.Equal(query, ff.Create())

	ff.WithSkipHeader(skipHeader)
	query += fmt.Sprintf(` SKIP_HEADER = %v`, skipHeader)
	r.Equal(query, ff.Create())

	ff.WithSkipBlankLines(skipBlankLines)
	query += fmt.Sprintf(` SKIP_BLANK_LINES = %v`, skipBlankLines)
	r.Equal(query, ff.Create())

	ff.WithDateFormat(dateFormat)
	query += fmt.Sprintf(` DATE_FORMAT = "%v"`, snowflake.EscapeString(dateFormat))
	r.Equal(query, ff.Create())

	ff.WithTimeFormat(timeFormat)
	query += fmt.Sprintf(` TIME_FORMAT = "%v"`, snowflake.EscapeString(timeFormat))
	r.Equal(query, ff.Create())

	ff.WithTimestampFormat(timestampFormat)
	query += fmt.Sprintf(` TIMESTAMP_FORMAT = "%v"`, snowflake.EscapeString(timestampFormat))
	r.Equal(query, ff.Create())

	ff.WithBinaryFormat(binaryFormat)
	query += fmt.Sprintf(` BINARY_FORMAT = %v`, binaryFormat)
	r.Equal(query, ff.Create())

	ff.WithEscape(escape)
	query += fmt.Sprintf(` ESCAPE = "%v"`, snowflake.EscapeString(escape))
	r.Equal(query, ff.Create())

	ff.WithEscapeUnenclosedField(escapeUnenclosedField)
	query += fmt.Sprintf(` ESCAPE_UNENCLOSED_FIELD = "%v"`, snowflake.EscapeString(escapeUnenclosedField))
	r.Equal(query, ff.Create())

	ff.WithFieldOptionallyEnclosedBy(fieldOptionallyEnclosedBy)
	query += fmt.Sprintf(` FIELD_OPTIONALLY_ENCLOSED_BY = "%v"`, snowflake.EscapeString(fieldOptionallyEnclosedBy))
	r.Equal(query, ff.Create())

	ff.WithErrorOnColumnCountMismatch(errorOnColumnCountMismatch)
	query += fmt.Sprintf(` ERROR_ON_COLUMN_COUNT_MISMATCH = %v`, errorOnColumnCountMismatch)
	r.Equal(query, ff.Create())

	ff.WithReplaceInvalidCharacters(replaceInvalidCharacters)
	query += fmt.Sprintf(` REPLACE_INVALID_CHARACTERS = %v`, replaceInvalidCharacters)
	r.Equal(query, ff.Create())

	ff.WithValidateUtf8(validateUtf8)
	query += fmt.Sprintf(` VALIDATE_UTF8 = %v`, validateUtf8)
	r.Equal(query, ff.Create())

	ff.WithEmptyFieldAsNull(emptyFieldAsNull)
	query += fmt.Sprintf(` EMPTY_FIELD_AS_NULL = %v`, emptyFieldAsNull)
	r.Equal(query, ff.Create())

	ff.WithSkipByteOrderMark(skipByteOrderMark)
	query += fmt.Sprintf(` SKIP_BYTE_ORDER_MARK = %v`, skipByteOrderMark)
	r.Equal(query, ff.Create())

	ff.WithEncoding(encoding)
	query += fmt.Sprintf(` ENCODING = "%v"`, snowflake.EscapeString(encoding))
	r.Equal(query, ff.Create())

}

func TestFileFormatChangeComment(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET COMMENT = "%v"`, databaseName, schemaName, fileFormatName, snowflake.EscapeString(comment)),
		ff.ChangeComment(comment),
	)
}

func TestFileFormatChangeCompression(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET COMPRESSION = "%v"`, databaseName, schemaName, fileFormatName, compression),
		ff.ChangeCompression(compression),
	)
}

func TestFileFormatChangeBinaryAsText(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET BINARY_AS_TEXT = %v`, databaseName, schemaName, fileFormatName, binaryAsText),
		ff.ChangeBinaryAsText(binaryAsText),
	)
}

func TestFileFormatChangeTrimSpace(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET TRIM_SPACE = %v`, databaseName, schemaName, fileFormatName, trimSpace),
		ff.ChangeTrimSpace(trimSpace),
	)
}

func TestFileFormatChangeNullIf(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET NULL_IF = ('\\N','NULL','')`, databaseName, schemaName, fileFormatName),
		ff.ChangeNullIf([]string{`\N`, "NULL", ""}),
	)
}

func TestFileFormatRecordDelimiter(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET RECORD_DELIMITER = "%v"`, databaseName, schemaName, fileFormatName, snowflake.EscapeString(recordDelimiter)),
		ff.ChangeRecordDelimiter(recordDelimiter),
	)
}

func TestFileFormatFieldDelimiter(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET FIELD_DELIMITER = "%v"`, databaseName, schemaName, fileFormatName, snowflake.EscapeString(fieldDelimiter)),
		ff.ChangeFieldDelimiter(fieldDelimiter),
	)
}

func TestFileFormatFileExtension(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET FILE_EXTENSION = "%v"`, databaseName, schemaName, fileFormatName, snowflake.EscapeString(fileExtension)),
		ff.ChangeFileExtension(fileExtension),
	)
}

func TestFileFormatSkipHeader(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET SKIP_HEADER = %v`, databaseName, schemaName, fileFormatName, skipHeader),
		ff.ChangeSkipHeader(skipHeader),
	)
}

func TestFileFormatSkipBlankLines(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET SKIP_BLANK_LINES = %v`, databaseName, schemaName, fileFormatName, skipBlankLines),
		ff.ChangeSkipBlankLines(skipBlankLines),
	)
}

func TestFileFormatDateFormat(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET DATE_FORMAT = "%v"`, databaseName, schemaName, fileFormatName, snowflake.EscapeString(dateFormat)),
		ff.ChangeDateFormat(dateFormat),
	)
}

func TestFileFormatTimeFormat(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET TIME_FORMAT = "%v"`, databaseName, schemaName, fileFormatName, snowflake.EscapeString(timeFormat)),
		ff.ChangeTimeFormat(timeFormat),
	)
}

func TestFileFormatTimestampFormat(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET TIMESTAMP_FORMAT = "%v"`, databaseName, schemaName, fileFormatName, snowflake.EscapeString(timestampFormat)),
		ff.ChangeTimestampFormat(timestampFormat),
	)
}

func TestFileFormatBinaryFormat(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET BINARY_FORMAT = %v`, databaseName, schemaName, fileFormatName, binaryFormat),
		ff.ChangeBinaryFormat(binaryFormat),
	)
}

func TestFileFormatEscape(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET ESCAPE = "%v"`, databaseName, schemaName, fileFormatName, snowflake.EscapeString(escape)),
		ff.ChangeEscape(escape),
	)
}

func TestFileFormatEscapeUnenclosedField(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET ESCAPE_UNENCLOSED_FIELD = "%v"`, databaseName, schemaName, fileFormatName, snowflake.EscapeString(escapeUnenclosedField)),
		ff.ChangeEscapeUnenclosedField(escapeUnenclosedField),
	)
}

func TestFileFormatFieldOptionallyEnclosedBy(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET FIELD_OPTIONALLY_ENCLOSED_BY = "%v"`, databaseName, schemaName, fileFormatName, snowflake.EscapeString(fieldOptionallyEnclosedBy)),
		ff.ChangeFieldOptionallyEnclosedBy(fieldOptionallyEnclosedBy),
	)
}

func TestFileFormatErrorOnColumnCountMismatch(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET ERROR_ON_COLUMN_COUNT_MISMATCH = %v`, databaseName, schemaName, fileFormatName, errorOnColumnCountMismatch),
		ff.ChangeErrorOnColumnCountMismatch(errorOnColumnCountMismatch),
	)
}

func TestFileFormatReplaceInvalidCharacters(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET REPLACE_INVALID_CHARACTERS = %v`, databaseName, schemaName, fileFormatName, replaceInvalidCharacters),
		ff.ChangeReplaceInvalidCharacters(replaceInvalidCharacters),
	)
}

func TestFileFormatValidateUtf8(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET VALIDATE_UTF8 = %v`, databaseName, schemaName, fileFormatName, validateUtf8),
		ff.ChangeValidateUtf8(validateUtf8),
	)
}

func TestFileFormatEmptyFieldAsNull(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET EMPTY_FIELD_AS_NULL = %v`, databaseName, schemaName, fileFormatName, emptyFieldAsNull),
		ff.ChangeEmptyFieldAsNull(emptyFieldAsNull),
	)
}

func TestFileFormatSkipByteOrderMark(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET SKIP_BYTE_ORDER_MARK = %v`, databaseName, schemaName, fileFormatName, skipByteOrderMark),
		ff.ChangeSkipByteOrderMark(skipByteOrderMark),
	)
}

func TestFileFormatEncoding(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET ENCODING = "%v"`, databaseName, schemaName, fileFormatName, snowflake.EscapeString(encoding)),
		ff.ChangeEncoding(encoding),
	)
}

func TestFileFormatDrop(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`DROP FILE FORMAT "%v"."%v"."%v"`, databaseName, schemaName, fileFormatName),
		ff.Drop(),
	)
}

func TestFileFormatDescribe(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`DESCRIBE FILE FORMAT "%v"."%v"."%v"`, databaseName, schemaName, fileFormatName),
		ff.Describe(),
	)
}

func TestFileFormatShow(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`SHOW FILE FORMATS LIKE '%v' IN DATABASE "%v"`, fileFormatName, databaseName),
		ff.Show(),
	)
}
