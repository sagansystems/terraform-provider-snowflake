package snowflake_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/snowflake"
)

const (
	fileFormatName = "test_file_format"
	databaseName   = "test_db"
	schemaName     = "test_schema"
	binaryAsText   = true
	trimSpace      = true
	fileFormatType = "parquet"
	comment        = "This is a test"
	compression    = "lzo"
)

func TestFileFormatCreate(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)

	r.Equal(fmt.Sprintf(`"%v"."%v"."%v"`, databaseName, schemaName, fileFormatName), ff.QualifiedName())

	query := fmt.Sprintf(
		`CREATE FILE FORMAT "%v"."%v"."%v" BINARY_AS_TEXT = false TRIM_SPACE = false`,
		databaseName, schemaName, fileFormatName,
	)
	r.Equal(query, ff.Create())

	ff.WithBinaryAsText(binaryAsText)
	query = fmt.Sprintf(
		`CREATE FILE FORMAT "%v"."%v"."%v" BINARY_AS_TEXT = %v TRIM_SPACE = false`,
		databaseName, schemaName, fileFormatName, binaryAsText,
	)
	r.Equal(query, ff.Create())

	ff.WithTrimSpace(trimSpace)
	query = fmt.Sprintf(
		`CREATE FILE FORMAT "%v"."%v"."%v" BINARY_AS_TEXT = %v TRIM_SPACE = %v`,
		databaseName, schemaName, fileFormatName, binaryAsText, trimSpace,
	)
	r.Equal(query, ff.Create())

	ff.WithType(fileFormatType)
	query += fmt.Sprintf(` TYPE = "%v"`, fileFormatType)
	r.Equal(query, ff.Create())

	ff.WithComment(comment)
	query += fmt.Sprintf(` COMMENT = "%v"`, comment)
	r.Equal(query, ff.Create())

	ff.WithCompression(compression)
	query += fmt.Sprintf(` COMPRESSION = "%v"`, compression)
	r.Equal(query, ff.Create())

	ff.WithNullIf([]string{`\N`, "NULL", ""})
	query += fmt.Sprintf(` NULL_IF = ('\\N','NULL','')`)
	r.Equal(query, ff.Create())
}

func TestFileFormatChangeComment(t *testing.T) {
	r := require.New(t)
	ff := snowflake.FileFormat(fileFormatName, databaseName, schemaName)
	r.Equal(
		fmt.Sprintf(`ALTER FILE FORMAT "%v"."%v"."%v" SET COMMENT = "%v"`, databaseName, schemaName, fileFormatName, comment),
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
