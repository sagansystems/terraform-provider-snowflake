package resources_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/stretchr/testify/require"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/provider"
	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/resources"
	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/testhelpers"
)

const (
	fileFormatName = "test_file_format"
	databaseName   = "test_db"
	schemaName     = "test_schema"
	fileFormatType = "parquet"
	comment        = "This is a test"
)

func TestFileFormat(t *testing.T) {
	r := require.New(t)
	err := resources.FileFormat().InternalValidate(provider.Provider().Schema, true)
	r.NoError(err)
}

func TestFileFormatRead(t *testing.T) {
	r := require.New(t)

	data := schema.TestResourceDataRaw(t, resources.FileFormat().Schema, map[string]interface{}{
		"name":     fileFormatName,
		"database": databaseName,
		"schema":   schemaName,
		"type":     fileFormatType,
		"comment":  comment,
	})
	data.SetId(strings.Join([]string{databaseName, schemaName, fileFormatName}, "|"))
	r.NotNil(data)

	testhelpers.WithMockDb(t, func(db *sql.DB, sqlmock sqlmock.Sqlmock) {
		expectReadFileFormat(sqlmock)

		err := resources.ReadFileFormat(data, db)
		r.NoError(err)
	})
}

func TestFileFormatCreate(t *testing.T) {
	r := require.New(t)

	data := schema.TestResourceDataRaw(t, resources.FileFormat().Schema, map[string]interface{}{
		"name":     fileFormatName,
		"database": databaseName,
		"schema":   schemaName,
		"type":     fileFormatType,
		"comment":  comment,
	})
	r.NotNil(data)

	testhelpers.WithMockDb(t, func(db *sql.DB, mock sqlmock.Sqlmock) {
		mock.ExpectExec(
			fmt.Sprintf(
				`^CREATE FILE FORMAT "%v"."%v"."%v" TYPE = "%v" COMMENT = "%v" BINARY_AS_TEXT = true TRIM_SPACE = false$`,
				databaseName, schemaName, fileFormatName, fileFormatType, comment,
			),
		).WillReturnResult(sqlmock.NewResult(1, 1))

		expectReadFileFormat(mock)

		err := resources.CreateFileFormat(data, db)
		r.NoError(err)
	})
}

func expectReadFileFormat(sqlmock sqlmock.Sqlmock) {
	showRows := sqlmock.NewRows([]string{
		"format_options", "created_on", "name", "database_name", "schema_name", "type", "owner", "comment",
	}).AddRow("{}", "2000-01-01 00:00:00.000 +0000", fileFormatName, databaseName, schemaName, fileFormatType, "SYSADMIN", comment)
	sqlmock.ExpectQuery(fmt.Sprintf(`^SHOW FILE FORMATS LIKE '%v' IN DATABASE "%v"$`, fileFormatName, databaseName)).
		WillReturnRows(showRows)

	descRows := sqlmock.NewRows([]string{
		"property", "property_type", "property_value", "property_default",
	}).AddRow("TYPE", "String", fileFormatType, "CSV").
		AddRow("TRIM_SPACE", "Boolean", "false", "false").
		AddRow("NULL_IF", "List", `["\\N","NULL",""]`, `["\\N"]`).
		AddRow("COMPRESSION", "String", "AUTO", "AUTO").
		AddRow("BINARY_AS_TEXT", "Boolean", "true", "true")
	sqlmock.ExpectQuery(fmt.Sprintf(`^DESCRIBE FILE FORMAT "%v"."%v"."%v"$`, databaseName, schemaName, fileFormatName)).
		WillReturnRows(descRows)
}
