package resources_test

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/stretchr/testify/require"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/provider"
	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/resources"
	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/testhelpers"
)

func TestFileFormat(t *testing.T) {
	r := require.New(t)
	err := resources.FileFormat().InternalValidate(provider.Provider().Schema, true)
	r.NoError(err)
}

func TestFileFormatRead(t *testing.T) {
	r := require.New(t)

	data := schema.TestResourceDataRaw(t, resources.FileFormat().Schema, map[string]interface{}{
		"name":     "test_file_format",
		"database": "test_db",
		"schema":   "test_schema",
		"type":     "parquet",
		"comment":  "This is a test",
	})
	r.NotNil(data)

	testhelpers.WithMockDb(t, func(db *sql.DB, sqlmock sqlmock.Sqlmock) {
		_ = sqlmock.ExpectExec(``)
	})
}
