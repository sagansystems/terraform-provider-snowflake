package resources

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/pkg/errors"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/snowflake"
)

const (
	fileFormatIDDelimiter = '|'
)

var fileFormatSchema = map[string]*schema.Schema{
	"name": {
		Type:        schema.TypeString,
		Required:    true,
		Description: "Specifies the identifier for the file format; must be unique for the schema in which the file format is created.",
		ForceNew:    true, // TODO: Support RENAME TO
	},
	"database": {
		Type:        schema.TypeString,
		Required:    true,
		Description: "The database in which to create the file format.",
		ForceNew:    true,
	},
	"schema": {
		Type:        schema.TypeString,
		Required:    true,
		Description: "The schema in which to create the file format.",
		ForceNew:    true,
	},
	"type": {
		Type:        schema.TypeString,
		Required:    true,
		Description: "Specifies the format of the input files (for data loading) or output files (for data unloading). Depending on the format type, additional format-specific options can be specified.",
		ForceNew:    true,
		ValidateFunc: func(val interface{}, _ string) ([]string, []error) {
			t := strings.ToLower(val.(string))

			switch t {
			case "parquet":
				return nil, nil
			default:
				return nil, []error{fmt.Errorf("%s is not a supported type", val)}
			}
		},
	},
	"comment": {
		Type:        schema.TypeString,
		Optional:    true,
		Description: "Specifies a comment for the file format.",
	},
	"compression": {
		Type:        schema.TypeString,
		Optional:    true,
		Default:     "AUTO",
		Description: "Specifies the current compression algorithm for columns in the Parquet files.",
		ValidateFunc: func(val interface{}, _ string) ([]string, []error) {
			c := strings.ToLower(val.(string))

			switch c {
			case "auto", "lzo", "snappy", "none":
				return nil, nil
			default:
				return nil, []error{fmt.Errorf("%s is not a supported compression algorithm", val)}
			}
		},
	},
	"binary_as_text": {
		Type:        schema.TypeBool,
		Optional:    true,
		Default:     true,
		Description: "Boolean that specifies whether to interpret columns with no defined logical data type as UTF-8 text. When set to FALSE, Snowflake interprets these columns as binary data.",
	},
	"trim_space": {
		Type:        schema.TypeBool,
		Optional:    true,
		Default:     false,
		Description: "Applied only when loading Parquet data into separate columns (i.e. using the MATCH_BY_COLUMN_NAME copy option or a COPY transformation). Boolean that specifies whether to remove leading and trailing white space from strings.",
	},
	"null_if": {
		Type:        schema.TypeList,
		Elem:        &schema.Schema{Type: schema.TypeString},
		Optional:    true,
		Description: "Applied only when loading Parquet data into separate columns (i.e. using the MATCH_BY_COLUMN_NAME copy option or a COPY transformation). String used to convert to and from SQL NULL. Snowflake replaces these strings in the data load source with SQL NULL.",
	},
}

type fileFormatID struct {
	DatabaseName   string
	SchemaName     string
	FileFormatName string
}

// String() takes in a stageID object and returns a pipe-delimited string:
// DatabaseName|SchemaName|FileFormatName
func (id *fileFormatID) String() (string, error) {
	var buf bytes.Buffer
	csvWriter := csv.NewWriter(&buf)
	csvWriter.Comma = stageIDDelimiter
	dataIdentifiers := [][]string{{id.DatabaseName, id.SchemaName, id.FileFormatName}}
	err := csvWriter.WriteAll(dataIdentifiers)
	if err != nil {
		return "", err
	}
	strStageID := strings.TrimSpace(buf.String())
	return strStageID, nil
}

// fileFormatIDFromString() takes in a pipe-delimited string: DatabaseName|SchemaName|FileFormatName
// and returns a pointer to fileFormatID object
func fileFormatIDFromString(stringID string) (*fileFormatID, error) {
	reader := csv.NewReader(strings.NewReader(stringID))
	reader.Comma = fileFormatIDDelimiter
	lines, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("Not CSV compatible")
	}

	if len(lines) != 1 {
		return nil, fmt.Errorf("1 line per file format")
	}
	if len(lines[0]) != 3 {
		return nil, fmt.Errorf("3 fields allowed")
	}

	return &fileFormatID{
		DatabaseName:   lines[0][0],
		SchemaName:     lines[0][1],
		FileFormatName: lines[0][2],
	}, nil
}

// FileFormat returns a pointer to the resource representing a file format
func FileFormat() *schema.Resource {
	return &schema.Resource{
		Create: CreateFileFormat,
		Read:   ReadFileFormat,
		Update: UpdateFileFormat,
		Delete: DeleteFileFormat,

		Schema: fileFormatSchema,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
	}
}

// CreateFileFormat implements schema.CreateFunc
func CreateFileFormat(data *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := data.Get("name").(string)
	database := data.Get("database").(string)
	schema := data.Get("schema").(string)

	builder := snowflake.FileFormat(name, database, schema)

	fileFormatType := data.Get("type").(string)
	builder.WithType(fileFormatType)

	// Set optionals
	if v, ok := data.GetOk("comment"); ok {
		builder.WithComment(v.(string))
	}

	if v, ok := data.GetOk("compression"); ok {
		builder.WithCompression(v.(string))
	}

	if v, ok := data.GetOk("binary_as_text"); ok {
		builder.WithBinaryAsText(v.(bool))
	}

	if v, ok := data.GetOk("trim_space"); ok {
		builder.WithTrimSpace(v.(bool))
	}

	if v, ok := data.GetOk("null_if"); ok {
		ns := v.([]interface{})
		nulls := make([]string, len(ns))
		for i, n := range ns {
			if n == nil {
				nulls[i] = ""
			} else {
				nulls[i] = n.(string)
			}
		}
		builder.WithNullIf(nulls)
	}

	if err := snowflake.Exec(db, builder.Create()); err != nil {
		return errors.Wrapf(err, "error creating file format %v", name)
	}

	id := &fileFormatID{
		DatabaseName:   database,
		SchemaName:     schema,
		FileFormatName: name,
	}
	idStr, err := id.String()
	if err != nil {
		return err
	}
	data.SetId(idStr)

	return ReadFileFormat(data, meta)
}

// ReadStage implements schema.ReadFunc
func ReadFileFormat(data *schema.ResourceData, metadata interface{}) error {
	db := metadata.(*sql.DB)
	fileFormatID, err := fileFormatIDFromString(data.Id())
	if err != nil {
		return err
	}

	builder := snowflake.FileFormat(fileFormatID.FileFormatName, fileFormatID.DatabaseName, fileFormatID.SchemaName)

	ffData, err := snowflake.DescFileFormat(db, builder.Describe())
	if err != nil {
		return err
	}

	row := snowflake.QueryRow(db, builder.Show())
	ffMeta, err := snowflake.ScanFileFormatShow(row)
	if err != nil {
		return err
	}

	if err := data.Set("name", ffMeta.Name); err != nil {
		return err
	}

	if err := data.Set("database", ffMeta.DatabaseName); err != nil {
		return err
	}

	if err := data.Set("schema", ffMeta.SchemaName); err != nil {
		return err
	}

	if err := data.Set("comment", ffMeta.Comment); err != nil {
		return err
	}

	if err := data.Set("type", ffData.Type); err != nil {
		return err
	}

	if err := data.Set("compression", ffData.Compression); err != nil {
		return err
	}

	if err := data.Set("binary_as_text", ffData.BinaryAsText); err != nil {
		return err
	}

	if err := data.Set("trim_space", ffData.TrimSpace); err != nil {
		return err
	}

	if err := data.Set("null_if", ffData.NullIf); err != nil {
		return err
	}

	return nil
}

// UpdateFileFormat implements schema.UpdateFunc
func UpdateFileFormat(data *schema.ResourceData, meta interface{}) error {
	// https://www.terraform.io/docs/extend/writing-custom-providers.html#error-handling-amp-partial-state
	data.Partial(true)

	fileFormatID, err := fileFormatIDFromString(data.Id())
	if err != nil {
		return err
	}

	builder := snowflake.FileFormat(fileFormatID.FileFormatName, fileFormatID.DatabaseName, fileFormatID.SchemaName)

	db := meta.(*sql.DB)

	if data.HasChange("comment") {
		_, comment := data.GetChange("comment")
		if err := snowflake.Exec(db, builder.ChangeComment(comment.(string))); err != nil {
			return errors.Wrapf(err, "error updating file format comment on %v", data.Id())
		}

		data.SetPartial("comment")
	}

	if data.HasChange("compression") {
		_, compression := data.GetChange("compression")
		if err := snowflake.Exec(db, builder.ChangeCompression(compression.(string))); err != nil {
			return errors.Wrapf(err, "error updating file format compression on %v", data.Id())
		}

		data.SetPartial("compression")
	}

	if data.HasChange("binary_as_text") {
		_, binaryAsText := data.GetChange("binary_as_text")
		if err := snowflake.Exec(db, builder.ChangeBinaryAsText(binaryAsText.(bool))); err != nil {
			return errors.Wrapf(err, "error updating file format binary as text on %v", data.Id())
		}

		data.SetPartial("binary_as_text")
	}

	if data.HasChange("trim_space") {
		_, trimSpace := data.GetChange("trim_space")
		if err := snowflake.Exec(db, builder.ChangeTrimSpace(trimSpace.(bool))); err != nil {
			return errors.Wrapf(err, "error updating file format trim space on %v", data.Id())
		}

		data.SetPartial("trim_space")
	}

	if data.HasChange("null_if") {
		_, newValue := data.GetChange("null_if")

		ns := newValue.([]interface{})
		nulls := make([]string, len(ns))
		for i, n := range ns {
			if n == nil {
				nulls[i] = ""
			} else {
				nulls[i] = n.(string)
			}
		}

		if err := snowflake.Exec(db, builder.ChangeNullIf(nulls)); err != nil {
			return errors.Wrapf(err, "error updating file format null if on %v", data.Id())
		}

		data.SetPartial("null_if")
	}

	return ReadFileFormat(data, meta)
}

// DeleteFileFormat implements schema.DeleteFunc
func DeleteFileFormat(data *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	fileFormatID, err := fileFormatIDFromString(data.Id())
	if err != nil {
		return err
	}

	builder := snowflake.FileFormat(fileFormatID.FileFormatName, fileFormatID.DatabaseName, fileFormatID.SchemaName)

	if err := snowflake.Exec(db, builder.Drop()); err != nil {
		return errors.Wrapf(err, "error deleting file format %v", data.Id())
	}

	data.SetId("")

	return nil
}
