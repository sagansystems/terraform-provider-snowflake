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
			case "csv", "parquet":
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
		Computed:    true,
		Description: "Specifies the current compression algorithm for columns in the Parquet files.",
		ValidateFunc: func(val interface{}, _ string) ([]string, []error) {
			c := strings.ToLower(val.(string))

			switch c {
			case "auto", "brotli", "bz2", "deflate", "gzip", "lzo", "raw_deflate", "snappy", "zstd", "none":
				return nil, nil
			default:
				return nil, []error{fmt.Errorf("%s is not a supported compression algorithm", val)}
			}
		},
	},
	"binary_as_text": {
		Type:        schema.TypeBool,
		Optional:    true,
		Computed:    true,
		Description: "Boolean that specifies whether to interpret columns with no defined logical data type as UTF-8 text. When set to FALSE, Snowflake interprets these columns as binary data.",
	},
	"trim_space": {
		Type:        schema.TypeBool,
		Optional:    true,
		Computed:    true,
		Description: "Applied only when loading Parquet data into separate columns (i.e. using the MATCH_BY_COLUMN_NAME copy option or a COPY transformation). Boolean that specifies whether to remove leading and trailing white space from strings.",
	},
	"null_if": {
		Type:        schema.TypeList,
		Elem:        &schema.Schema{Type: schema.TypeString},
		Optional:    true,
		Computed:    true,
		Description: "Applied only when loading Parquet data into separate columns (i.e. using the MATCH_BY_COLUMN_NAME copy option or a COPY transformation). String used to convert to and from SQL NULL. Snowflake replaces these strings in the data load source with SQL NULL.",
	},
	"record_delimiter": {
		Type:        schema.TypeString,
		Optional:    true,
		Computed:    true,
		Description: "One or more singlebyte or multibyte characters that separate records in an input file (data loading) or unloaded file (data unloading).",
	},
	"field_delimiter": {
		Type:        schema.TypeString,
		Optional:    true,
		Computed:    true,
		Description: "One or more singlebyte or multibyte characters that separate fields in an input file (data loading) or unloaded file (data unloading).",
	},
	"file_extension": {
		Type:        schema.TypeString,
		Optional:    true,
		Computed:    true,
		Description: "Specifies the extension for files unloaded to a stage.",
	},
	"skip_header": {
		Type:        schema.TypeInt,
		Optional:    true,
		Computed:    true,
		Description: "Number of lines at the start of the file to skip.",
	},
	"skip_blank_lines": {
		Type:        schema.TypeBool,
		Optional:    true,
		Computed:    true,
		Description: "Boolean that specifies to skip any blank lines encountered in the data files.",
	},
	"date_format": {
		Type:        schema.TypeString,
		Optional:    true,
		Computed:    true,
		Description: "Defines the format of date values in the data files (data loading) or table (data unloading).",
	},
	"time_format": {
		Type:        schema.TypeString,
		Optional:    true,
		Computed:    true,
		Description: "Defines the format of time values in the data files (data loading) or table (data unloading).",
	},
	"timestamp_format": {
		Type:        schema.TypeString,
		Optional:    true,
		Computed:    true,
		Description: "Defines the format of timestamp values in the data files (data loading) or table (data unloading).",
	},
	"binary_format": {
		Type:        schema.TypeString,
		Optional:    true,
		Computed:    true,
		Description: "Defines the encoding format for binary input or output.",
		ValidateFunc: func(val interface{}, _ string) ([]string, []error) {
			c := strings.ToLower(val.(string))

			switch c {
			case "hex", "base64", "utf":
				return nil, nil
			default:
				return nil, []error{fmt.Errorf("%s is not a supported binary format", val)}
			}
		},
	},
	"escape": {
		Type:        schema.TypeString,
		Optional:    true,
		Computed:    true,
		Description: "Single character string used as the escape character for field values.",
	},
	"escape_unenclosed_field": {
		Type:        schema.TypeString,
		Optional:    true,
		Computed:    true,
		Description: "Single character string used as the escape character for unenclosed field values only.",
	},
	"field_optionally_enclosed_by": {
		Type:        schema.TypeString,
		Optional:    true,
		Computed:    true,
		Description: "Character used to enclose strings.",
	},
	"error_on_column_count_mismatch": {
		Type:        schema.TypeBool,
		Optional:    true,
		Computed:    true,
		Description: "Boolean that specifies whether to generate a parsing error if the number of delimited columns (i.e. fields) in an input file does not match the number of columns in the corresponding table.",
	},
	"replace_invalid_characters": {
		Type:        schema.TypeBool,
		Optional:    true,
		Computed:    true,
		Description: "Boolean that specifies whether to replace invalid UTF-8 characters with the Unicode replacement character.",
	},
	"validate_utf8": {
		Type:        schema.TypeBool,
		Optional:    true,
		Computed:    true,
		Description: "Boolean that specifies whether to validate UTF-8 character encoding in string column data.",
	},
	"empty_field_as_null": {
		Type:        schema.TypeBool,
		Optional:    true,
		Computed:    true,
		Description: "When loading data, specifies whether to insert SQL NULL for empty fields in an input file, which are represented by two successive delimiters (e.g. ,,). When unloading data, this option is used in combination with FIELD_OPTIONALLY_ENCLOSED_BY. When FIELD_OPTIONALLY_ENCLOSED_BY = NONE, setting EMPTY_FIELD_AS_NULL = FALSE specifies to unload empty strings in tables to empty string values without quotes enclosing the field values. If set to TRUE, FIELD_OPTIONALLY_ENCLOSED_BY must specify a character to enclose strings.",
	},
	"skip_byte_order_mark": {
		Type:        schema.TypeBool,
		Optional:    true,
		Computed:    true,
		Description: "Boolean that specifies whether to skip the BOM (byte order mark), if present in a data file.",
	},
	"encoding": {
		Type:        schema.TypeString,
		Optional:    true,
		Computed:    true,
		Description: "String (constant) that specifies the character set of the source data when loading data into a table.",
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

	if v, ok := data.GetOk("record_delimiter"); ok {
		builder.WithRecordDelimiter(v.(string))
	}

	if v, ok := data.GetOk("field_delimiter"); ok {
		builder.WithFieldDelimiter(v.(string))
	}

	if v, ok := data.GetOk("file_extension"); ok {
		builder.WithFileExtension(v.(string))
	}

	if v, ok := data.GetOk("skip_header"); ok {
		builder.WithSkipHeader(v.(int))
	}

	if v, ok := data.GetOk("skip_blank_lines"); ok {
		builder.WithSkipBlankLines(v.(bool))
	}

	if v, ok := data.GetOk("date_format"); ok {
		builder.WithDateFormat(v.(string))
	}

	if v, ok := data.GetOk("time_format"); ok {
		builder.WithTimeFormat(v.(string))
	}

	if v, ok := data.GetOk("timestamp_format"); ok {
		builder.WithTimestampFormat(v.(string))
	}

	if v, ok := data.GetOk("binary_format"); ok {
		builder.WithBinaryFormat(v.(string))
	}

	if v, ok := data.GetOk("escape"); ok {
		builder.WithEscape(v.(string))
	}

	if v, ok := data.GetOk("escape_unenclosed_field"); ok {
		builder.WithEscapeUnenclosedField(v.(string))
	}

	if v, ok := data.GetOk("field_optionally_enclosed_by"); ok {
		builder.WithFieldOptionallyEnclosedBy(v.(string))
	}

	if v, ok := data.GetOk("error_on_column_count_mismatch"); ok {
		builder.WithErrorOnColumnCountMismatch(v.(bool))
	}

	if v, ok := data.GetOk("replace_invalid_characters"); ok {
		builder.WithReplaceInvalidCharacters(v.(bool))
	}

	if v, ok := data.GetOk("validate_utf8"); ok {
		builder.WithValidateUtf8(v.(bool))
	}

	if v, ok := data.GetOk("empty_field_as_null"); ok {
		builder.WithEmptyFieldAsNull(v.(bool))
	}

	if v, ok := data.GetOk("skip_byte_order_mark"); ok {
		builder.WithSkipByteOrderMark(v.(bool))
	}

	if v, ok := data.GetOk("encoding"); ok {
		builder.WithEncoding(v.(string))
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

	if err := data.Set("record_delimiter", ffData.RecordDelimiter); err != nil {
		return err
	}

	if err := data.Set("field_delimiter", ffData.FieldDelimiter); err != nil {
		return err
	}

	if err := data.Set("file_extension", ffData.FileExtension); err != nil {
		return err
	}

	if err := data.Set("skip_header", ffData.SkipHeader); err != nil {
		return err
	}

	if err := data.Set("skip_blank_lines", ffData.SkipBlankLines); err != nil {
		return err
	}

	if err := data.Set("date_format", ffData.DateFormat); err != nil {
		return err
	}

	if err := data.Set("time_format", ffData.TimeFormat); err != nil {
		return err
	}

	if err := data.Set("timestamp_format", ffData.TimestampFormat); err != nil {
		return err
	}

	if err := data.Set("binary_format", ffData.BinaryFormat); err != nil {
		return err
	}

	if err := data.Set("escape", ffData.Escape); err != nil {
		return err
	}

	if err := data.Set("escape_unenclosed_field", ffData.EscapeUnenclosedField); err != nil {
		return err
	}

	if err := data.Set("field_optionally_enclosed_by", ffData.FieldOptionallyEnclosedBy); err != nil {
		return err
	}

	if err := data.Set("error_on_column_count_mismatch", ffData.ErrorOnColumnCountMismatch); err != nil {
		return err
	}

	if err := data.Set("replace_invalid_characters", ffData.ReplaceInvalidCharacters); err != nil {
		return err
	}

	if err := data.Set("validate_utf8", ffData.ValidateUtf8); err != nil {
		return err
	}

	if err := data.Set("empty_field_as_null", ffData.EmptyFieldAsNull); err != nil {
		return err
	}

	if err := data.Set("skip_byte_order_mark", ffData.SkipByteOrderMark); err != nil {
		return err
	}

	if err := data.Set("encoding", ffData.Encoding); err != nil {
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

	if data.HasChange("record_delimiter") {
		_, recordDelimiter := data.GetChange("record_delimiter")
		if err := snowflake.Exec(db, builder.ChangeRecordDelimiter(recordDelimiter.(string))); err != nil {
			return errors.Wrapf(err, "error updating file format record_delimiter on %v", data.Id())
		}

		data.SetPartial("record_delimiter")
	}

	if data.HasChange("field_delimiter") {
		_, fieldDelimiter := data.GetChange("field_delimiter")
		if err := snowflake.Exec(db, builder.ChangeFieldDelimiter(fieldDelimiter.(string))); err != nil {
			return errors.Wrapf(err, "error updating file format field_delimiter on %v", data.Id())
		}

		data.SetPartial("field_delimiter")
	}

	if data.HasChange("file_extension") {
		_, fileExtension := data.GetChange("file_extension")
		if err := snowflake.Exec(db, builder.ChangeFileExtension(fileExtension.(string))); err != nil {
			return errors.Wrapf(err, "error updating file format file_extension on %v", data.Id())
		}

		data.SetPartial("file_extension")
	}

	if data.HasChange("skip_header") {
		_, skipHeader := data.GetChange("skip_header")
		if err := snowflake.Exec(db, builder.ChangeSkipHeader(skipHeader.(int))); err != nil {
			return errors.Wrapf(err, "error updating file format skip_header on %v", data.Id())
		}

		data.SetPartial("skip_header")
	}

	if data.HasChange("skip_blank_lines") {
		_, skipBlankLines := data.GetChange("skip_blank_lines")
		if err := snowflake.Exec(db, builder.ChangeSkipBlankLines(skipBlankLines.(bool))); err != nil {
			return errors.Wrapf(err, "error updating file format skip_blank_lines on %v", data.Id())
		}

		data.SetPartial("skip_blank_lines")
	}

	if data.HasChange("date_format") {
		_, dateFormat := data.GetChange("date_format")
		if err := snowflake.Exec(db, builder.ChangeDateFormat(dateFormat.(string))); err != nil {
			return errors.Wrapf(err, "error updating file format date_format on %v", data.Id())
		}

		data.SetPartial("date_format")
	}

	if data.HasChange("time_format") {
		_, timeFormat := data.GetChange("time_format")
		if err := snowflake.Exec(db, builder.ChangeTimeFormat(timeFormat.(string))); err != nil {
			return errors.Wrapf(err, "error updating file format time_format on %v", data.Id())
		}

		data.SetPartial("time_format")
	}

	if data.HasChange("timestamp_format") {
		_, timestampFormat := data.GetChange("timestamp_format")
		if err := snowflake.Exec(db, builder.ChangeTimestampFormat(timestampFormat.(string))); err != nil {
			return errors.Wrapf(err, "error updating file format timestamp_format on %v", data.Id())
		}

		data.SetPartial("timestamp_format")
	}

	if data.HasChange("binary_format") {
		_, binaryFormat := data.GetChange("binary_format")
		if err := snowflake.Exec(db, builder.ChangeBinaryFormat(binaryFormat.(string))); err != nil {
			return errors.Wrapf(err, "error updating file format binary_format on %v", data.Id())
		}

		data.SetPartial("binary_format")
	}

	if data.HasChange("escape") {
		_, escape := data.GetChange("escape")
		if err := snowflake.Exec(db, builder.ChangeEscape(escape.(string))); err != nil {
			return errors.Wrapf(err, "error updating file format escape on %v", data.Id())
		}

		data.SetPartial("escape")
	}

	if data.HasChange("escape_unenclosed_field") {
		_, escapeUnenclosedField := data.GetChange("escape_unenclosed_field")
		if err := snowflake.Exec(db, builder.ChangeEscapeUnenclosedField(escapeUnenclosedField.(string))); err != nil {
			return errors.Wrapf(err, "error updating file format escape_unenclosed_field on %v", data.Id())
		}

		data.SetPartial("escape_unenclosed_field")
	}

	if data.HasChange("field_optionally_enclosed_by") {
		_, fieldOptionallyEnclosedBy := data.GetChange("field_optionally_enclosed_by")
		if err := snowflake.Exec(db, builder.ChangeFieldOptionallyEnclosedBy(fieldOptionallyEnclosedBy.(string))); err != nil {
			return errors.Wrapf(err, "error updating file format field_optionally_enclosed_by on %v", data.Id())
		}

		data.SetPartial("field_optionally_enclosed_by")
	}

	if data.HasChange("error_on_column_count_mismatch") {
		_, errorOnColumnCountMismatch := data.GetChange("error_on_column_count_mismatch")
		if err := snowflake.Exec(db, builder.ChangeErrorOnColumnCountMismatch(errorOnColumnCountMismatch.(bool))); err != nil {
			return errors.Wrapf(err, "error updating file format error_on_column_count_mismatch on %v", data.Id())
		}

		data.SetPartial("error_on_column_count_mismatch")
	}

	if data.HasChange("replace_invalid_characters") {
		_, replaceInvalidCharacters := data.GetChange("replace_invalid_characters")
		if err := snowflake.Exec(db, builder.ChangeReplaceInvalidCharacters(replaceInvalidCharacters.(bool))); err != nil {
			return errors.Wrapf(err, "error updating file format replace_invalid_characters on %v", data.Id())
		}

		data.SetPartial("replace_invalid_characters")
	}

	if data.HasChange("validate_utf8") {
		_, validateUtf8 := data.GetChange("validate_utf8")
		if err := snowflake.Exec(db, builder.ChangeValidateUtf8(validateUtf8.(bool))); err != nil {
			return errors.Wrapf(err, "error updating file format validate_utf8 on %v", data.Id())
		}

		data.SetPartial("validate_utf8")
	}

	if data.HasChange("empty_field_as_null") {
		_, emptyFieldAsNull := data.GetChange("empty_field_as_null")
		if err := snowflake.Exec(db, builder.ChangeEmptyFieldAsNull(emptyFieldAsNull.(bool))); err != nil {
			return errors.Wrapf(err, "error updating file format empty_field_as_null on %v", data.Id())
		}

		data.SetPartial("empty_field_as_null")
	}

	if data.HasChange("skip_byte_order_mark") {
		_, skipByteOrderMark := data.GetChange("skip_byte_order_mark")
		if err := snowflake.Exec(db, builder.ChangeSkipByteOrderMark(skipByteOrderMark.(bool))); err != nil {
			return errors.Wrapf(err, "error updating file format skip_byte_order_mark on %v", data.Id())
		}

		data.SetPartial("skip_byte_order_mark")
	}

	if data.HasChange("encoding") {
		_, encoding := data.GetChange("encoding")
		if err := snowflake.Exec(db, builder.ChangeEncoding(encoding.(string))); err != nil {
			return errors.Wrapf(err, "error updating file format encoding on %v", data.Id())
		}

		data.SetPartial("encoding")
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
