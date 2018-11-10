//
// This db2 dialect supports a large subset of gorm functionality sufficient for
// many applications.
//
// It doesnt support all of gorm and as such this is a work in progress.
//

package db2

import (
	"fmt"
	_ "github.com/asifjalil/cli"
	"github.com/jinzhu/gorm"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type db2 struct {
	// Implements the dialect interface
	gorm.Dialect
	db            gorm.SQLCommon
	currentSchema string
}

func (db2) GetName() string {
	return "cli"
}

func (db2) BindVar(i int) string {
	return "?"
}

func (s *db2) SetDB(db gorm.SQLCommon) {
	s.db = db
}

func (db2) Quote(key string) string {
	return fmt.Sprintf(`%s`, key)
}

func (s *db2) fieldCanAutoIncrement(field *gorm.StructField) bool {
	if value, ok := field.TagSettings["AUTO_INCREMENT"]; ok {
		return strings.ToLower(value) != "false"
	}
	return field.IsPrimaryKey
}

func (s *db2) DataTypeOf(field *gorm.StructField) string {
	var dataValue, sqlType, size, additionalType = gorm.ParseFieldStructForDialect(field, s)

	incrementSql := "GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)"
	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool:
			sqlType = "boolean"
		case reflect.Int8, reflect.Int16, reflect.Uint8:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettings["AUTO_INCREMENT"] = "AUTO_INCREMENT"
				sqlType = fmt.Sprintf("smallint %s", incrementSql)
			} else {
				sqlType = "smallint"
			}
		case reflect.Int, reflect.Int32, reflect.Uint16, reflect.Uintptr:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettings["AUTO_INCREMENT"] = "AUTO_INCREMENT"
				sqlType = fmt.Sprintf("integer %s", incrementSql)
			} else {
				sqlType = "integer"
			}
		case reflect.Uint, reflect.Int64, reflect.Uint64:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettings["AUTO_INCREMENT"] = "AUTO_INCREMENT"
				sqlType = fmt.Sprintf("bigint %s", incrementSql)
			} else {
				sqlType = "bigint"
			}
		case reflect.Float32:
			sqlType = "real"
		case reflect.Float64:
			sqlType = "double"
		case reflect.String:
			if _, ok := field.TagSettings["SIZE"]; !ok {
				// set default size to 0
				size = 64
			}
			switch {
			case size > 0 && size < 32673:
				sqlType = fmt.Sprintf("varchar(%d)", size)
			case size >= 32673:
				sqlType = "clob"
			}
		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				sqlType = "timestamp"
			}
		default:
			if gorm.IsByteArrayOrSlice(dataValue) {
				if size > 0 && size < 32673 {
					sqlType = fmt.Sprintf("varbinary(%d)", size)
				} else {
					sqlType = "blob"
				}
			}
		}
	}

	if sqlType == "" {
		panic(fmt.Sprintf("invalid sql type %s (%s) ", dataValue.Type().Name(), dataValue.Kind().String()))
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType)
}

func (s db2) RemoveIndex(tableName string, indexName string) error {
	// This should be done via migrations
	panic("remove index not implemented")
}

func (s db2) ModifyColumn(tableName string, columnName string, typ string) error {
	// This should be done via migrations
	panic("modify column not implemented")
}

// offset needs a limit first
func (s db2) LimitAndOffsetSQL(limit, offset interface{}) string {
	sql := ""
	if limit == nil {
		return sql
	}
	if parsedLimit, err := strconv.ParseInt(fmt.Sprint(limit), 0, 0); err == nil && parsedLimit >= 0 {
		sql += fmt.Sprintf(" LIMIT %d", parsedLimit)

		if offset != nil {
			if parsedOffset, err := strconv.ParseInt(fmt.Sprint(offset), 0, 0); err == nil && parsedOffset >= 0 {
				sql += fmt.Sprintf(" OFFSET %d", parsedOffset)
			}
		}
	}
	return sql
}

func (s *db2) CurrentDatabase() string {
	var name string
	if s.currentSchema == "" {
		if err := s.db.QueryRow("select current schema as name from sysibm.sysdummy1;;").Scan(&name); err != nil {
			panic("current database not found")
		}
		s.currentSchema = name
	}
	return s.currentSchema
}

func (s *db2) HasTable(tableName string) bool {
	var count int
	schema := s.CurrentDatabase()
	s.db.QueryRow("SELECT count(*) FROM sysibm.systables WHERE name = ? AND type = 'T' AND tabspace=?", tableName, schema).Scan(&count)
	return count > 0
}

func (s db2) HasColumn(tableName string, columnName string) bool {
	var count int
	s.db.QueryRow(`SELECT TABNAME
FROM SYSCAT.COLUMNS
WHERE 
    TABNAME = ? AND 
    COLNAME = ? AND TABSCHEMA = ?`, tableName, columnName, s.CurrentDatabase()).Scan(&count)
	return count > 0
}

// Select tbname ,pkcolnames,fkcolnames from sysibm.sysrels ;
func (s db2) HasForeignKey(tableName string, foreignKeyName string) bool {
	var count int
	s.db.QueryRow("select count(*) from sysibm.sysrels where tbname=? and strip(fkcolnames)=? and creator=?", tableName, foreignKeyName, s.CurrentDatabase()).Scan(&count)
	return count > 0
}

func (s db2) HasIndex(tableName string, indexName string) bool {
	panic("has index not implemented")
}

func (db2) SupportLastInsertID() bool {
	return false
}

func (s db2) LastInsertIDReturningSuffix(tableName, key string) string {
	return ""
}

func (db2) DefaultValueStr() string {
	panic("default value not implemented")
}

func (db2) SelectFromDummyTable() string {
	return "FROM DUAL"
}

func BuildKeyName(kind, tableName string, fields ...string) string {
	panic("build keyname not implemented")
}

func changeableField(scope *gorm.Scope, field *gorm.Field) bool {
	if selectAttrs := scope.SelectAttrs(); len(selectAttrs) > 0 {
		for _, attr := range selectAttrs {
			if field.Name == attr || field.DBName == attr {
				return true
			}
		}
		return false
	}

	for _, attr := range scope.OmitAttrs() {
		if field.Name == attr || field.DBName == attr {
			return false
		}
	}

	return true
}

func addExtraSpaceIfExist(str string) string {
	if str != "" {
		return " " + str
	}
	return ""
}

// Creation of a row in a table in DB2 has a slightly modified sql syntax to obtain
// the id of the new row. Hence the create callback is modified for that purpose
func CreateCallback(scope *gorm.Scope) {
	if !scope.HasError() {

		var (
			columns, placeholders        []string
			blankColumnsWithDefaultValue []string
		)

		for _, field := range scope.Fields() {
			if changeableField(scope, field) {
				if field.IsNormal {
					if field.IsBlank && field.HasDefaultValue {
						blankColumnsWithDefaultValue = append(blankColumnsWithDefaultValue, scope.Quote(field.DBName))
						scope.InstanceSet("gorm:blank_columns_with_default_value", blankColumnsWithDefaultValue)
					} else if !field.IsPrimaryKey || !field.IsBlank {
						columns = append(columns, scope.Quote(field.DBName))
						placeholders = append(placeholders, scope.AddToVars(field.Field.Interface()))
					}
				} else if field.Relationship != nil && field.Relationship.Kind == "belongs_to" {
					for _, foreignKey := range field.Relationship.ForeignDBNames {
						if foreignField, ok := scope.FieldByName(foreignKey); ok && !changeableField(scope, foreignField) {
							columns = append(columns, scope.Quote(foreignField.DBName))
							placeholders = append(placeholders, scope.AddToVars(foreignField.Field.Interface()))
						}
					}
				}
			}
		}

		var (
			returningColumn = "*"
			quotedTableName = scope.QuotedTableName()
			primaryField    = scope.PrimaryField()
			extraOption     string
		)

		if str, ok := scope.Get("gorm:insert_option"); ok {
			extraOption = fmt.Sprint(str)
		}

		if primaryField != nil {
			returningColumn = scope.Quote(primaryField.DBName)
		}

		lastInsertIDReturningSuffix := scope.Dialect().LastInsertIDReturningSuffix(quotedTableName, returningColumn)

		if len(columns) == 0 {
			scope.Raw(fmt.Sprintf(
				"INSERT INTO %v %v%v%v",
				quotedTableName,
				scope.Dialect().DefaultValueStr(),
				addExtraSpaceIfExist(extraOption),
				addExtraSpaceIfExist(lastInsertIDReturningSuffix),
			))
		} else {
			scope.Raw(fmt.Sprintf(
				"INSERT INTO %v (%v) VALUES (%v)%v%v",
				scope.QuotedTableName(),
				strings.Join(columns, ","),
				strings.Join(placeholders, ","),
				addExtraSpaceIfExist(extraOption),
				addExtraSpaceIfExist(lastInsertIDReturningSuffix),
			))
		}

		// print the SQL
		if primaryField != nil {
			scope.SQL = fmt.Sprintf("SELECT %s FROM FINAL TABLE (%s)", primaryField.DBName, scope.SQL)
		}
		fmt.Println(scope.SQL, scope.SQLVars)
		// execute create sql
		if err := scope.SQLDB().QueryRow(scope.SQL, scope.SQLVars...).Scan(primaryField.Field.Addr().Interface()); scope.Err(err) == nil {
			primaryField.IsBlank = false
			scope.DB().RowsAffected = 1
		}

	}
}

func BeginTransactionCallback(scope *gorm.Scope) {
}

func CommitOrRollbackTransactionCallback(scope *gorm.Scope) {
}

func init() {
	gorm.RegisterDialect("cli", &db2{})
}
