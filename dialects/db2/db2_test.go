//
// CRUD, DB Tests - Abstracted from the gorm tests
//

package db2

import (
	"database/sql"
	"fmt"
	_ "github.com/asifjalil/cli"
	"github.com/jinzhu/gorm"
	"os"
	"strconv"
	"testing"
	"time"
)

var (
	DB                 *gorm.DB
	t1, t2, t3, t4, t5 time.Time
)

type CreditCard struct {
	Id        int64     `gorm:"PRIMARY_KEY;column:ID;not null"`
	Number    string    `gorm:"column:NUMBER"`
	UserId    int64     `gorm:"column:USER_ID"`
	CreatedAt time.Time `gorm:"column:CREATEDAT" sql:"not null"`
	UpdatedAt time.Time `gorm:"column:UPDATEDAT"`
}

type Product struct {
	Id        int64     `gorm:"PRIMARY_KEY;column:ID;not null"`
	Code      string    `gorm:"column:CODE"`
	Price     int64     `gorm:"column:PRICE"`
	CreatedAt time.Time `gorm:"column:CREATEDAT"`
	UpdatedAt time.Time `gorm:"column:UPDATEDAT"`
}

type Email struct {
	Id        int64     `gorm:"PRIMARY_KEY;column:ID;not null"`
	Email     string    `gorm:"column:EMAIL"`
	UserId    int64     `gorm:"column:USER_ID"`
	CreatedAt time.Time `gorm:"column:CREATEDAT"`
	UpdatedAt time.Time `gorm:"column:UPDATEDAT"`
}

type User struct {
	Id           int64         `gorm:"PRIMARY_KEY;column:ID;not null"`
	Age          int64         `gorm:"column:AGE"`
	UserNum      int           `gorm:"column:USERNUM"`
	Name         string        `gorm:"column:NAME"`
	Email        string        `gorm:"column:EMAIL"`
	TestStr      string        `gorm:"column:TESTSTR"`
	Birthday     *time.Time    `gorm:"column:BIRTHDAY"`
	CreatedAt    time.Time     `gorm:"column:CREATEDAT"`
	UpdatedAt    time.Time     `gorm:"column:UPDATEDAT"`
	PasswordHash string        `gorm:"column:PASSWORDHASH"`
	Latitude     float64       `gorm:"column:LATITUDE"`
	ForeignKey   sql.NullInt64 `gorm:"column:FOREIGNKEY"`
	CreditCard   CreditCard
	Emails       []Email
}

type ElementWithIgnoredField struct {
	Id           int64  `gorm:"PRIMARY_KEY;column:ID;not null"`
	Value        string `gorm:"column:VALUE"`
	IgnoredField int64  `sql:"-"`
}

func (e ElementWithIgnoredField) TableName() string {
	return "element_with_ignored_field"
}

type EmailWithIdx struct {
	Id           int64      `gorm:"PRIMARY_KEY;column:ID;not null"`
	UserId       int64      `gorm:"column:USER_ID"`
	Email        string     `gorm:"column:EMAIL"`
	UserAgent    string     `gorm:"column:USERAGENT"`
	RegisteredAt *time.Time `gorm:"column:REGISTEREDAT"`
	CreatedAt    time.Time  `gorm:"column:CREATEDAT"`
	UpdatedAt    time.Time  `gorm:"column:UPDATEDAT"`
}

func TestStringPrimaryKey(t *testing.T) {
	type UUIDStruct struct {
		Id   string `gorm:"primary_key;column:ID;not null"`
		Name string `gorm:"column:NAME"`
	}
	DB.DropTable(&UUIDStruct{})
	DB.AutoMigrate(&UUIDStruct{})

	data := UUIDStruct{Id: "uuid", Name: "hello"}
	if err := DB.Create(&data).Error; err != nil || data.Id != "uuid" || data.Name != "hello" {
		t.Errorf("string primary key should not be populated")
	}

	data = UUIDStruct{Id: "uuid", Name: "hello world"}
	if err := DB.Save(&data).Error; err != nil || data.Id != "uuid" || data.Name != "hello world" {
		t.Errorf("string primary key should not be populated")
	}
}

func TestExceptionsWithInvalidSql(t *testing.T) {
	var columns []string
	if DB.Where("sdsd.zaaa = ?", "sd;;;aa").Pluck("aaa", &columns).Error == nil {
		t.Errorf("Should got error with invalid SQL")
	}

	if DB.Model(&User{}).Where("sdsd.zaaa = ?", "sd;;;aa").Pluck("aaa", &columns).Error == nil {
		t.Errorf("Should got error with invalid SQL")
	}

	if DB.Where("sdsd.zaaa = ?", "sd;;;aa").Find(&User{}).Error == nil {
		t.Errorf("Should got error with invalid SQL")
	}

	var count1, count2 int64
	DB.Create(&User{Name: "test1"})
	DB.Model(&User{}).Count(&count1)
	if count1 <= 0 {
		t.Errorf("Should find some users")
	}

	if DB.Where("name = ?", "jinzhu; delete * from users").First(&User{}).Error == nil {
		t.Errorf("Should got error with invalid SQL")
	}

	DB.Model(&User{}).Count(&count2)
	if count1 != count2 {
		t.Errorf("No user should not be deleted by invalid SQL")
	}
}

func TestDbTransaction(t *testing.T) {
	tx := DB.Begin()
	u := User{Name: "transcation"}
	if err := tx.Save(&u).Error; err != nil {
		t.Errorf("No error should raise")
	}

	if err := tx.First(&User{}, "name = ?", "transcation").Error; err != nil {
		t.Errorf("Should find saved record")
	}

	if sqlTx, ok := tx.CommonDB().(*sql.Tx); !ok || sqlTx == nil {
		t.Errorf("Should return the underlying sql.Tx")
	}

	tx.Rollback()

	if err := tx.First(&User{}, "name = ?", "transcation").Error; err == nil {
		t.Errorf("Should not find record after rollback")
	}

	tx2 := DB.Begin()
	u2 := User{Name: "transcation-2"}
	if err := tx2.Save(&u2).Error; err != nil {
		t.Errorf("No error should raise")
	}

	if err := tx2.First(&User{}, "name = ?", "transcation-2").Error; err != nil {
		t.Errorf("Should find saved record")
	}

	tx2.Commit()

	if err := DB.First(&User{}, "name = ?", "transcation-2").Error; err != nil {
		t.Errorf("Should be able to find committed record")
	}
}

func TestDbJoins(t *testing.T) {
	var user = User{
		Name:       "joins",
		CreditCard: CreditCard{Number: "411111111111"},
		Emails:     []Email{{Email: "join1@example.com"}, {Email: "join2@example.com"}},
	}
	DB.Save(&user)

	var users1 []User
	DB.Joins("left join emails on emails.user_id = users.id").Where("name = ?", "joins").Find(&users1)
	if len(users1) != 2 {
		t.Errorf("should find two users using left join")
	}

	var users2 []User
	DB.Joins("left join emails on emails.user_id = users.id AND emails.email = ?", "join1@example.com").Where("name = ?", "joins").First(&users2)
	if len(users2) != 1 {
		t.Errorf("should find one users using left join with conditions")
	}

	var users3 []User
	DB.Joins("join emails on emails.user_id = users.id AND emails.email = ?", "join1@example.com").Joins("join credit_cards on credit_cards.user_id = users.id AND credit_cards.number = ?", "411111111111").Where("name = ?", "joins").First(&users3)
	if len(users3) != 1 {
		t.Errorf("should find one users using multiple left join conditions")
	}

	var users4 []User
	DB.Joins("join emails on emails.user_id = users.id AND emails.email = ?", "join1@example.com").Joins("join credit_cards on credit_cards.user_id = users.id AND credit_cards.number = ?", "422222222222").Where("name = ?", "joins").First(&users4)
	if len(users4) != 0 {
		t.Errorf("should find no user when searching with unexisting credit card")
	}

	var users5 []User
	db5 := DB.Joins("join emails on emails.user_id = users.id AND emails.email = ?", "join1@example.com").Joins("join credit_cards on credit_cards.user_id = users.id AND credit_cards.number = ?", "411111111111").Where(User{Id: 1}).Where(Email{Id: 1}).Not(Email{Id: 10}).First(&users5)
	if db5.Error != nil {
		t.Errorf("Should not raise error for join where identical fields in different tables. Error: %s", db5.Error.Error())
	}
}

func TestDriverBug2(t *testing.T) {
	var emptyString string
	err := DB.DB().QueryRow("SELECT '' FROM sysibm.sysdummy1").Scan(&emptyString)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		if emptyString != "" {
			fmt.Println("bug", emptyString, len(emptyString), []byte(emptyString))
		} else {
			fmt.Println("not a bug")
		}
	}
}

func TestDriverBug(t *testing.T) {
	type TestStruct struct {
		Id   int    `gorm:"primary_key;column:ID;not null"`
		Name string `gorm:"column:NAME"`
		Bug  string `gorm:"column:BUG"`
	}
	DB.DropTable(&TestStruct{})
	DB.AutoMigrate(&TestStruct{})
	s := TestStruct{Name: "test"}
	DB.Create(&s)
	f := TestStruct{}
	DB.Where("id = ?", 1).First(&f)
	fmt.Println("found", f)
	if f.Bug == "" {
		fmt.Println("not a bug")
	} else {
		fmt.Println("a bug")
	}
	rows, _ := DB.Raw("select * from test_structs").Rows()
	defer rows.Close()
	for rows.Next() {
		var name, bug string
		var id int
		rows.Scan(&id, &name, &bug)
		if bug == "" {
			fmt.Println("Ok", len(bug), []byte(bug))
		} else {
			fmt.Println("found bug", len(bug), []byte(bug))
		}
	}
}

func TestDbHaving(t *testing.T) {
	rows, err := DB.Select("name, count(*) as total").Table("users").Group("name").Having("name IN (?)", []string{"2", "3"}).Rows()

	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var name string
			var total int64
			rows.Scan(&name, &total)

			if name == "2" && total != 1 {
				t.Errorf("Should have one user having name 2")
			}
			if name == "3" && total != 2 {
				t.Errorf("Should have two users having name 3")
			}
		}
	} else {
		t.Errorf("Should not raise any error")
	}
}

func TestDbQueryBuilderSubselectInWhere(t *testing.T) {
	user := User{Name: "query_expr_select_ruser1", Email: "root@user1.com", Age: 32}
	DB.Save(&user)
	user = User{Name: "query_expr_select_ruser2", Email: "nobody@user2.com", Age: 16}
	DB.Save(&user)
	user = User{Name: "query_expr_select_ruser3", Email: "root@user3.com", Age: 64}
	DB.Save(&user)
	user = User{Name: "query_expr_select_ruser4", Email: "somebody@user3.com", Age: 128}
	DB.Save(&user)

	var users []User
	DB.Select("*").Where("name IN (?)", DB.
		Select("name").Table("users").Where("name LIKE ?", "query_expr_select%").QueryExpr()).Find(&users)

	if len(users) != 4 {
		t.Errorf("Four users should be found, instead found %d", len(users))
	}

	DB.Select("*").Where("name LIKE ?", "query_expr_select%").Where("age >= (?)", DB.
		Select("AVG(age)").Table("users").Where("name LIKE ?", "query_expr_select%").QueryExpr()).Find(&users)

	if len(users) != 2 {
		t.Errorf("Two users should be found, instead found %d", len(users))
	}
}

func TestDbQueryBuilderRawQueryWithSubquery(t *testing.T) {
	user := User{Name: "subquery_test_user1", Age: 10}
	DB.Save(&user)
	user = User{Name: "subquery_test_user2", Age: 11}
	DB.Save(&user)
	user = User{Name: "subquery_test_user3", Age: 12}
	DB.Save(&user)

	var count int
	err := DB.Raw("select count(*) from (?) tmp",
		DB.Table("users").
			Select("name").
			Where("age >= ? and name in (?)", 10, []string{"subquery_test_user1", "subquery_test_user2"}).
			Group("name").
			QueryExpr(),
	).Count(&count).Error

	if err != nil {
		t.Errorf("Expected to get no errors, but got %v", err)
	}
	if count != 2 {
		t.Errorf("Row count must be 2, instead got %d", count)
	}

	err = DB.Raw("select count(*) from (?) tmp",
		DB.Table("users").
			Select("name").
			Where("name LIKE ?", "subquery_test%").
			Not("age <= ?", 10).Not("name in (?)", []string{"subquery_test_user1", "subquery_test_user2"}).
			Group("name").
			QueryExpr(),
	).Count(&count).Error

	if err != nil {
		t.Errorf("Expected to get no errors, but got %v", err)
	}
	if count != 1 {
		t.Errorf("Row count must be 1, instead got %d", count)
	}
}

func TestDbQueryBuilderSubselectInHaving(t *testing.T) {
	user := User{Name: "query_expr_having_ruser1", Email: "root@user1.com", Age: 64}
	DB.Save(&user)
	user = User{Name: "query_expr_having_ruser2", Email: "root@user2.com", Age: 128}
	DB.Save(&user)
	user = User{Name: "query_expr_having_ruser3", Email: "root@user1.com", Age: 64}
	DB.Save(&user)
	user = User{Name: "query_expr_having_ruser4", Email: "root@user2.com", Age: 128}
	DB.Save(&user)

	var users []User
	DB.Select("AVG(age) as avgage").Where("name LIKE ?", "query_expr_having_%").Group("email").Having("AVG(age) > (?)", DB.
		Select("AVG(age)").Where("name LIKE ?", "query_expr_having_%").Table("users").QueryExpr()).Find(&users)

	if len(users) != 1 {
		t.Errorf("Two user group should be found, instead found %d", len(users))
	}
}

func BenchmarkGorm(b *testing.B) {
	b.N = 10
	for x := 0; x < b.N; x++ {
		e := strconv.Itoa(x) + "benchmark@example.org"
		now := time.Now()
		email := EmailWithIdx{Email: e, UserAgent: "pc", RegisteredAt: &now}
		// Insert
		DB.Save(&email)
		// Query
		DB.First(&EmailWithIdx{}, "email = ?", e)
		// Update
		DB.Model(&email).UpdateColumn("email", "new-"+e)
		// Delete
		DB.Delete(&email)
	}
}

func BenchmarkRawSql(b *testing.B) {
	insertSql := "SELECT ID FROM FINAL TABLE (INSERT INTO email_with_idxes (user_id,email,useragent,registeredat,createdat,updatedat) VALUES (?,?,?,?,?,?))"
	querySql := "SELECT * FROM email_with_idxes WHERE email = ? ORDER BY id LIMIT 1"
	updateSql := "UPDATE email_with_idxes SET email = ?, updatedat = ? WHERE id = ?"
	deleteSql := "DELETE FROM email_with_idxes WHERE id = ?"

	b.N = 10
	for x := 0; x < b.N; x++ {
		var id int64
		e := strconv.Itoa(x) + "benchmark@example.org"
		now := time.Now()
		email := EmailWithIdx{Email: e, UserAgent: "pc", RegisteredAt: &now}
		// Insert
		DB.DB().QueryRow(insertSql, email.UserId, email.Email, email.UserAgent, email.RegisteredAt, time.Now(), time.Now()).Scan(&id)
		// Query
		rows, _ := DB.DB().Query(querySql, email.Email)
		rows.Close()
		// Update
		DB.DB().Exec(updateSql, "new-"+e, time.Now(), id)
		// Delete
		DB.DB().Exec(deleteSql, id)
	}
}

func init() {
	dbDSN := os.Getenv("GORM_DSN")
	if dbDSN == "" {
		fmt.Println(`please set the env variable GORM_DSN="database dsn"`)
		os.Exit(1)
	}
	var err error
	DB, err = gorm.Open("cli", dbDSN)
	if err != nil {
		panic("failed to open db")
	}
	fmt.Println("Connected to DB..")
	DB.LogMode(true)
	DB.DB().SetMaxIdleConns(10)
	DB.Callback().Create().Replace("gorm:create", CreateCallback)
	// Let application control transactions
	DB.Callback().Create().Replace("gorm:begin_transaction", BeginTransactionCallback)
	DB.Callback().Create().Replace("gorm:commit_or_rollback_transaction", CommitOrRollbackTransactionCallback)
	DB.Callback().Update().Replace("gorm:begin_transaction", BeginTransactionCallback)
	DB.Callback().Update().Replace("gorm:commit_or_rollback_transaction", CommitOrRollbackTransactionCallback)
	DB.Callback().Delete().Replace("gorm:begin_transaction", BeginTransactionCallback)
	DB.Callback().Delete().Replace("gorm:commit_or_rollback_transaction", CommitOrRollbackTransactionCallback)
	DB.DropTable(&User{})
	DB.DropTable(&Product{})
	DB.DropTable(&Email{})
	DB.DropTable(&CreditCard{})
	DB.DropTable(&ElementWithIgnoredField{})
	DB.DropTable(&EmailWithIdx{})
	DB.AutoMigrate(
		&Product{},
		&User{},
		&Email{},
		&CreditCard{},
		&ElementWithIgnoredField{},
		&EmailWithIdx{},
	)
}
