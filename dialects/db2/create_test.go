//
// Create Tests - Abstracted from the gorm tests
//

package db2

import (
	"fmt"
	"github.com/jinzhu/now"
	"reflect"
	"testing"
	"time"
)

func TestCreate(t *testing.T) {
	float := 35.03554004971999
	now := time.Now()
	user := User{Name: "CreateUser", Age: 18, Birthday: &now, UserNum: 111, PasswordHash: "fak4", Latitude: float}

	if !DB.NewRecord(user) || !DB.NewRecord(&user) {
		t.Error("User should be new record before create")
	}

	if count := DB.Save(&user).RowsAffected; count != 1 {
		t.Error("There should be one record be affected when create record")
	}

	if DB.NewRecord(user) || DB.NewRecord(&user) {
		t.Error("User should not new record after save")
	}

	var newUser User
	if err := DB.First(&newUser, user.Id).Error; err != nil {
		t.Errorf("No error should happen, but got %v", err)
	}

	if !reflect.DeepEqual(newUser.PasswordHash, "fak4") {
		t.Errorf("User's PasswordHash should be saved (string)")
	}

	if newUser.Age != 18 {
		t.Errorf("User's Age should be saved (int)")
	}

	if newUser.UserNum != 111 {
		t.Errorf("User's UserNum should be saved (custom type), but got %v", newUser.UserNum)
	}

	if newUser.Latitude != float {
		t.Errorf("Float64 should not be changed after save")
	}

	if user.CreatedAt.IsZero() {
		t.Errorf("Should have created_at after create")
	}

	if newUser.CreatedAt.IsZero() {
		t.Errorf("Should have created_at after create")
	}

	if newUser.ForeignKey.Valid {
		t.Errorf("Foreign key should have been null")
	}

	DB.Model(user).Update("name", "create_user_new_name")
	DB.First(&user, user.Id)
	if user.CreatedAt.Format(time.RFC3339Nano) != newUser.CreatedAt.Format(time.RFC3339Nano) {
		t.Errorf("CreatedAt should not be changed after update")
	}
}

func TestCreateWithExistingTimestamp(t *testing.T) {
	user := User{Name: "CreateUserExistingTimestamp"}

	timeA := now.MustParse("2016-01-01")
	user.CreatedAt = timeA
	user.UpdatedAt = timeA
	DB.Save(&user)

	if user.CreatedAt.UTC().Format(time.RFC3339) != timeA.UTC().Format(time.RFC3339) {
		t.Errorf("CreatedAt should not be changed")
	}

	if user.UpdatedAt.UTC().Format(time.RFC3339) != timeA.UTC().Format(time.RFC3339) {
		t.Errorf("UpdatedAt should not be changed")
	}

	var newUser User
	DB.First(&newUser, user.Id)

	if newUser.CreatedAt.UTC().Format(time.RFC3339) != timeA.UTC().Format(time.RFC3339) {
		t.Errorf("CreatedAt should not be changed")
	}

	if newUser.UpdatedAt.UTC().Format(time.RFC3339) != timeA.UTC().Format(time.RFC3339) {
		t.Errorf("UpdatedAt should not be changed")
	}
}

func TestCRUDWithNullValues(t *testing.T) {
	type NullTest struct {
		Id    string  `gorm:"primary_key;column:ID;not null"`
		NameP *string `gorm:"column:NAMEP"`
		Name  string  `gorm:"column:NAME"`
		IntP  *int    `gorm:"column:INTP"`
		Int   int     `gorm:"column:INT"`
	}
	DB.DropTable(&NullTest{})
	DB.AutoMigrate(&NullTest{})

	data := NullTest{Id: "nulltest"}

	if err := DB.Create(&data).Error; err != nil || data.Id != "nulltest" {
		t.Errorf("create failed " + err.Error())
		return
	}

	r := NullTest{}
	err := DB.Where("ID = ?", "nulltest").First(&r).Error
	if err != nil || r.Id != "nulltest" {
		t.Errorf("read failed: %v", r)
	}
	fmt.Printf("after create read %v", r)
	if r.NameP != nil {
		t.Errorf("expected null NameP")
	}
	if r.IntP != nil {
		t.Errorf("expected null IntP")
	}
	if r.Int != 0 {
		t.Errorf("expected 0 Int")
	}
	if r.Name != "" {
		t.Errorf("expected nullstring Name")
	}
	s := ""
	i := 0
	r.NameP = &s
	r.IntP = &i

	if err := DB.Save(&r).Error; err != nil {
		t.Errorf("save failed " + err.Error())
		return
	}
	err = DB.Where("ID = ?", "nulltest").First(&r).Error
	fmt.Printf("after  update read %v", r)
	if err != nil || r.Id != "nulltest" {
		t.Errorf("read failed: %v", r)
	}
	if *r.NameP != "" {
		t.Errorf("expected nullstring NameP")
	}
	if *r.IntP != 0 {
		t.Errorf("expected 0 IntP")
	}

	// set back to null
	r.NameP = nil
	r.IntP = nil

	if err := DB.Save(&r).Error; err != nil {
		t.Errorf("save failed " + err.Error())
		return
	}
	err = DB.Where("ID = ?", "nulltest").First(&r).Error
	fmt.Printf("after  update second read %v", r)
	if err != nil || r.Id != "nulltest" {
		t.Errorf("read failed: %v", r)
	}
	if r.NameP != nil {
		t.Errorf("expected null NameP")
	}
	if r.IntP != nil {
		t.Errorf("expected null IntP")
	}

}
