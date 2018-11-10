//
// Update tests - Abstracted from the gorm tests
//

package db2

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	product1 := Product{Code: "product1code"}
	product2 := Product{Code: "product2code"}

	DB.Create(&product1)
	DB.Create(&product2)
	DB.Model(&product2).Update("Code", "product2newcode")

	if product2.Code != "product2newcode" {
		t.Errorf("Record should be updated")
	}

	DB.First(&product1, product1.Id)
	DB.First(&product2, product2.Id)
	updatedAt1 := product1.UpdatedAt

	if DB.First(&Product{}, "Code = ?", product1.Code).RecordNotFound() {
		t.Errorf("Product1 should not be updated")
	}

	if !DB.First(&Product{}, "Code = ?", "product2code").RecordNotFound() {
		t.Errorf("Product2's code should be updated")
	}

	if DB.First(&Product{}, "Code = ?", "product2newcode").RecordNotFound() {
		t.Errorf("Product2's code should be updated")
	}

	DB.Table("products").Where("Code in (?)", []string{"product1code"}).Update("code", "product1newcode")

	var product4 Product
	DB.First(&product4, product1.Id)
	if updatedAt1.Format(time.RFC3339Nano) != product4.UpdatedAt.Format(time.RFC3339Nano) {
		t.Errorf("updatedAt should be updated if something changed")
	}

	if !DB.First(&Product{}, "Code = 'product1code'").RecordNotFound() {
		t.Errorf("Product1's code should be updated")
	}

	if DB.First(&Product{}, "Code = 'product1newcode'").RecordNotFound() {
		t.Errorf("Product should not be changed to 789")
	}

	if DB.Model(product2).Update("CreatedAt", time.Now().Add(time.Hour)).Error != nil {
		t.Error("No error should raise when update with CamelCase")
	}

	if DB.Model(&product2).UpdateColumn("CreatedAt", time.Now().Add(time.Hour)).Error != nil {
		t.Error("No error should raise when update_column with CamelCase")
	}

	var products []Product
	DB.Find(&products)
	if count := DB.Model(Product{}).Update("CreatedAt", time.Now().Add(2*time.Hour)).RowsAffected; count != int64(len(products)) {
		t.Error("RowsAffected should be correct when do batch update")
	}

	DB.First(&product4, product4.Id)
	updatedAt4 := product4.UpdatedAt
	DB.Model(&product4).Update("Price", gorm.Expr("Price + ? - ?", 100, 50))
	var product5 Product
	DB.First(&product5, product4.Id)
	if product5.Price != product4.Price+100-50 {
		t.Errorf("Update with expression")
	}
	if product4.UpdatedAt.Format(time.RFC3339Nano) == updatedAt4.Format(time.RFC3339Nano) {
		t.Errorf("Update with expression should update UpdatedAt")
	}
}

func TestUpdates(t *testing.T) {
	product1 := Product{Code: "product1code", Price: 10}
	product2 := Product{Code: "product2code", Price: 10}
	DB.Save(&product1).Save(&product2)
	DB.Model(&product1).Updates(map[string]interface{}{"Code": "product1newcode", "Price": 100})
	if product1.Code != "product1newcode" || product1.Price != 100 {
		t.Errorf("Record should be updated also with map")
	}

	DB.First(&product1, product1.Id)
	DB.First(&product2, product2.Id)
	updatedAt2 := product2.UpdatedAt

	if DB.First(&Product{}, "code = ? and price = ?", product2.Code, product2.Price).RecordNotFound() {
		t.Errorf("Product2 should not be updated")
	}

	if DB.First(&Product{}, "code = ?", "product1newcode").RecordNotFound() {
		t.Errorf("Product1 should be updated")
	}

	DB.Table("products").Where("code in (?)", []string{"product2code"}).Updates(Product{Code: "product2newcode"})
	if !DB.First(&Product{}, "code = 'product2code'").RecordNotFound() {
		t.Errorf("Product2's code should be updated")
	}

	var product4 Product
	DB.First(&product4, product2.Id)
	if updatedAt2.Format(time.RFC3339Nano) != product4.UpdatedAt.Format(time.RFC3339Nano) {
		t.Errorf("updatedAt should be updated if something changed")
	}

	if DB.First(&Product{}, "code = ?", "product2newcode").RecordNotFound() {
		t.Errorf("product2's code should be updated")
	}

	updatedAt4 := product4.UpdatedAt
	DB.Model(&product4).Updates(map[string]interface{}{"Price": gorm.Expr("Price + ?", 100)})
	var product5 Product
	DB.First(&product5, product4.Id)
	if product5.Price != product4.Price+100 {
		t.Errorf("Updates with expression")
	}
	// product4's UpdatedAt will be reset when updating
	if product4.UpdatedAt.Format(time.RFC3339Nano) == updatedAt4.Format(time.RFC3339Nano) {
		t.Errorf("Updates with expression should update UpdatedAt")
	}
}

func TestUpdateColumn(t *testing.T) {
	product1 := Product{Code: "product1code", Price: 10}
	product2 := Product{Code: "product2code", Price: 20}
	DB.Save(&product1).Save(&product2).UpdateColumn(map[string]interface{}{"Code": "product2newcode", "Price": 100})
	if product2.Code != "product2newcode" || product2.Price != 100 {
		t.Errorf("product 2 should be updated with update column")
	}

	var product3 Product
	DB.First(&product3, product1.Id)
	if product3.Code != "product1code" || product3.Price != 10 {
		t.Errorf("product 1 should not be updated")
	}

	DB.First(&product2, product2.Id)
	updatedAt2 := product2.UpdatedAt
	DB.Model(product2).UpdateColumn("Code", "update_column_new")
	var product4 Product
	DB.First(&product4, product2.Id)
	if updatedAt2.Format(time.RFC3339Nano) != product4.UpdatedAt.Format(time.RFC3339Nano) {
		t.Errorf("updatedAt should not be updated with update column")
	}

	DB.Model(&product4).UpdateColumn("Price", gorm.Expr("price + 100 - 50"))
	var product5 Product
	DB.First(&product5, product4.Id)
	if product5.Price != product4.Price+100-50 {
		t.Errorf("UpdateColumn with expression")
	}
	if product5.UpdatedAt.Format(time.RFC3339Nano) != product4.UpdatedAt.Format(time.RFC3339Nano) {
		t.Errorf("UpdateColumn with expression should not update UpdatedAt")
	}
}

func TestUpdatesWithBlankValues(t *testing.T) {
	product := Product{Code: "product1", Price: 10}
	DB.Save(&product)

	DB.Model(&Product{Id: product.Id}).Updates(&Product{Price: 100})

	var product1 Product
	DB.First(&product1, product.Id)

	if product1.Code != "product1" || product1.Price != 100 {
		t.Errorf("product's code should not be updated")
	}
}

func TestUpdatesTableWithIgnoredValues(t *testing.T) {
	elem := ElementWithIgnoredField{Value: "foo", IgnoredField: 10}
	DB.Save(&elem)

	DB.Table(elem.TableName()).
		Where("id = ?", elem.Id).
		// DB.Model(&ElementWithIgnoredField{Id: elem.Id}).
		Updates(&ElementWithIgnoredField{Value: "bar", IgnoredField: 100})

	var elem1 ElementWithIgnoredField
	err := DB.First(&elem1, elem.Id).Error
	if err != nil {
		t.Errorf("error getting an element from database: %s", err.Error())
	}

	if elem1.IgnoredField != 0 {
		fmt.Println(elem1)
		t.Errorf("element's ignored field should not be updated")
	}
}
