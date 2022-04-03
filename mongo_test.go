package qmg

import "testing"

func TestTransaction(t *testing.T) {
	url := "mongodb://root:root@localhost:27017"
	database := "shop"
	m := MustNewModel(url, database)
	_, err := m.Transaction(func() (err error) {
		err = m.Insert("ttt", nil)
		return
	})
	if err != nil {
		t.Error(err)
	}
}
