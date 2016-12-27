package main

import (
	"database/sql"

	"github.com/go-gorp/gorp"
	_ "github.com/mattn/go-sqlite3"
)

type THash struct {
	Id   int64
	Hash int64
	Path string
}

type TSimilar struct {
	Id        int64 //who
	IdSimilar int64 //to whom
	Dist      int64 //distance
}

type Storage struct {
	db *DB
}

func NewStorage() (*Storage, error) {
	var this *Storage = &Storage{}

	db_orig, err := sql.Open("sqlite3", "sqlite.db")
	if err != nil {
		return nil, err
	}

	_, err = db_orig.Exec("PRAGMA cache_size = -150000") //150MB cache
	if err != nil {
		return nil, err
	}

	_, err = db_orig.Exec("PRAGMA count_changes = false")
	if err != nil {
		return nil, err
	}
	db0_m := DB_atom{
		db: &gorp.DbMap{Db: db_orig, Dialect: gorp.SqliteDialect{}},
	}
	this.db = &DB{
		db0:           db0_m,
		blocking_mode: true,
	}

	tb := this.db.AddTableWithName(THash{}, "hashes")
	tb.SetKeys(false, "Id")
	tb.AddIndex("hashes_hash", "Btree", []string{"Hash"})
	tb.AddIndex("hashes_path", "Btree", []string{"Path"})

	tb = this.db.AddTableWithName(TSimilar{}, "similar")
	tb.AddIndex("similar_id", "Btree", []string{"Id"})
	tb.AddIndex("similar_idsimilar", "Btree", []string{"IdSimilar"})

	err = this.db.CreateTablesIfNotExists()
	if err != nil {
		return this, err
	}

	return this, err
}

func (s *Storage) AddImage(th *THash) error {
	return s.db.InsertMass(th)
}

func (s *Storage) AddSimilar(id int64, id2 int64, d int64) error {
	return s.db.InsertMass(&TSimilar{Id: id, IdSimilar: id2, Dist: d})
}

func (s *Storage) GetImageByHash(h int64) (*THash, error) {
	th := THash{}
	err := s.db.SelectOne(&th, "select * from hashes where hash=:hash limit 1", map[string]int64{
		"hash": h,
	})
	if err != nil {
		return nil, err
	}
	return &th, nil
}

func (s *Storage) GetImageById(id int64) (*THash, error) {
	th := THash{}
	err := s.db.SelectOne(&th, "select * from hashes where id=:id", map[string]int64{
		"id": id,
	})
	if err != nil {
		return nil, err
	}
	return &th, nil
}

func (s *Storage) GetSimilar(id int64) ([]TSimilar, error) {
	th := []TSimilar{}
	_, err := s.db.Select(&th, "select * from similar where id=:id", map[string]int64{
		"id": id,
	})
	if err != nil {
		return th, err
	}
	return th, nil
}
