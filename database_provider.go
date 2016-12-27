package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"

	"github.com/go-gorp/gorp"
)

type DB_atom struct {
	db   *gorp.DbMap
	file string //имя файла БД
}

type DB struct {
	db0 DB_atom

	firstOrder  chan chan bool
	secondOrder chan chan bool

	chanPool sync.Pool
	once     sync.Once
	wg       sync.WaitGroup

	blocking_mode bool
}

func (d *DB) SetBlockingMode(m bool) {
	d.blocking_mode = m
}

func (d *DB) Init() {
	d.firstOrder = make(chan chan bool, 10000) //выполняются одновременно, для select-ов, в этот момент нет insert,update
	d.secondOrder = make(chan chan bool, 10000)

	d.chanPool = sync.Pool{
		New: func() interface{} { return make(chan bool) },
	}

	go d.Worker()
}

//управляет порядком обращения к субд
//сначала пропускает на выполнение всех, кто first
//ждем, пока не выполняься first
//затем последовательно запускает second
//если при этом пришел first, то сначала выполнится он
func (d *DB) Worker() {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("db worker panic: %s\n", e)
		}
	}()

	for {
	innerloop:
		for {
			select {
			case p := <-d.firstOrder:
				d.wg.Add(1)
				p <- true
			default:
				break innerloop
			}
		}
		//ждем пока отработают быстряки
		d.wg.Wait()
		select {
		case p := <-d.secondOrder:
			d.wg.Add(1)
			p <- true
			d.wg.Wait()
		case p := <-d.firstOrder:
			d.wg.Add(1)
			p <- true
			//fmt.Println("first")
		}
	}
}

//регистрирует запрос, возвращает канал, в который будет записано true тогда,
//когда можно будет выполнять работу данному запросу
func (d *DB) StartParallel(firstOrder bool) chan bool {
	d.once.Do(d.Init)

	if !d.blocking_mode {
		firstOrder = true //все одновременно можно выполнять
	}

	//  вызывать d.chanPool.New() не надо - он автоматически вызывается внутри кода d.chanPool.Get()
	p := d.chanPool.Get()

	t, ok := p.(chan bool)
	if !ok {
		panic("Неверный тип получен из пула каналов в database_provider")
	}

	if firstOrder {
		d.firstOrder <- t
	} else {
		d.secondOrder <- t
	}
	return t
}

//вернуть в пул
func (d *DB) Put(p chan bool) *sync.WaitGroup {
	d.chanPool.Put(p)
	return &d.wg
}

func (d *DB) CreateTablesIfNotExists() error {
	p := d.StartParallel(true)
	<-p
	defer d.Put(p).Done()

	return d.db0.db.CreateTablesIfNotExists()
}

func (d *DB) CreateIndex() error {
	p := d.StartParallel(true)
	<-p
	defer d.Put(p).Done()
	return d.db0.db.CreateIndex()
}

func (d *DB) AddTableWithName(i interface{}, name string) *gorp.TableMap {
	p := d.StartParallel(true)
	<-p
	defer d.Put(p).Done()

	return d.db0.db.AddTableWithName(i, name)
}

//получить имя структуры(типа) и ругнуться если был передан **
func getNameForType(item interface{}) string {
	ptrv := reflect.ValueOf(item)
	if ptrv.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("getNameForType: passed non-pointer: %v (kind=%v)", item, ptrv.Kind()))
		return ""
	}
	elem := ptrv.Elem()
	etype := reflect.TypeOf(elem.Interface())
	return etype.Name()
}

func (d *DB_atom) CreateBackup() error {
	if len(d.file) == 0 {
		return nil
	}
	f, err := os.Open(d.file)
	if err != nil {
		return fmt.Errorf("os.Open(d.file) error: %s", err.Error())
	}
	defer f.Close()

	fb, err := os.Create(d.file + ".bak")
	if err != nil {
		return fmt.Errorf("os.Create(d.file error: %s", err.Error())
	}
	defer fb.Close()

	buf := make([]byte, 2<<20)
	_, err = io.CopyBuffer(fb, f, buf)
	if err != nil {
		return fmt.Errorf("io.CopyBuffer: %s", err.Error())
	}
	return nil
}

//обновляет id записи!
func (d *DB) Insert(list ...interface{}) error {
	p := d.StartParallel(false)
	<-p
	defer d.Put(p).Done()

	return d.db0.db.Insert(list...)
}

//прокидывает SelectNullInt
func (d *DB) SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error) {
	p := d.StartParallel(true)
	<-p
	defer d.Put(p).Done()

	return d.db0.db.SelectNullInt(query, args...)
}

//прокидывает SelectInt
func (d *DB) SelectInt(query string, args ...interface{}) (int64, error) {
	p := d.StartParallel(true)
	<-p
	defer d.Put(p).Done()

	return d.db0.db.SelectInt(query, args...)
}

//прокидывает Select
func (d *DB) Select(i interface{}, query string, args ...interface{}) ([]interface{}, error) {
	p := d.StartParallel(true)
	<-p
	defer d.Put(p).Done()

	return d.db0.db.Select(i, query, args...)
}

//ищет нужную базу по типу структуры и делает SelectOne
func (d *DB) SelectOne(i interface{}, query string, args ...interface{}) error {
	p := d.StartParallel(true)
	<-p
	defer d.Put(p).Done()

	return d.db0.db.SelectOne(i, query, args...)
}

func (d *DB) SelectMultiple(tablename string, i interface{}, query string, args ...interface{}) ([]interface{}, error) {
	p := d.StartParallel(true)
	<-p
	defer d.Put(p).Done()

	return d.db0.db.Select(i, query, args...)
}

//делает Exec
func (d *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	p := d.StartParallel(false)
	<-p
	defer d.Put(p).Done()

	return d.db0.db.Exec(query, args...)
}

//не может возвращать .id записи!!!!
//работает через общую транзакцию! может выполниться несколько позже
func (d *DB) InsertMass(list ...interface{}) error {
	for _, li := range list {
		err := d.commonTran(DB_COMMON_INSERT, li)
		if err != nil {
			return err
		}
	}
	return nil
}

//работает через общую транзакцию! может выполниться несколько позже
func (d *DB) Update(i interface{}) error {
	return d.commonTran(DB_COMMON_UPDATE, i)
}

//работает через общую транзакцию! может выполниться несколько позже
func (d *DB) Delete(i interface{}) error {
	return d.commonTran(DB_COMMON_DELETE, i)
}

type execData struct {
	query string
	args  []interface{}
}

//работает через общую транзакцию! может выполниться несколько позже
func (d *DB) ExecMass(query string, args ...interface{}) (sql.Result, error) {
	return nil, d.commonTran(DB_COMMON_EXEC, &execData{query: query, args: args})
}

const (
	DB_COMMON_EXEC = iota
	DB_COMMON_INSERT
	DB_COMMON_UPDATE
	DB_COMMON_DELETE
)

type db_common_item struct {
	val interface{}
	typ int
}

var db_common_saver chan *db_common_item = make(chan *db_common_item, 1000)
var db_common_once sync.Once

//вспомогательная функция выполнения одновременных запросов в транзакции
func (d *DB) commonTran(typ int, val interface{}) error {
	if val != nil {
		db_common_saver <- &db_common_item{val: val, typ: typ}
	}

	db_common_once.Do(func() {
		go func() {
			reps := make([]*db_common_item, 0, 1000)
			for {
				reps = reps[0:0]
				//вынимаем первое значение очереди
				select {
				case r := <-db_common_saver:
					reps = append(reps, r)
				}
				was := true
				//вынимаем все оставшиеся, если есть
				for was {
					select {
					case r := <-db_common_saver:
						reps = append(reps, r)
					default:
						was = false
					}
				}

				//выполняем все эти запросы в одной транзакции (кроме select)
				func() {
					p := d.StartParallel(false)
					<-p
					defer d.Put(p).Done()

					tx, err := d.db0.db.Begin()
					if err != nil {
						fmt.Printf("ERROR commonTran1: %s\n", err.Error())
						return
					}
					for _, r := range reps {
						switch r.typ {
						case DB_COMMON_EXEC:
							rv := r.val.(*execData)
							_, err = tx.Exec(rv.query, rv.args...)
							if err != nil {
								fmt.Println("on exec, query: " + rv.query)
							}

						case DB_COMMON_UPDATE:
							_, err = tx.Update(r.val)
							if err != nil {
								fmt.Println("on update")
							}

						case DB_COMMON_INSERT:
							err = tx.Insert(r.val)
							if err != nil {
								fmt.Println("on insert")
							}
						case DB_COMMON_DELETE:
							_, err = tx.Delete(r.val)
							if err != nil {
								fmt.Println("on delete")
							}
						}

						if err != nil {
							tx.Rollback()
							fmt.Printf("ERROR commonTran2: %s\n", err.Error())
							return
						}
					}
					tx.Commit()
				}()
			}
		}()
	})
	return nil
}

//чтобы не завершалась программа до того, как запишем базу
func (d *DB) Flush() error {
	var vals = []bool{false, true, false}
	for _, v := range vals {
		func() {
			p := d.StartParallel(v)
			<-p
			defer d.Put(p).Done()
		}()
	}

	return nil
}
