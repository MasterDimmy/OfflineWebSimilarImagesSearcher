package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jteeuwen/imghash"
)

var storage *Storage

var images []string
var hashes map[int64]*THash = make(map[int64]*THash)

var similar map[uint64][]uint64

func walkFn(path string, info os.FileInfo, err error) error {
	if info == nil {
		return nil
	}
	if info.IsDir() {
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		fmt.Println("Ошибка чтения файла: " + err.Error())
		return err
	}
	f.Close()

	images = append(images, path)
	return nil
}

var similarity = uint64(5)
var static string = "./static"
var port int = 7075

func main() {
	web_mode := true
	image_path := "images"

	if len(os.Args) > 1 {
		if os.Args[1] == "dbcreate" {
			web_mode = false
		}
	}

	var err error
	storage, err = NewStorage()
	if err != nil {
		fmt.Println("ERROR: " + err.Error())
		return
	}

	if web_mode { //Веб-режим
		err = createWebServer()
		if err != nil {
			fmt.Println("ERROR: " + err.Error())
			return
		}
		return
	}

	fmt.Println("Чтение базы картинок...")

	err = filepath.Walk(image_path, walkFn)
	if err != nil {
		fmt.Println("Ошибка: " + err.Error())
		return
	}

	total := len(images)
	var cur int64 = 0
	fmt.Printf("Обнаружено %d файлов.\nФормирование хешей...\n", total)
	for _, p := range images {
		cur++
		th := THash{Id: cur, Hash: int64(getHash(imghash.Average, p)), Path: p}
		//fmt.Printf("hash: %s %d\n", p, th.Hash)
		hashes[cur] = &th
		if total < 100 {
			fmt.Printf("%d of %d...\n", cur, total)
		} else {
			if cur%50 == 0 {
				fmt.Printf("%d%%...\n", int(cur)*100/total)
			}
		}
		storage.AddImage(hashes[cur])
	}

	cur = 0
	fmt.Println("Построение карты похожих картинок...")

	for i, h := range hashes {
		if total < 100 {
			fmt.Printf("%d of %d...\n", cur, total)
		} else {
			if cur%50 == 0 {
				fmt.Printf("%d%%...\n", int(cur)*100/total)
			}
		}
		for i2, h2 := range hashes {
			if i2 != i {
				dist := imghash.Distance(uint64(h.Hash), uint64(h2.Hash))
				//fmt.Printf("%d %20s %d - %d %20s %d: %d\n", h.Id, h.Path, h.Hash, h2.Id, h2.Path, h2.Hash, dist)
				if dist < similarity {
					storage.AddSimilar(i, i2, int64(dist))
				}
			}
		}

		cur++
	}
	storage.db.Flush()
	fmt.Println("База данных создана!")
}
