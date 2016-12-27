package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/jteeuwen/imghash"
)

type TProtoJSError struct {
	Success bool
	Message string
}

type TProtoJSSuccess struct {
	Success bool
	Items   interface{}
}

func ProtoError(w http.ResponseWriter, s string) {
	p := TProtoJSError{
		Success: false,
		Message: s,
	}
	buf, _ := json.MarshalIndent(p, "", " ")
	w.Write(buf)
}

func ProtoSuccess(w http.ResponseWriter, s interface{}) {
	p := TProtoJSSuccess{
		Success: true,
		Items:   s,
	}
	buf, _ := json.MarshalIndent(p, "", " ")
	w.Write(buf)
}

func root(w http.ResponseWriter, r *http.Request) {
	fmt.Println("ROOT")
	w.Header().Set("Cache-Control", "no-cache")
	fmt.Println(r.URL.String())
	switch r.URL.String() {
	case "/":
		http.Redirect(w, r, "/static/index.html", http.StatusMovedPermanently)
	}
	http.Error(w, "404 Page Not Found!", 404)
}

//сколько всего будет коллажей (на основе количества групп)
func getFile(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(1024 * 1024 * 100)
	if err != nil {
		ProtoError(w, "ERROR: "+err.Error())
		return
	}
	index, err := ioutil.ReadFile(static + "/index.html")
	if err != nil {
		ProtoError(w, "ERROR INDEX READ: "+err.Error())
		return
	}

	r.ParseMultipartForm(32 << 10)
	file, handler, err := r.FormFile("picture")
	if err != nil {
		ProtoError(w, "ERROR READ FILE: "+err.Error())
		return
	}
	defer file.Close()
	tempfilepath := os.TempDir() + "/image_searcher"
	f, err := os.OpenFile(tempfilepath, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		ProtoError(w, "ERROR create temp FILE: "+err.Error())
		return
	}
	defer f.Close()
	io.Copy(f, file)

	your_img, err := ioutil.ReadFile(static + "/img_data.html")
	if err != nil {
		ProtoError(w, "ERROR read img_data.html: "+err.Error())
		return
	}
	img_html := strings.Replace(string(your_img), "{YOURFILE}", handler.Filename, -1)

	ret := strings.Replace(string(index), "<!--IMG-->", img_html, -1)

	//делаем поиск подобных
	hash := getHash(imghash.Average, tempfilepath)
	fmt.Printf("your hash: %d\n", hash)

	h, err := storage.GetImageByHash(int64(hash))
	if h == nil {
		if err != nil {
			if strings.Index(err.Error(), "no rows in result set") == -1 {
				ProtoError(w, "ERROR GetImageByHash: "+err.Error())
				return
			}
		}
		ProtoError(w, "Ваша картинка отстутствует в Базе! В неоплаченной версии поиск разрешен толко среди картинок базы!")
		return
	}

	similars, err := storage.GetSimilar(h.Id)
	if err != nil {
		ProtoError(w, "ERROR GetSimilar: "+err.Error())
		return
	}

	ret = ret + "<div align=\"center\"><h2>Похожих: " + fmt.Sprintf("%d", len(similars)) + "</h1> (выставлен порог похожести в \"5\")</div><br>"

	ret = ret + `
	<table width="100%">
`

	if len(similars) > 0 {
		//формируем карту подобных картинок
		num := 0
		for _, s := range similars {
			if num == 0 {
				ret = ret + "\n<tr>\n"
			}
			si, err := storage.GetImageById(s.IdSimilar)
			if err != nil {
				ProtoError(w, "ERROR GetImageById: "+err.Error())
				return
			}

			ret = ret + "\n<td>" + fmt.Sprintf("Distance:%d<br>Name:%s<br>", s.Dist, si.Path) + "<img src=\"/get_image?id=" + fmt.Sprintf("%d", s.IdSimilar) + "\"></td>\n"
			num++
			if num == 3 {
				ret = ret + "\n</tr>\n"
				num = 0
			}
		}
	}

	ret = ret + `
	</tr>
</table>
`

	w.Write([]byte(ret))
}

func getTempImage(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadFile(os.TempDir() + "/image_searcher")
	if err != nil {
		ProtoError(w, "ERROR getTempImage: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "image")
	w.Write(data)
}

func getImage(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	id := r.Form.Get("id")
	var uid int64
	fmt.Sscanf(id, "%d", &uid)

	th, err := storage.GetImageById(uid)
	if err != nil {
		ProtoError(w, "ERROR GetImageById: "+err.Error())
		return
	}
	if th == nil {
		ProtoError(w, "ERROR no image found: "+err.Error())
		return
	}

	img, err := ioutil.ReadFile(th.Path)
	if err != nil {
		ProtoError(w, "ERROR ReadFile image: "+err.Error())
		return
	}

	w.Header().Set("Content-Type", "image")
	w.Write(img)
}

func createWebServer() error {

	http.HandleFunc("/", root)
	http.HandleFunc("/new_file", getFile)
	http.HandleFunc("/get_image", getImage)
	http.HandleFunc("/get_temp_image", getTempImage)
	fileServer := http.StripPrefix("/static/", http.FileServer(http.Dir(static)))
	http.Handle("/static/", fileServer)

	fmt.Printf("Запуск Веб-сервера по адресу http://localhost:%d\nКаталог Веб-сервера: %s\n", port, static)

	err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	return err
}
