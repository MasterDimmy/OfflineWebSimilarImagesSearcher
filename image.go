package main

import (
	"image"
	"image/jpeg"
	"image/png"
	"os"

	"github.com/jteeuwen/imghash"
)

func loadImg(file string) (image.Image, error) {
	fd, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	defer fd.Close()

	img, _, err := image.Decode(fd)
	if err != nil {
		img, err = png.Decode(fd)
		if err != nil {
			img, err = jpeg.Decode(fd)
			if err != nil {
				return nil, err
			}
		}
	}

	return img, nil
}

func getHash(hf imghash.HashFunc, file string) uint64 {
	img, err := loadImg(file)

	if err != nil {
		panic(err)
	}

	return hf(img)
}
