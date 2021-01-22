#!/bin/bash

cp blog.md out/blog.md
cp style.css out/style.css
cp -r img out/img
pandoc blog.md -s --toc --css style.css -o out/blog.html --default-image-extension svg
pandoc blog.md -s --toc -o out/blog.epub --default-image-extension svg
pandoc blog.md -s --toc -o out/blog.pdf --default-image-extension eps
