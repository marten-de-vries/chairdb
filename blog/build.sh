#!/bin/bash

rm -r out
mkdir out
cp blog.md out/blog.md
cp style.css out/style.css
cp -r img out/img
pandoc blog.md -s --number-sections --toc --css style.css -o out/index.html --default-image-extension svg
pandoc blog.md -s --number-sections --toc -o out/blog.epub --default-image-extension png
pandoc blog.md -s --number-sections --toc -V toc-title="Table of Contents" -o out/blog.pdf --default-image-extension eps
