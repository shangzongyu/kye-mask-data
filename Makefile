all:
	go build -ldflags "-w -s" -o bin/kye-mask -v main.go
	cp config.json bin

pro:
	go build -tags pro -ldflags "-w -s" -o bin/kye-mask -v main_pro.go
	#cp config.json bin
	#cp regexp.txt bin
	#cp dbfile.txt bin
	upx -q bin/kye-mask

clean:
	go clean
	rm -fr bin/kye-mask

.PHONY: all clean
