all:
	go build -ldflags "-w -s" -o kye-mask -v main.go

compress:
	go build -ldflags "-w -s" -o kye-mask -v main.go
	upx -q kye-mask

clean:
	rm -fr kye-mask
	go clean

.PHONY: all clean
