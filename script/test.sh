#!/usr/bin/env bash

source ./func.sh

if [[ "$OSTYPE" == "linux-gnu" ]]; then
    export GOOS=linux
elif [[ "$OSTYPE" == "darwin"* ]]; then
    export GOOS=darwin
fi

info "cd to ../"
cd ../

info "GOROOT=$GOROOT"
info "GOPATH=$GOPATH"
info "GOOS=$GOOS"

info "clean"
make clean

info "make"
make

msg "generate test data start.........."
time ./kye-mask -model generate-test -generate-test-db-file dbfile.txt -generate-test-table-count 1 -generate-test-table-row-count 100000 -generate-test-table-pool-count 100 -generate-test-db-username root -generate-test-db-password root -generate-test-db-host localhost -generate-test-db-port 3306
success "generate test data end.........."

msg "generate config start.........."
time ./kye-mask -model generate-conf -generate-conf-db-file dbfile.txt -generate-conf-db-cal-count 15000 -generate-conf-db-per-count 1024 -generate-conf-db-percent 0.9
success "generate config end.........."

msg "process start..."
time ./kye-mask -model run
success "process end..."

#msg "run test start..."
#time ./kye-mask -model process-test
#success "run test end..."

#./kye-mask -model generate-conf -dbs-file dbfile.txt -db-username crm_tmp_liuyinwei -db-password crm_tmp_liuinwei11v -db-host 10.124.204.2 -db-port 3306
