FROM golang:1.20 as dependencies

FROM dependencies 

RUN apt-get update && apt-get install -y iputils-ping

ENV HOME /home
WORKDIR $HOME/rdb

COPY internal ./internal
COPY cmd ./cmd
COPY module.go \
  rdb.go \
  types.go \
  utils.go \
  go.mod \
  go.sum ./

RUN go build -o rdb_replica ./cmd/rdb/main.go 

EXPOSE 8080
EXPOSE 54321
EXPOSE 54322
EXPOSE 54323

CMD ["./rdb_replica"]