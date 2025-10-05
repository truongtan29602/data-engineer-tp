FROM golang:latest

WORKDIR /app

COPY . .

RUN go build -o dispatcher dispatcher.go

EXPOSE 80

CMD ["./dispatcher"]