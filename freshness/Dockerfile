# build stage
FROM golang:1.21-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=mod -o /out/dtms-fresh main.go

# final stage
FROM gcr.io/distroless/static-debian11
COPY --from=build /out/dtms-fresh /usr/local/bin/dtms-fresh
EXPOSE 8004
ENTRYPOINT ["/usr/local/bin/dtms-fresh"]
