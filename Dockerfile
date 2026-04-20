# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/inesdb ./cmd/cli

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=builder /out/inesdb /inesdb

EXPOSE 5432
ENTRYPOINT ["/inesdb"]

