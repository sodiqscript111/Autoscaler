FROM golang:1.25.3 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/autoscaler ./cmd/autoscaler

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=build /out/autoscaler /usr/local/bin/autoscaler
COPY config.docker.yaml /app/config.docker.yaml

ENV AUTOSCALER_CONFIG=/app/config.docker.yaml
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/autoscaler"]
