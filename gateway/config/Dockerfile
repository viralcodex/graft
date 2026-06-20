FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY gateway ./gateway

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/gateway ./gateway

# Use a minimal base image to reduce the size of the final image
FROM gcr.io/distroless/static-debian12

WORKDIR /

COPY --from=builder /app/bin/gateway /gateway

EXPOSE 7000
ENTRYPOINT ["/gateway"]
