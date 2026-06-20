FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

# Copy the peer source code to the /app/peer directory in the container
COPY peer ./peer

#build the peer binary from the peer source code to the /app/peer directory in the container
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/peer ./peer

# Use a minimal base image to reduce the size of the final image
FROM gcr.io/distroless/static-debian12

# Set the working directory to the root of the container
WORKDIR /

# Copy the built peer binary from the builder stage from /app/peer to /peer in the final image
COPY --from=builder /app/bin/peer /peer

EXPOSE 8080
ENTRYPOINT ["/peer"]