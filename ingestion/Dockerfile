FROM golang:1.21-alpine AS builder
RUN apk --no-cache add gcc musl-dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o bridge .

FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata libc6-compat
WORKDIR /app
COPY --from=builder /app/bridge .
EXPOSE 2112
CMD ["./bridge"]
