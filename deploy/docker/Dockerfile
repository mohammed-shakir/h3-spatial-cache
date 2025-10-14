# Builder
FROM golang:1.24-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential=12.9 \
  ca-certificates=20230311+deb12u1 \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ENV CGO_ENABLED=1 GOOS=linux
RUN --mount=type=cache,target=/root/.cache/go-build \
  --mount=type=cache,target=/go/pkg/mod \
  go build -trimpath -ldflags="-s -w" -o /out/middleware ./cmd/middleware

# Runtime
FROM gcr.io/distroless/base-debian12:nonroot
WORKDIR /app
COPY --from=builder /out/middleware /app/middleware
USER nonroot:nonroot
ENTRYPOINT ["/app/middleware"]

