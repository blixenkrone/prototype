FROM golang:1.27.3-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
COPY events/ ./events/
RUN go mod download

COPY eventbus/ ./eventbus/
COPY cmd/demo-producer/ ./cmd/demo-producer/
COPY cmd/demo-consumer/ ./cmd/demo-consumer/

ARG SERVICE
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/service "./cmd/${SERVICE}"

FROM alpine:3.22

RUN apk add --no-cache ca-certificates
WORKDIR /
COPY --from=build /out/service /service
USER 65532:65532
ENTRYPOINT ["/service"]
