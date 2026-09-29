# syntax=docker/dockerfile:1

ARG GO_VERSION=1.27.1
FROM golang:${GO_VERSION}-bookworm AS builder

ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=1 GOOS=linux GOARCH=${TARGETARCH} \
	go build -trimpath -o /out/overturetunkki ./cmd/overturetunkki \
	&& CGO_ENABLED=1 GOOS=linux GOARCH=${TARGETARCH} \
	go build -trimpath -o /out/native-probe ./cmd/native-probe

FROM scratch

COPY --from=builder /out/overturetunkki /overturetunkki
COPY --from=builder /out/native-probe /native-probe
