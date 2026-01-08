FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/sentiment-grpc ./cmd/sentiment-grpc

FROM gcr.io/distroless/base-debian12
COPY --from=build /out/sentiment-grpc /sentiment-grpc
EXPOSE 50051
USER 65532:65532
ENTRYPOINT ["/sentiment-grpc"]
