FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/pod-kueue-demo .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/pod-kueue-demo /pod-kueue-demo
EXPOSE 8080
ENTRYPOINT ["/pod-kueue-demo"]
