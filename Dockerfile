FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /last9-spinnaker-integration .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /last9-spinnaker-integration /last9-spinnaker-integration
EXPOSE 8080
ENTRYPOINT ["/last9-spinnaker-integration"]
