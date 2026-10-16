# Minimal image: static binary on distroless, no shell or package manager.
# GOMEMLIMIT isn't baked in on purpose; set it per run alongside the container memory limit:
#   docker run --memory=256m -e GOMEMLIMIT=230MiB -p 8080:8080 -p 9090:9090 tensorgate

FROM golang:1.27 AS build
WORKDIR /src

# dependencies get their own layer, so editing code doesn't re-download modules
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tensorgate ./cmd/tensorgate

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /tensorgate /tensorgate
# API, metrics
EXPOSE 8080 9090
ENTRYPOINT ["/tensorgate"]
