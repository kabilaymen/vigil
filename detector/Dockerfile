FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /detector .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /detector /detector
EXPOSE 9101
ENTRYPOINT ["/detector"]
