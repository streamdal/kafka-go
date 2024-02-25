module github.com/streamdal/segmentio-kafka-go

go 1.15

require (
	github.com/klauspost/compress v1.15.9
	github.com/pierrec/lz4/v4 v4.1.15
	github.com/streamdal/streamdal/sdks/go v0.1.10
	github.com/stretchr/testify v1.8.4
	github.com/xdg-go/scram v1.1.2
	golang.org/x/net v0.20.0
)

retract [v0.4.36, v0.4.37]
