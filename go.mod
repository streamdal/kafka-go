module github.com/streamdal/segmentio-kafka-go

go 1.15

require (
	github.com/klauspost/compress v1.15.9
	github.com/pierrec/lz4/v4 v4.1.15
	github.com/segmentio/kafka-go v0.4.40
	github.com/streamdal/dataqual v0.0.19-0.20230620185622-328ff8e5adff
	github.com/stretchr/testify v1.8.0
	github.com/xdg-go/scram v1.1.2
	golang.org/x/net v0.11.0
)

retract [v0.4.36, v0.4.37]
