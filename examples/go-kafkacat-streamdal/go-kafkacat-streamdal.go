/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This is a modified & slimmed-down version of the go-kafkacat-streamdal
 * example from the Confluent Kafka Go client library that has been modified to
 * use Streamdal.
 *
 * See https://github.com/streamdal/confluent-kafka-go/blob/master/examples/go-kafkacat-streamdal/go-kafkacat-streamdal.go
 */

// Example kafkacat clone written in Golang
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/alecthomas/kingpin"
	kafka "github.com/streamdal/segmentio-kafka-go"
	streamdal "github.com/streamdal/streamdal/sdks/go"
)

var (
	keyDelim               = ""
	sigs                   chan os.Signal
	streamdalComponentName string
	streamdalOperationName string
	streamdalStrictErrors  bool
)

func runWriter(config *kafka.WriterConfig, topic string, partition int) {
	// Create a producer that has Streamdal enabled
	w := kafka.NewWriter(*config)
	if w == nil {
		fmt.Fprint(os.Stderr, ">> Failed to create writer\n")
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, ">> Created Writer, topic %s [partition %d]\n", topic, partition)

	if streamdalComponentName != kafka.StreamdalDefaultComponentName || streamdalOperationName != kafka.StreamdalDefaultOperationName || streamdalStrictErrors {
		fmt.Fprintf(os.Stderr, ">> Streamdal runtime-config enabled: component=%s, operation=%s, strict-errors=%v\n", streamdalComponentName, streamdalOperationName, streamdalStrictErrors)
	}

	reader := bufio.NewReader(os.Stdin)
	stdinChan := make(chan string)

	// Read input from stdin and send to stdinChan
	go func() {
		for true {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}

			line = strings.TrimSuffix(line, "\n")
			if len(line) == 0 {
				continue
			}

			stdinChan <- line
		}
		close(stdinChan)
	}()

	run := true

	for run == true {
		select {
		case sig := <-sigs:
			fmt.Fprintf(os.Stderr, ">> Terminating on signal %v\n", sig)
			run = false

		case line, ok := <-stdinChan:
			if !ok {
				run = false
				break
			}

			msg := kafka.Message{
				Topic:     topic,
				Partition: partition,
			}

			// If keyDelim is set, split the line into key and value
			if keyDelim != "" {
				vec := strings.SplitN(line, keyDelim, 2)
				if len(vec[0]) > 0 {
					msg.Key = ([]byte)(vec[0])
				}
				if len(vec) == 2 && len(vec[1]) > 0 {
					msg.Value = ([]byte)(vec[1])
				}
			} else {
				msg.Value = ([]byte)(line)
			}

			// Inject Streamdal runtime-config into the context IF component name, operation or strict-errors are set
			ctx := injectRuntimeConfig(context.Background(), streamdalComponentName, streamdalOperationName, streamdalStrictErrors)

			if err := w.WriteMessages(ctx, msg); err != nil {
				fmt.Fprintf(os.Stderr, ">> Produce error: %v\n", err)
			}
		}
	}

	fmt.Fprintf(os.Stderr, ">> Closing\n")
	w.Close()
}

func runReader(config *kafka.ReaderConfig) {
	topics := config.Topic

	if len(config.GroupTopics) > 0 {
		topics = fmt.Sprintf("%+v", config.GroupTopics)
	}

	// Create a consumer that has Streamdal enabled
	r := kafka.NewReader(*config)
	if r == nil {
		fmt.Fprint(os.Stderr, "Failed to create reader\n")
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, ">> Created Reader for topic(s) %+v, using last-offset\n", topics)

	if streamdalComponentName != kafka.StreamdalDefaultComponentName || streamdalOperationName != kafka.StreamdalDefaultOperationName || streamdalStrictErrors {
		fmt.Fprintf(os.Stderr, ">> Streamdal runtime-config enabled: component=%s, operation=%s, strict-errors=%v\n", streamdalComponentName, streamdalOperationName, streamdalStrictErrors)
	}

	run := true
	ctx, cancel := context.WithCancel(context.Background())

	// Watch for termination signal
	go func() {
		sig := <-sigs
		fmt.Fprintf(os.Stderr, ">> Terminating on signal %v\n", sig)
		run = false
		cancel()
	}()

	for run == true {
		// Inject Streamdal runtime-config into the context IF component name, operation or strict-errors are set
		ctx = injectRuntimeConfig(ctx, streamdalComponentName, streamdalOperationName, streamdalStrictErrors)

		// ReadMessage auto-commits offsets
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				fmt.Fprint(os.Stderr, ">> Detected interrupt, exiting consumer\n")
				run = false
				break
			}

			fmt.Fprintf(os.Stderr, ">> Fetch error: %v\n", err)
			continue
		}

		fmt.Printf("%s\n", msg.Value)
	}

	fmt.Fprintf(os.Stderr, ">> Closing consumer\n")
	r.Close()
}

func injectRuntimeConfig(ctx context.Context, cn, on string, se bool) context.Context {
	if cn != kafka.StreamdalDefaultComponentName || on != kafka.StreamdalDefaultOperationName || se {
		src := &kafka.StreamdalRuntimeConfig{
			Audience: &streamdal.Audience{},
		}

		if cn != "" {
			src.Audience.ComponentName = cn
		}

		if on != "" {
			src.Audience.OperationName = on
		}

		if se {
			src.StrictErrors = true
		}

		ctx = context.WithValue(ctx, kafka.StreamdalContextValueKey, src)
	}

	return ctx
}

func main() {
	sigs = make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	/* General options */
	brokers := kingpin.Flag("broker", "Bootstrap broker(s)").Required().String()
	keyDelimArg := kingpin.Flag("key-delim", "Key and value delimiter (empty string=dont print/parse key)").Default("").String()
	kingpin.Flag("streamdal-component-name", "Streamdal component name").Default("kafka").StringVar(&streamdalComponentName)
	kingpin.Flag("streamdal-operation-name", "Streamdal operation name").Default(kafka.StreamdalDefaultOperationName).StringVar(&streamdalOperationName)
	kingpin.Flag("streamdal-strict-errors", "Streamdal enable strict errors").Default("false").BoolVar(&streamdalStrictErrors)

	/* Producer mode options */
	modeP := kingpin.Command("produce", "Produce messages")
	topic := modeP.Flag("topic", "Topic to produce to").Required().String()
	partition := modeP.Flag("partition", "Partition to produce to").Default("-1").Int()

	/* Consumer mode options */
	modeC := kingpin.Command("consume", "Consume messages").Default()
	group := modeC.Flag("group", "Consumer group").Required().String()
	topics := modeC.Arg("topic", "Topic(s) to subscribe to").Required().Strings()

	mode := kingpin.Parse()
	keyDelim = *keyDelimArg

	switch mode {
	case "produce":
		wc := &kafka.WriterConfig{
			Brokers:         []string{*brokers},
			EnableStreamdal: true,
		}

		runWriter(wc, *topic, *partition)

	case "consume":
		rc := &kafka.ReaderConfig{
			Brokers:         []string{*brokers},
			Partition:       *partition,
			StartOffset:     kafka.LastOffset,
			EnableStreamdal: true,
		}

		if *group != "" && *topics != nil {
			rc.GroupTopics = *topics
			rc.GroupID = *group
		}

		runReader(rc)
	}
}
