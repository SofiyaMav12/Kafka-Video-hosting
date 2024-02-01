package com.example.demokafka.subscriber.topologyConfig;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Date;

import static com.example.demokafka.media.topologyConfig.MediaTopology.VIDEOS_COUNT;
import static com.example.demokafka.media.topologyConfig.MediaTopology.VIDEOS_OUTPUT;

public class PopularVedioKafkaStreamsPunctuation {

    public Topology buildTopology(StreamsBuilder streamsBuilder) {

        final String loginTimeInputTopic = VIDEOS_OUTPUT;
        final String outputTopic = "high-purchase-video-topic";

        KStream<String, Long> videoStreams = streamsBuilder.stream(VIDEOS_OUTPUT,
                Consumed.with(Serdes.String(), Serdes.Long()));

        videoStreams.print(Printed.<String, Long>toSysOut().withLabel("max-video-product------"));

        StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(VIDEOS_COUNT),Serdes.String(), Serdes.Long());
        streamsBuilder.addStateStore(storeBuilder);


        videoStreams.transform(getTransformerSupplier(VIDEOS_COUNT), Named.as("max-video-product"),VIDEOS_COUNT)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final KStream<String, Long> finalGood = streamsBuilder.stream(outputTopic,
                Consumed.with(Serdes.String(),Serdes.Long()));

        finalGood.print(Printed.<String, Long>toSysOut().withLabel("max-video-product------"));


        return streamsBuilder.build();
    }


    private TransformerSupplier<String, Long, KeyValue<String, Long>> getTransformerSupplier(final String storeName) {
        return () -> new Transformer<String, Long, KeyValue<String, Long>>() {
            private KeyValueStore<String, Long> store;
            private ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                store = (KeyValueStore<String, Long>) this.context.getStateStore(storeName);
                this.context.schedule(Duration.ofSeconds(3600), PunctuationType.STREAM_TIME, this::streamTimePunctuator);
                this.context.schedule(Duration.ofSeconds(20), PunctuationType.WALL_CLOCK_TIME, this::wallClockTimePunctuator);
            }


            void wallClockTimePunctuator(Long timestamp){
                try (KeyValueIterator<String, Long> iterator = store.all()) {
                    while (iterator.hasNext()) {
                        KeyValue<String, Long> keyValue = iterator.next();
                        store.put(keyValue.key, 0L);
                    }
                }
                System.out.println("@" + new Date(timestamp) +" Reset all view-times to zero");
            }

            void streamTimePunctuator(Long timestamp) {
                Long maxValue = Long.MIN_VALUE;
                String maxValueKey = "";
                try (KeyValueIterator<String, Long> iterator = store.all()) {
                    while (iterator.hasNext()) {
                        KeyValue<String, Long> keyValue = iterator.next();
                        if (keyValue.value > maxValue) {
                            maxValue = keyValue.value;
                            maxValueKey = keyValue.key;
                        }
                    }
                }
                context.forward(maxValueKey +" @" + new Date(timestamp), maxValue);
            }

            @Override
            public KeyValue<String, Long> transform(String key, Long value) {
                Long currentVT = store.putIfAbsent(key, value);
                if (currentVT != null) {
                    store.put(key, Long.valueOf(currentVT + value));
                }
                return null;
            }

            @Override
            public void close() {

            }
        };
    }




}

