package com.example.demokafka.subscribeNotification;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class TumblingWindow {

    public Properties buildStreamsProperties(Properties allProps) {
        allProps.put(StreamsConfig.APPLICATION_ID_CONFIG, allProps.getProperty("application.id"));
        allProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        allProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        allProps.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        try {
            allProps.put(StreamsConfig.STATE_DIR_CONFIG,
                    Files.createTempDirectory("tumbling-windows").toAbsolutePath().toString());
        }
        catch(IOException e) {
        }
        return allProps;
    }

    public Topology buildTopology(Properties allProps) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String ratingTopic = allProps.getProperty("rating.topic.name");
        final String ratingCountTopic = allProps.getProperty("rating.count.topic.name");

        builder.<String, Subscriber>stream(ratingTopic)
                .map((key, rating) -> new KeyValue<>(rating.getId(), rating))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(10), Duration.ofMinutes(60)))
                .count()
                .toStream().map((longWindowed, aLong) -> new KeyValue<>(longWindowed.key(),aLong.toString()))
                .to(ratingCountTopic);

        return builder.build();
    }

    private String windowedKeyToString(Windowed<String> key) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd'T'HH:mm:ssZZZZ");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return String.format("[%s@%s/%s]",
                key.key(),
                sdf.format(key.window().startTime().getEpochSecond()),
                sdf.format(key.window().endTime().getEpochSecond()));
    }


    private SpecificAvroSerde<Subscriber> ratedMovieAvroSerde(final Properties allProps) {
        final SpecificAvroSerde<Subscriber> movieAvroSerde = new SpecificAvroSerde<>();

        Map<String, String> config = new HashMap<>();
        for (final String name: allProps.stringPropertyNames())
            config.put(name, allProps.getProperty(name));
        movieAvroSerde.configure(config, false);
        return movieAvroSerde;
    }

    public void createTopics(Properties allProps) {
        AdminClient client = AdminClient.create(allProps);

        List<NewTopic> topics = new ArrayList<>();
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put("retention.ms", Long.toString(Long.MAX_VALUE));

        NewTopic ratings = new NewTopic(allProps.getProperty("rating.topic.name"),
                Integer.parseInt(allProps.getProperty("rating.topic.partitions")),
                Short.parseShort(allProps.getProperty("rating.topic.replication.factor")));
        ratings.configs(topicConfigs);
        topics.add(ratings);

        NewTopic counts = new NewTopic(allProps.getProperty("rating.count.topic.name"),
                Integer.parseInt(allProps.getProperty("rating.count.topic.partitions")),
                Short.parseShort(allProps.getProperty("rating.count.topic.replication.factor")));
        counts.configs(topicConfigs);
        topics.add(counts);


        client.createTopics(topics);
        client.close();
    }

    public Properties loadEnvProperties(String fileName) throws IOException {
        Properties allProps = new Properties();
        FileInputStream input = new FileInputStream(fileName);
        allProps.load(input);
        input.close();

        return allProps;
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        TumblingWindow tw = new TumblingWindow();
        Properties allProps = tw.buildStreamsProperties(tw.loadEnvProperties(args[0]));
        Topology topology = tw.buildTopology(allProps);

        tw.createTopics(allProps);

        final KafkaStreams streams = new KafkaStreams(topology, allProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
