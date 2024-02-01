package com.example.demokafka;

import com.example.demokafka.media.topologyConfig.PopularVedioKafkaStreamsPunctuation;
import com.example.demokafka.media.topologyConfig.VideoRecord;
import com.example.demokafka.media.topologyConfig.MediaTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static com.example.demokafka.media.topologyConfig.MediaTopology.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MediaTopologyTest {

    TopologyTestDriver topologyTestDriver = null;
    TestInputTopic<String, VideoRecord> videosInputTopic = null;
    TestInputTopic<String, Long> videosInputTopic1 = null;
    static String INPUT_TOPIC = VIDEOS;
    StreamsBuilder streamsBuilder;
    MediaTopology videosTopology = new MediaTopology();


    @BeforeEach
    void setUp() {
        streamsBuilder = new StreamsBuilder();
        videosTopology.process(streamsBuilder);
        topologyTestDriver = new TopologyTestDriver(streamsBuilder.build());

        videosInputTopic =
                topologyTestDriver.
                        createInputTopic(
                                INPUT_TOPIC, Serdes.String().serializer(),
                                new JsonSerde<VideoRecord>(VideoRecord.class).serializer());

    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void videosCount() {

        videosInputTopic.pipeKeyValueList(videos());

        ReadOnlyKeyValueStore<String, Long> OrdersCountStore = topologyTestDriver.getKeyValueStore(
                VIDEOS_COUNT);

        var v1234OrdersCount = OrdersCountStore.get("v_1234");
        assertEquals(2, v1234OrdersCount);

        var v4567OrdersCount = OrdersCountStore.get("v_4567");
        assertEquals(2, v4567OrdersCount);

    }

    static List<KeyValue<String, VideoRecord>> videos() {

        var video1 = new VideoRecord(54321, "v_1234", new BigDecimal("27.00"));
        var video2 = new VideoRecord(54321, "v_1234", new BigDecimal("15.00"));
        var video3 = new VideoRecord(12345, "v_4567", new BigDecimal("27.00"));
        var video4 = new VideoRecord(12345, "v_4567", new BigDecimal("27.00"));

        var keyValue1 = KeyValue.pair(video1.videoId().toString(), video1);
        var keyValue2 = KeyValue.pair(video2.videoId().toString(), video2);
        var keyValue3 = KeyValue.pair(video3.videoId().toString(), video3);
        var keyValue4 = KeyValue.pair(video4.videoId().toString(), video4);

        return List.of(keyValue1, keyValue2, keyValue3, keyValue4);
    }
    

    @Test
    public void punctuationTest() throws IOException {

        final PopularVedioKafkaStreamsPunctuation instance = new PopularVedioKafkaStreamsPunctuation();

        final Topology topology = instance.buildTopology(new StreamsBuilder());

        final StreamsBuilder builder = new StreamsBuilder();
        

        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "video-streams-app");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


        try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, settings)) {

            final TestInputTopic<String, Long> inputTopic = testDriver.createInputTopic(VIDEOS_OUTPUT,
                    Serdes.String().serializer(),
                    Serdes.Long().serializer());

            final TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic("high-purchase-video-topic", Serdes.String().deserializer(),
                    Serdes.Long().deserializer());

            List<KeyValue<String, Long>> keyValues = null;

            var keyValue1 = KeyValue.pair("v_1234", 27L);
            var keyValue2 = KeyValue.pair("v_1234", 15L);
            var keyValue3 = KeyValue.pair("v_4567", 27L);
            var keyValue4 = KeyValue.pair("v_4567", 27L);

            keyValues = List.of(keyValue1, keyValue2, keyValue3, keyValue4);

            inputTopic.pipeKeyValueList(keyValues, Instant.now(), Duration.ofSeconds(2));


            final List<KeyValue<String, Long>> actualResults = outputTopic.readKeyValuesToList();
            assertThat(actualResults.size(), is(greaterThanOrEqualTo(1)));

            KeyValueStore<String, Long> store = testDriver.getKeyValueStore(VIDEOS_COUNT);

            testDriver.advanceWallClockTime(Duration.ofSeconds(20));

            assertSame(store.get("v_1234"), 0L);
            assertSame(store.get("v_1234"), 0L);
            assertSame(store.get("v_4567"), 0L);

        }


    }


}

