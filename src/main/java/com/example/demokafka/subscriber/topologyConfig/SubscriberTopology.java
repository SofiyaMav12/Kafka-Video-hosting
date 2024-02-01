package com.example.demokafka.subscriber.topologyConfig;




import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SubscriberTopology {

    public static final String VIDEOS = "newvideos";
    public static final String VIDEOS_COUNT = "videos_count";
    public static final String VIDEOS_OUTPUT = "videos-output";
    


    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

        KStream<String, VideoRecord> videoStreams = streamsBuilder.stream(VIDEOS,
                        Consumed.with(Serdes.String(), new JsonSerde<>(VideoRecord.class)))
                .selectKey((key, value) -> value.videoId().toString());

        videoStreams.print(Printed.<String, VideoRecord>toSysOut().withLabel("videos"));
        videosCount(videoStreams);
    }

    private void videosCount(KStream<String, VideoRecord> generalOrdersStream) {

        KTable<String, Long> videosCount = generalOrdersStream.map(
                        (key, video) -> KeyValue.pair(video.videoId().toString(), video))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(VideoRecord.class)))
                .count(Named.as(SubscriberTopology.VIDEOS_COUNT),
                        //use state store to save data
                        Materialized.as(SubscriberTopology.VIDEOS_COUNT));

        videosCount.toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(SubscriberTopology.VIDEOS_COUNT));

        /*         * Publish data to output kafka topic
         *
         videosCount.toStream()
         .to(VIDEOS_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));
         */

        videosCount.toStream()
                .to(VIDEOS_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));
    }

}
