package com.example.demokafka.subscribeNotification;


import com.example.demokafka.StreamsUtils;
import com.example.demokafka.channel.MediaChannel;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Service
@Slf4j
public class SubscriberService {

    @Autowired
    SubscriberRepository subscriberRepository;


    public void registerService(SubscriberDto subscriberDto){

        Subscriber subscriber =new Subscriber();
        subscriber.setEmail( subscriberDto.getEmail());
        subscriber.setFirstName(subscriberDto.getFirstName());
        subscriber.setLastName(subscriberDto.getLastName());
        subscriber.setMediaChannels(subscriberDto.getMediaChannels());
        subscriber=  subscriberRepository.save(subscriber);

    }


    public void SubScribeToChannel(Subscriber subscriber)
    {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        final Properties streamProperties = StreamsUtils.loadProperties("src/main/resources/stream.properties");
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-stream");

        final Map<String, Object> configMap = new HashMap<>();

        configMap.put("application.id", "aggregate-stream");
        configMap.put("aggregate.input.topic", "subscription");
        configMap.put("aggregate.output.topic", "a-output");
        configMap.put("schema.registry.url", "http://localhost:9092");
        final String inputTopic = streamProperties.getProperty("aggregate.input.topic");
        final String outputTopic = streamProperties.getProperty("aggregate.output.topic");



        final SpecificAvroSerde<MediaChannel> mediaSerde = new SpecificAvroSerde<>();
        mediaSerde.configure(configMap,true);

        final KStream<String, MediaChannel> mediaStream = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String()
                , mediaSerde
        ));

        mediaStream.groupByKey().aggregate(() -> 0.0, (key, value, total) -> total + value.getId(), Materialized.with(Serdes.String(), Serdes.Double()))

                .toStream()
                .peek((k, v) -> System.out.println("k - " + k + " v -" + v)).to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamProperties);


        kafkaStreams.start();
    }


}
