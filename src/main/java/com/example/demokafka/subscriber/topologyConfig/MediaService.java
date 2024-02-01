package com.example.demokafka.subscriber.topologyConfig;


import com.example.demokafka.channel.MediaChannel;
import com.example.demokafka.channel.MediaChannelRepository;
import com.example.demokafka.media.MediaInfoDto;
import com.example.demokafka.media.Video;
import com.example.demokafka.media.VideoRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.example.demokafka.media.topologyConfig.MediaTopology.VIDEOS_COUNT;

@Slf4j
@Service
public class MediaService {

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public List<SubscriberCountPerStoreDTO> videosCount() {
        ReadOnlyKeyValueStore<String, Long> videosStoreData = streamsBuilderFactoryBean.getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType(
                        VIDEOS_COUNT,
                        QueryableStoreTypes.keyValueStore()
                ));

        var videos = videosStoreData.all();
        var spliterator = Spliterators.spliteratorUnknownSize(videos, 0);
        return StreamSupport.stream(spliterator, false)
                .map(data -> new SubscriberCountPerStoreDTO(data.key, data.value))
                .collect(Collectors.toList());
    }


    @Autowired
    private VideoRepository videoRepository;

    @Autowired
    private MediaChannelRepository mediaChannelRepository;

    @Autowired
    private SubscriberTopology mediaTopology;

    @Autowired
    private PopularVedioKafkaStreamsPunctuation popularVedioKafkaStreamsPunctuation;



    public void  addMediaDetails(MediaInfoDto mediaInfoDto)
    {
        MediaChannel mediaChannel = new MediaChannel();
        mediaChannel.setName(mediaInfoDto.getName());
        mediaChannel.setCreatedBy(mediaInfoDto.getCreatedBy());
        mediaChannel.setCreatedDate(LocalDateTime.now());
        mediaChannel= mediaChannelRepository.save(mediaChannel);

        Video video= new Video();
        video.setName(mediaInfoDto.getMediaName());
        video.setHeightResolution(mediaInfoDto.getHeightResolution());
        video.setDescription(mediaInfoDto.getDescription());
        video.setTitle(mediaInfoDto.getTitle());
        video.setUploadedTo(mediaChannel.getId());
        video.setViewsCount(mediaInfoDto.getViewsCount());
        videoRepository.save(video);

        mediaTopology.process(new StreamsBuilder());
        popularVedioKafkaStreamsPunctuation.buildTopology(new StreamsBuilder());

    }

    public Optional<MediaChannel> getChannelById(Long id)
    {
        return  mediaChannelRepository.findById(id);
    }




}
