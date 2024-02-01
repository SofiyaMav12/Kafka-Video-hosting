package com.example.demokafka.media.topologyConfig;


import com.example.demokafka.channel.MediaChannel;
import com.example.demokafka.media.MediaInfoDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/v1/videos")
public class MediaController {

    @Autowired
    private MediaService videoService;


    @GetMapping("/count")
    public List<MediaCountPerStoreDTO> videosCount() {
        return videoService.videosCount();
    }


    @PostMapping("/items")
    void createNew(@RequestBody MediaInfoDto mediaInfoDto) {
         videoService.addMediaDetails(mediaInfoDto);
    }

    @GetMapping("/get-channel")
    public Optional<MediaChannel> videosCount(@RequestParam("od")Long id) {
        return videoService.getChannelById(id);
    }

}