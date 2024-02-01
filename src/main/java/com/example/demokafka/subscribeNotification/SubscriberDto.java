package com.example.demokafka.subscribeNotification;

import com.example.demokafka.channel.MediaChannel;
import jakarta.persistence.Column;
import lombok.Data;

import java.util.List;


@Data
public class SubscriberDto {


    private Long id;


    
    private  String firstName;


    

    private String lastName;


    

    private List<MediaChannel> mediaChannels;


    

    private String email;
}
