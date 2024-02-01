package com.example.demokafka.media;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class MediaInfoDto {

    private String name;

    private String createdBy;

    private LocalDateTime createdDate;

    private Integer widthResolution;

    private Integer heightResolution;

    private Long uploadedTo;/// mediaChannel id

    private String mediaName;

    private String title;

    private Long viewsCount;

    private String description;
}
