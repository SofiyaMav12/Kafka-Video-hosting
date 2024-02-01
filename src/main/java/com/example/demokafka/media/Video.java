package com.example.demokafka.media;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(name = "video")
public class Video {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    private Long id;

    @Column
    private Integer widthResolution;

    @Column
    private Integer heightResolution;

    @Column
    private Long uploadedTo;/// mediaChannel id

    @Column
    private String name;

    @Column
    private String title;

    @Column
    private LocalDateTime uploadDateTime;

    @Column
    private Long viewsCount;

    @Column
    private String description;


}