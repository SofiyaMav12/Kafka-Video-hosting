package com.example.demokafka.channel;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(name = "media_channel")
public class MediaChannel implements SpecificRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    private Long id;

    @Column
    private String name;

    @Column
    private String createdBy;

    @Column
    private LocalDateTime createdDate;

    @Override
    public void put(int i, Object o) {

    }

    @Override
    public Object get(int i) {
        return null;
    }

    @Override
    public Schema getSchema() {
        return null;
    }
}