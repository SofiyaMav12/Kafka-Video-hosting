package com.example.demokafka.subscribeNotification;

import com.example.demokafka.channel.MediaChannel;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import java.util.List;

@Getter
@Setter
@Entity
@Table(name = "subscriber")
public class Subscriber implements SpecificRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    private Long id;


    @Column
    private  String firstName;


    @Column

    private String lastName;


    @Column

    private List<MediaChannel> mediaChannels;


    @Column

    private String email;

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