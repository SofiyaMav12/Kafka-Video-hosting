package com.example.demokafka.subscribeNotification;

import com.example.demokafka.subscribeNotification.Subscriber;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SubscriberRepository extends JpaRepository<Subscriber, Long> {
}