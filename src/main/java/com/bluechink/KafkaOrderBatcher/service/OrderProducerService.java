package com.bluechink.KafkaOrderBatcher.service;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public OrderProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(String orderMessage) {
        kafkaTemplate.send(new ProducerRecord<>("orders", orderMessage));
    }
}
