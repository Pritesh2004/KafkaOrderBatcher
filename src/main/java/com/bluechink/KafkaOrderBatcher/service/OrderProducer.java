package com.bluechink.KafkaOrderBatcher.service;


import com.bluechink.KafkaOrderBatcher.entity.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@EnableKafka
public class OrderProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private static final String TOPIC = "orders";

    public void sendOrder(Order order) {
        try {
            // Convert order object to JSON string
            String orderJson = objectMapper.writeValueAsString(order);

            // Send order message to Kafka topic
            kafkaTemplate.send(TOPIC, order.getOrderId(), orderJson);

            System.out.println("Order sent to Kafka: " + orderJson);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}