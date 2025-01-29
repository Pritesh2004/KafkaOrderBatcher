package com.bluechink.KafkaOrderBatcher.service;

import com.bluechink.KafkaOrderBatcher.entity.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumerService {

    private final OrderProcessingService orderProcessingService;
    private final ObjectMapper objectMapper;

    @Autowired
    public OrderConsumerService(OrderProcessingService orderProcessingService) {
        this.orderProcessingService = orderProcessingService;
        this.objectMapper = new ObjectMapper();
    }

    @KafkaListener(topics = "orders", groupId = "order-consumer-group")
    public void consume(String message) {
        try {
            System.err.println("Received order message: " + message);  // Log the message
            Order order = parseOrder(message);
            orderProcessingService.processOrder(order);
        } catch (Exception e) {
            System.err.println("Failed to parse order message: " + e.getMessage());
        }
    }


    private Order parseOrder(String message) throws Exception {
        return objectMapper.readValue(message, Order.class);
    }
}
