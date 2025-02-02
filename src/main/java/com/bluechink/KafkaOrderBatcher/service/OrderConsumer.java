package com.bluechink.KafkaOrderBatcher.service;


import com.bluechink.KafkaOrderBatcher.entity.Order;
import com.bluechink.KafkaOrderBatcher.repository.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    private final OrderRepository orderRepository;
    private final ObjectMapper objectMapper;

    public OrderConsumer(OrderRepository orderRepository, ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "orders", groupId = "order-consumer-group")
    public void consumeOrder(String orderMessage) {
        try {
            System.out.println("Received Order: " + orderMessage);
            Order order = objectMapper.readValue(orderMessage, Order.class);
            orderRepository.save(order);
            System.out.println("Saved order: " + order);
        } catch (Exception e) {
            System.err.println("Error processing order: " + e.getMessage());
        }
    }
}