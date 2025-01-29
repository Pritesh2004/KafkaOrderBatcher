package com.bluechink.KafkaOrderBatcher.controller;

import com.bluechink.KafkaOrderBatcher.entity.Order;
import com.bluechink.KafkaOrderBatcher.service.OrderProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
@RestController
public class OrderController {

    private final OrderProducerService producerService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public OrderController(OrderProducerService producerService, KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.producerService = producerService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping("/orders")
    public ResponseEntity<String> sendOrderMessage(@RequestBody Order order) {
        try {
            String orderMessage = objectMapper.writeValueAsString(order);
            kafkaTemplate.send("orders", orderMessage);
            return ResponseEntity.ok("Order message sent");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error sending message: " + e.getMessage());
        }
    }
}
