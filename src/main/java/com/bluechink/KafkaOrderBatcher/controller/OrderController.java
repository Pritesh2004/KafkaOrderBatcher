package com.bluechink.KafkaOrderBatcher.controller;


import com.bluechink.KafkaOrderBatcher.entity.Order;
import com.bluechink.KafkaOrderBatcher.service.OrderProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    @Autowired
    private OrderProducer orderProducer;

    @PostMapping
    public String placeOrder(@RequestBody Order order) {
        orderProducer.sendOrder(order);
        return "Order sent to Kafka!";
    }
}
