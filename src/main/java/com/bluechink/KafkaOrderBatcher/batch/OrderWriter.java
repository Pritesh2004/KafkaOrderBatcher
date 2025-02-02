package com.bluechink.KafkaOrderBatcher.batch;

import com.bluechink.KafkaOrderBatcher.entity.Order;
import com.bluechink.KafkaOrderBatcher.repository.OrderRepository;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OrderWriter implements ItemWriter<Order> {

    private final OrderRepository orderRepository;

    public OrderWriter(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Override
    public void write(Chunk<? extends Order> items) throws Exception {
        orderRepository.saveAll(items);
    }
}
