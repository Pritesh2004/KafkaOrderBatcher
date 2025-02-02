package com.bluechink.KafkaOrderBatcher.batch;

import com.bluechink.KafkaOrderBatcher.entity.Order;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import java.util.Queue;

public class OrderReader implements ItemReader<Order> {

    private final Queue<Order> orderQueue;


    public OrderReader(Queue<Order> orderQueue) {
        this.orderQueue = orderQueue;
    }

    @Override
    public Order read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return orderQueue.poll();
    }
}
