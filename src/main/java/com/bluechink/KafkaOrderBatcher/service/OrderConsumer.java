package com.bluechink.KafkaOrderBatcher.service;

import com.bluechink.KafkaOrderBatcher.batch.BatchConfig;
import com.bluechink.KafkaOrderBatcher.entity.Order;
import com.bluechink.KafkaOrderBatcher.repository.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

@Service
public class OrderConsumer {

    private final OrderRepository orderRepository;
    private final ObjectMapper objectMapper;
    private final JobLauncher jobLauncher;
    private final Job processOrdersJob;
    private final BatchConfig batchConfig;

    // Queue to hold orders for batch processing
    private final Queue<Order> orderQueue = new LinkedList<>();

    public OrderConsumer(OrderRepository orderRepository, ObjectMapper objectMapper, JobLauncher jobLauncher, Job processOrdersJob, BatchConfig batchConfig) {
        this.orderRepository = orderRepository;
        this.objectMapper = objectMapper;
        this.jobLauncher = jobLauncher;
        this.processOrdersJob = processOrdersJob;
        this.batchConfig = batchConfig;
    }

    @KafkaListener(topics = "orders", groupId = "order-consumer-group")
    public void consumeOrder(String orderMessage) {
        try {
            System.out.println("Orders Recieved by Kafka - "+orderMessage);
            Order order = objectMapper.readValue(orderMessage, Order.class);
            orderQueue.add(order);

            if (orderQueue.size() >= 10) {
                processBatch();
            }
        } catch (Exception e) {
            System.err.println("Error processing order: " + e.getMessage());
        }
    }

    private void processBatch() {
        try {

            batchConfig.addOrderToQueue(orderQueue);
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp", System.currentTimeMillis())  // Unique parameter
                    .toJobParameters();
            JobExecution jobExecution = jobLauncher.run(processOrdersJob, jobParameters);

            if (jobExecution.getStatus().isUnsuccessful()) {
                System.err.println("Job failed to execute");
            } else {
                System.out.println("Batch processing completed successfully");
            }

            orderQueue.clear();
        } catch (Exception e) {
            System.err.println("Error executing batch job: " + e.getMessage());
        }
    }
}
