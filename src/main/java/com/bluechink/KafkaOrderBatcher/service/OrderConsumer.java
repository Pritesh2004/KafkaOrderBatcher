package com.bluechink.KafkaOrderBatcher.service;

import com.bluechink.KafkaOrderBatcher.entity.Order;
import com.bluechink.KafkaOrderBatcher.repository.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class OrderConsumer {

    private final OrderRepository orderRepository;
    private final ObjectMapper objectMapper;
    private final JobLauncher jobLauncher;
    private final Job processOrdersJob;

    // Queue to hold orders for batch processing
    private final List<Order> orderQueue = new ArrayList<>();

    public OrderConsumer(OrderRepository orderRepository, ObjectMapper objectMapper, JobLauncher jobLauncher, Job processOrdersJob) {
        this.orderRepository = orderRepository;
        this.objectMapper = objectMapper;
        this.jobLauncher = jobLauncher;
        this.processOrdersJob = processOrdersJob;
    }

    @KafkaListener(topics = "orders", groupId = "order-consumer-group")
    public void consumeOrder(String orderMessage) {
        try {
            System.out.println("Orders Recieved by Kafka - "+orderMessage);
            Order order = objectMapper.readValue(orderMessage, Order.class);
            orderQueue.add(order); // Add order to the queue

            // Trigger batch processing if queue size reaches a certain threshold
            if (orderQueue.size() >= 10) {
                processBatch();
            }
        } catch (Exception e) {
            System.err.println("Error processing order: " + e.getMessage());
        }
    }

    private void processBatch() {
        try {
            // Create job parameters with the orders in the queue
            JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
            jobParametersBuilder.addString("orderQueue", orderQueue.toString());  // You can serialize this list to JSON if needed

            // Trigger Spring Batch job with the orders in the queue
            JobExecution jobExecution = jobLauncher.run(processOrdersJob, jobParametersBuilder.toJobParameters());

            if (jobExecution.getStatus().isUnsuccessful()) {
                System.err.println("Job failed to execute");
            } else {
                System.out.println("Batch processing completed successfully");
            }

            // Clear the queue after processing
            orderQueue.clear();
        } catch (Exception e) {
            System.err.println("Error executing batch job: " + e.getMessage());
        }
    }
}
