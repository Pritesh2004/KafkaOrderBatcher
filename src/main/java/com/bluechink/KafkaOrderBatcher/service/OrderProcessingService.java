package com.bluechink.KafkaOrderBatcher.service;

import com.bluechink.KafkaOrderBatcher.entity.Order;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class OrderProcessingService {

    private final JobLauncher jobLauncher;
    private final Job orderProcessingJob;
    private final List<Order> orderBuffer = new ArrayList<>();

    @Autowired
    public OrderProcessingService(JobLauncher jobLauncher, Job orderProcessingJob) {
        this.jobLauncher = jobLauncher;
        this.orderProcessingJob = orderProcessingJob;
    }

    public void processOrder(Order order) {
        orderBuffer.add(order);

        // Trigger batch job when buffer reaches 10 orders (configurable)
        if (orderBuffer.size() >= 10) {
            executeBatchJob();
        }
    }

    private void executeBatchJob() {
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis()) // Ensures uniqueness
                    .toJobParameters();

            jobLauncher.run(orderProcessingJob, jobParameters);

            // Clear buffer after processing
            orderBuffer.clear();
        } catch (Exception e) {
            System.err.println("Error executing batch job: " + e.getMessage());
        }
    }
}
