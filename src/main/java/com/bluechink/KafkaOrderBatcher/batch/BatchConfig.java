package com.bluechink.KafkaOrderBatcher.batch;

import com.bluechink.KafkaOrderBatcher.entity.Order;
import com.bluechink.KafkaOrderBatcher.repository.OrderRepository;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.LinkedList;
import java.util.Queue;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private final Queue<Order> orderQueue = new LinkedList<>();


    private final OrderRepository orderRepository;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public BatchConfig(OrderRepository orderRepository, JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        this.orderRepository = orderRepository;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    @Bean
    public Job processOrdersJob(JobRepository jobRepository, Step processOrdersStep) {
        return new JobBuilder("processOrdersJob", jobRepository)
                .start(processOrdersStep)
                .build();
    }

    @Bean
    public Step processOrdersStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("processOrdersStep", jobRepository)
                .<Order, Order>chunk(10, transactionManager)
                .reader(orderReader())
                .processor(orderProcessor())
                .writer(orderWriter())
                .faultTolerant()  //Ensures errors don't stop the batch
                .build();
    }

    @Bean
    public OrderReader orderReader() {
        return new OrderReader(orderQueue);
    }

    @Bean
    public ItemProcessor<Order, Order> orderProcessor() {
        return order -> order;
    }

    @Bean
    public OrderWriter orderWriter() {
        return new OrderWriter(orderRepository);
    }

    public void addOrderToQueue(Queue<Order> orders) {
        synchronized (orderQueue) {
            orderQueue.addAll(orders);
        }
    }
}
