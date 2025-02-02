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

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private final OrderRepository orderRepository;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final EntityManagerFactory entityManagerFactory;

    public BatchConfig(OrderRepository orderRepository, JobRepository jobRepository, PlatformTransactionManager transactionManager, EntityManagerFactory entityManagerFactory) {
        this.orderRepository = orderRepository;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.entityManagerFactory = entityManagerFactory;
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
                .writer(orderWriter()) // Spring will handle this bean automatically
                .build();
    }

    @Bean
    public ListItemReader<Order> orderReader() {
        return new ListItemReader<>(orderRepository.findAll());
    }

    @Bean
    public ItemProcessor<Order, Order> orderProcessor() {
        return order -> order; // Pass-through processor, customize if needed
    }

    @Bean
    public OrderWriter orderWriter() {
        return new OrderWriter(orderRepository); // Using the constructor directly
    }
}
