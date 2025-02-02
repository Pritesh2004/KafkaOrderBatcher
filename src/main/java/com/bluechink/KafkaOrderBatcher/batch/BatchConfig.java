package com.bluechink.KafkaOrderBatcher.batch;

import com.bluechink.KafkaOrderBatcher.entity.Order;
import com.bluechink.KafkaOrderBatcher.repository.OrderRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Collections;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private final OrderRepository orderRepository;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public BatchConfig(OrderRepository orderRepository, JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        this.orderRepository = orderRepository;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    @Bean
    public ItemReader<Order> orderReader() {
        RepositoryItemReader<Order> reader = new RepositoryItemReader<>();
        reader.setRepository(orderRepository);
        reader.setMethodName("findAll");
        reader.setSort(Collections.singletonMap("id", Sort.Direction.ASC));
        return reader;
    }

    @Bean
    public ItemProcessor<Order, Order> orderProcessor() {
        return order -> {
            System.out.println("Processing order: " + order);
            return order;
        };
    }

    @Bean
    public ItemWriter<Order> orderWriter() {
        RepositoryItemWriter<Order> writer = new RepositoryItemWriter<>();
        writer.setRepository(orderRepository);
        writer.setMethodName("save");
        return writer;
    }

    @Bean
    public Step orderProcessingStep() {
        return new StepBuilder("orderProcessingStep", jobRepository)
                .<Order, Order>chunk(5, transactionManager)
                .reader(orderReader())
                .processor(orderProcessor())
                .writer(orderWriter())
                .build();
    }

    @Bean
    public Job orderJob() {
        return new JobBuilder("orderJob", jobRepository)
                .start(orderProcessingStep())
                .build();
    }
}