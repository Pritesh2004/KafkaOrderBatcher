package com.bluechink.KafkaOrderBatcher.config;

import com.bluechink.KafkaOrderBatcher.entity.Order;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private final List<Order> orderQueue = new ArrayList<>();

    @Bean
    public Job orderProcessingJob(JobRepository jobRepository, Step orderStep) {
        return new JobBuilder("orderProcessingJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(orderStep)
                .build();
    }

    @Bean
    public Step orderStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                          ItemReader<Order> reader, ItemProcessor<Order, Order> processor, ItemWriter<Order> writer) {
        return new StepBuilder("orderStep", jobRepository)
                .<Order, Order>chunk(10, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .transactionManager(transactionManager)  // Ensure transaction manager is being used
                .build();
    }


    @Bean
    public ItemReader<Order> orderItemReader() {
        return new ListItemReader<>(orderQueue);
    }

    @Bean
    public ItemProcessor<Order, Order> orderItemProcessor() {
        return order -> {
            // You can add any validation or transformation logic here
            return order;
        };
    }

    @Bean
    public JdbcBatchItemWriter<Order> orderItemWriter(DataSource dataSource) {
        JdbcBatchItemWriter<Order> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("INSERT INTO orders (order_id, customer_id, product_ids, quantities, order_status) VALUES (?, ?, ?, ?, ?)");
        writer.setItemPreparedStatementSetter((order, ps) -> {
            ps.setString(1, order.getOrderId());
            ps.setString(2, order.getCustomerId());
            ps.setString(3, order.getProductIds());
            ps.setString(4, order.getQuantities());
            ps.setString(5, order.getOrderStatus());
        });
        return writer;
    }

    public void addOrderToQueue(Order order) {
        synchronized (orderQueue) {
            orderQueue.add(order);
        }
    }
}
