package com.bluechink.KafkaOrderBatcher.kafka;

import com.bluechink.KafkaOrderBatcher.entity.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class KafkaTestProducer {

    public static void main(String[] args) {
        // Kafka producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);  // We'll serialize the Order to a JSON string

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ObjectMapper objectMapper = new ObjectMapper();

        // Send 10 test orders
        for (int i = 1; i <= 10; i++) {
            Order order = new Order();
            order.setOrderId(String.valueOf(i));
            order.setCustomerId("Customer " + i);
            order.setProductIds(List.of("Product " + i));
            order.setQuantities(List.of(1));
            order.setOrderStatus("PENDING");

            try {
                // Convert the Order object to JSON string
                String orderJson = objectMapper.writeValueAsString(order);

                // Send the Order message as a JSON string to the Kafka topic
                producer.send(new ProducerRecord<>("orders", String.valueOf(i), orderJson));
                System.out.println("Sent order: " + orderJson);
            } catch (JsonProcessingException e) {
                System.err.println("Error serializing order: " + e.getMessage());
            }
        }

        // Close the producer after sending all messages
        producer.close();
    }
}
