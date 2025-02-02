package com.bluechink.KafkaOrderBatcher.entity;


import jakarta.persistence.*;
import lombok.*;

import java.util.List;

@Entity
@Data
@Table(name = "orders") // Rename the table
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String orderId;
    private String customerId;
    @ElementCollection
    private List<String> productIds;

    @ElementCollection
    private List<Integer> quantities;
    private String orderStatus;
}
