package com.bluechink.KafkaOrderBatcher.repository;

import com.bluechink.KafkaOrderBatcher.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderRepository extends JpaRepository<Order, Long> {
}
