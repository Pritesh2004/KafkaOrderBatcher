package com.bluechink.KafkaOrderBatcher.entity;


import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    private String orderId;
    private String customerId;
    private String productIds;
    private String quantities;
    private String orderStatus;

    public Order() {
    }

    public Order(String orderId, String customerId, String productIds, String quantities, String orderStatus) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.productIds = productIds;
        this.quantities = quantities;
        this.orderStatus = orderStatus;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getProductIds() {
        return productIds;
    }

    public void setProductIds(String productIds) {
        this.productIds = productIds;
    }

    public String getQuantities() {
        return quantities;
    }

    public void setQuantities(String quantities) {
        this.quantities = quantities;
    }

    public String getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }
}
