package demo.pharmacyservice.service;

import demo.pharmacyservice.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducerService {

    private static final Logger logger = LoggerFactory.getLogger(OrderProducerService.class);
    private static final String TOPIC = "medicine-stock-events";

    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    public OrderEvent sendOrderEvent(String medicineId, int quantity) {
        // 1. Khởi tạo đối tượng OrderEvent
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8);
        OrderEvent orderEvent = new OrderEvent(orderId, medicineId, quantity);

        kafkaTemplate.send(
                "medicine-stock-events",
                orderEvent.getMedicineId(),
                orderEvent
        ).whenComplete((result, ex) -> {

            if (ex != null) {
                System.out.println("❌ Gửi Kafka thất bại: " + ex.getMessage());
                ex.printStackTrace();
                return;
            }

            System.out.println(
                    "✅ Kafka đã nhận event. Order ID: "
                            + orderEvent.getOrderId()
                            + ", topic: "
                            + result.getRecordMetadata().topic()
                            + ", partition: "
                            + result.getRecordMetadata().partition()
                            + ", offset: "
                            + result.getRecordMetadata().offset()
            );
        });
        return orderEvent;
    }
}