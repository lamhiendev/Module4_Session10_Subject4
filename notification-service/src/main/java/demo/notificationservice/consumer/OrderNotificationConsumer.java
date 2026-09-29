package demo.notificationservice.consumer;

import demo.notificationservice.dto.OrderEvent;
import jakarta.annotation.PostConstruct;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderNotificationConsumer {
    @PostConstruct
    public void test() {
        System.out.println("🔥 ORDER NOTIFICATION CONSUMER CREATED");
    }
    @KafkaListener(
            topics = "medicine-stock-events",
            groupId = "notification-service-group"
    )
    public void consume(OrderEvent event) {

        System.out.println("========== NOTIFICATION SERVICE ==========");
        System.out.println(
                "Hóa đơn cho đơn hàng " +
                        event.getOrderId() +
                        " đã được gửi tới khách hàng"
        );
    }
}