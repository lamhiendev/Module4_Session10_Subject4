package demo.notificationservice.consumer;

import demo.notificationservice.dto.OrderEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderNotificationConsumer {

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