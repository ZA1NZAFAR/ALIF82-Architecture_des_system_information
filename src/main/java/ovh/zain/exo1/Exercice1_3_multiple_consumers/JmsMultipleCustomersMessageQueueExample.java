package ovh.zain.exo1.Exercice1_3_multiple_consumers;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

import javax.jms.*;
import java.net.URI;
import java.net.URISyntaxException;

public class JmsMultipleCustomersMessageQueueExample {
    public static void main(String[] args) throws URISyntaxException, Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI("broker:(tcp://localhost:61616)"));
        broker.start();
        Connection connection = null;
        try {
            // Producer
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
            connection = connectionFactory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("customerQueue");

            // Consumer
            for (int i = 0; i < 4; i++) {
                MessageConsumer consumer = session.createConsumer(queue);
                consumer.setMessageListener(new ConsumerMessageListener("Consumer " + i));
            }
            connection.start();

            String basePayload = "This message will be consumed by multiple consumers";
            MessageProducer producer = session.createProducer(queue);
            for (int i = 0; i < 10; i++) {
                String payload = basePayload + i;
                Message msg = session.createTextMessage(payload);
                System.out.println("-> Sending text '" + payload + "'");
                producer.send(msg);
            }

            Thread.sleep(1000);
            session.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
            broker.stop();
        }
    }

}