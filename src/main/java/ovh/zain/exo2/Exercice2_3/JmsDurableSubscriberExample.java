package ovh.zain.exo2.Exercice2_3;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

import javax.jms.*;
import java.net.URI;
import java.net.URISyntaxException;

public class JmsDurableSubscriberExample {
    public static void main(String[] args) throws URISyntaxException, Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI("broker:(tcp://localhost:61616)"));
        broker.start();
        Connection connection = null;
        try {
            // Producer
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
            connection = connectionFactory.createConnection();
            connection.setClientID("DurabilityTest");
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic("customerTopic");

            // Publish
            String payload = "Meessage for durable subscribers";
            TextMessage msg = session.createTextMessage(payload);
            MessageProducer publisher = session.createProducer(topic);
            System.out.println("Sending text '" + payload + "'");
            publisher.send(msg, javax.jms.DeliveryMode.PERSISTENT, javax.jms.Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

            // Consumer1 subscribes to customerTopic
            MessageConsumer consumer1 = session.createDurableSubscriber(topic, "consumer1", "", false);

            // Consumer2 subscribes to customerTopic
            MessageConsumer consumer2 = session.createDurableSubscriber(topic, "consumer2", "", false);


            connection.start();
            System.out.println("Connection started");

            msg = (TextMessage) consumer1.receive();
            System.out.println("Consumer1 receives " + msg.getText());


            msg = (TextMessage) consumer2.receive();
            System.out.println("Consumer2 receives " + msg.getText());

            session.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
            broker.stop();
        }
    }
}