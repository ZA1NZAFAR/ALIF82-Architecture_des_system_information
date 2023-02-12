package ovh.zain.exo2.Exercice2_1;


import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

public class JmsTopicExample {
    public static void main(String[] args) throws URISyntaxException, Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI(
                "broker:(tcp://localhost:61616)"));
        broker.start();
        Connection connection = null;
        try {
            // Producer
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    "tcp://localhost:61616");
            connection = connectionFactory.createConnection();
            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic("customerTopic");

            // Consumer1 subscribes to customerTopic
            MessageConsumer consumer1 = session.createConsumer(topic);
            consumer1.setMessageListener(new ConsumerMessageListener("Consumer1"));

            // Consumer2 subscribes to customerTopic
            MessageConsumer consumer2 = session.createConsumer(topic);
            consumer2.setMessageListener(new ConsumerMessageListener("Consumer2"));

            connection.start();

            // Publish
            String payload = "This message is for all";
            Message msg = session.createTextMessage(payload);
            MessageProducer producer = session.createProducer(topic);
            System.out.println("Sending text '" + payload + "'");
            producer.send(msg);

            Thread.sleep(3000);
            session.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
            broker.stop();
        }
    }
}