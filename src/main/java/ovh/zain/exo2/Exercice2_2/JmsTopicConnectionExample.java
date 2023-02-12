package ovh.zain.exo2.Exercice2_2;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

import javax.jms.*;
import java.net.URI;
import java.net.URISyntaxException;

public class JmsTopicConnectionExample {
    public static void main(String[] args) throws URISyntaxException, Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI("broker:(tcp://localhost:61616)"));
        broker.setPersistent(true);
        broker.start();
        TopicConnection topicConnection = null;
        try {
            // Producer
            TopicConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
            topicConnection = connectionFactory.createTopicConnection();
            topicConnection.setClientID("JMSTOPIC");

            TopicSession topicConsumerSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = topicConsumerSession.createTopic("customerTopic");

            // Consumer1 subscribes to customerTopic
            MessageConsumer consumer1 = topicConsumerSession.createSubscriber(topic);
            consumer1.setMessageListener(new ConsumerMessageListener("Consumer created from TopicSession 1"));

            // Consumer2 subscribes to customerTopic
            MessageConsumer consumer2 = topicConsumerSession.createSubscriber(topic);
            consumer2.setMessageListener(new ConsumerMessageListener("Consumer created from TopicSession 2"));

            topicConnection.start();

            // Publish
            TopicSession topicPublisherSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            String payload = "This message will be sent using a TopicPublisher";
            Message msg = topicPublisherSession.createTextMessage(payload);
            TopicPublisher publisher = topicPublisherSession.createPublisher(topic);
            System.out.println("Sending text '" + payload + "'");
            publisher.publish(msg);


            Thread.sleep(3000);
            topicPublisherSession.close();
            topicConsumerSession.close();
        } finally {
            if (topicConnection != null) {
                topicConnection.close();
            }
            broker.stop();
        }
    }
}