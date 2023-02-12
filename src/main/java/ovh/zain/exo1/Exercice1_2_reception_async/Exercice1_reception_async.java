package ovh.zain.exo1.Exercice1_2_reception_async;


import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

public class Exercice1_reception_async {
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
            String payload = "This message will be consumed asynchronously.";
            Message msg = session.createTextMessage(payload);
            MessageProducer producer = session.createProducer(queue);
            System.out.println("Sending text '" + payload + "'");
            producer.send(msg);

            // Consumer
            MessageConsumer consumer = session.createConsumer(queue);
            consumer.setMessageListener(new ConsumerMessageListener("Consumer"));
            connection.start();
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