package ovh.zain.exo1.Exercice1_4_browse_queue;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

import javax.jms.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;

public class JmsBrowseQueueExample {
    public static void main(String[] args) throws URISyntaxException, Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI("broker:(tcp://localhost:61616)"));
        broker.start();
        Connection connection = null;
        try {
            // Producer
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
            connection = connectionFactory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("messagesQueue");

            String basePayload = "A";
            MessageProducer producer = session.createProducer(queue);
            for (int i = 0; i < 4; i++) {
                String payload = basePayload + i;
                Message msg = session.createTextMessage(payload);
                System.out.println("Sending text '" + payload + "'");
                producer.send(msg);
            }

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            System.out.println("Browse through the elements in queue");
            QueueBrowser browser = session.createBrowser(queue);
            Enumeration e = browser.getEnumeration();
            while (e.hasMoreElements()) {
                TextMessage message = (TextMessage) e.nextElement();
                System.out.println("Element inside = [" + message.getText() + "]");
            }
            System.out.println("Done");
            browser.close();

            TextMessage textMsg = (TextMessage) consumer.receive();
            System.out.println(textMsg);
            System.out.println("Received: " + textMsg.getText());

            textMsg = (TextMessage) consumer.receive();
            System.out.println(textMsg);
            System.out.println("Received: " + textMsg.getText());

            textMsg = (TextMessage) consumer.receive();
            System.out.println(textMsg);
            System.out.println("Received: " + textMsg.getText());

            textMsg = (TextMessage) consumer.receive();
            System.out.println(textMsg);
            System.out.println("Received: " + textMsg.getText());

            session.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
            broker.stop();
        }
    }

}