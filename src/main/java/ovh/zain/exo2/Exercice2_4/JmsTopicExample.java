package ovh.zain.exo2.Exercice2_4;

import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

public class JmsTopicExample implements MessageListener {
    private Session serverSession;
    private MessageProducer replyProducer;

    public static void main(String[] args) throws URISyntaxException, Exception {
        JmsTopicExample jmsTopicExample = new JmsTopicExample();
        jmsTopicExample.sendReqOnTempTopic();
    }

    public void sendReqOnTempTopic() throws URISyntaxException, Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI(
                "broker:(tcp://localhost:61616)"));
        broker.start();
        Connection serverConnection = null;
        Connection clientConnection = null;
        try {
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    "tcp://localhost:61616");

            serverConnection = connectionFactory.createConnection();
            serverConnection.setClientID("serverTempTopic");
            serverSession = serverConnection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);

            replyProducer = serverSession.createProducer(null);
            Topic requestDestination = serverSession.createTopic("SomeTopic");

            //Server is listening for queries
            final MessageConsumer requestConsumer = serverSession
                    .createConsumer(requestDestination);
            requestConsumer.setMessageListener(this);
            serverConnection.start();

            // Client sends a query to topic 'SomeTopic'
            clientConnection = connectionFactory.createConnection();
            clientConnection.setClientID("clientTempTopic");
            Session clientSession = clientConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            clientConnection.start();
            Destination replyDestination = clientSession.createTemporaryTopic();

            MessageProducer requestProducer = clientSession.createProducer(requestDestination);
            //Create a client listener on the temporary topic
            MessageConsumer replyConsumer = clientSession.createConsumer(replyDestination);

            TextMessage requestMessage = clientSession.createTextMessage("[CLIENT]: What is the answer to life, the universe and everything?");
            //Server is going to send the reply to the temporary topic
            requestMessage.setJMSReplyTo(replyDestination);
            requestProducer.send(requestMessage);

            System.out.println("[CLIENT] Sent request " + requestMessage.toString());

            //Read the answer from temporary queue.
            Message msg = replyConsumer.receive(5000);
            TextMessage replyMessage = (TextMessage)msg;
            System.out.println("[CLIENT] Received reply " + replyMessage.toString());
            System.out.println("[CLIENT] Received answer: " + replyMessage.getText());

            replyConsumer.close();

            clientSession.close();
            serverSession.close();
        } finally {
            if (clientConnection != null) {
                clientConnection.close();
            }
            if (serverConnection != null) {
                serverConnection.close();
            }
            broker.stop();
        }
    }

    //Server receives a query and sends reply to temporary topic set in JMSReplyTo
    public void onMessage(Message message) {
        System.out.println("Received message");
        try {
            TextMessage requestMessage = (TextMessage)message;

            System.out.println("[SERVER] Received request." + requestMessage.toString());
            System.out.println("[SERVER] Received question." + requestMessage.getText());

            Destination replyDestination = requestMessage.getJMSReplyTo();
            TextMessage replyMessage = serverSession.createTextMessage("[SERVER]: Answer is 42");

            replyMessage.setJMSCorrelationID(requestMessage.getJMSMessageID());

            replyProducer = serverSession.createProducer(replyDestination);
            replyProducer.send(replyMessage);

            System.out.println("[SERVER] Sent reply.");
            System.out.println(replyMessage.toString());
        } catch (JMSException e) {
            System.out.println(e);
        }
    }
}