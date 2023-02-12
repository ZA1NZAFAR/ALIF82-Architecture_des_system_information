package ovh.zain.exo1.Exercice1_3_multiple_consumers;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

public class ConsumerMessageListener implements MessageListener {
    private final String consumerName;

    public ConsumerMessageListener(String consumerName) {
        System.out.println("Starting async consumer!");
        this.consumerName = consumerName;
    }

    public void onMessage(Message message) {
        TextMessage textMessage = (TextMessage) message;
        try {
            System.out.println("<- " + consumerName + " received " + textMessage.getText());
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

}