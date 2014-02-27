package org.motech;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * Hello world!
 */
public class App {

    private static void usage() {
        System.out.println("App numThread messageCount messageSize");
    }

    public static void main(String[] args) throws Exception {
        int numThreads = new Integer(args[0]).intValue();
        int messageCount = new Integer(args[1]).intValue();
        int messageSize = new Integer(args[2]).intValue();

        if (3 != args.length) {
            usage();
        } else {
            for (int i = 0; i < messageCount ; i++) {
                thread(new HelloWorldProducer(messageCount, messageSize), false);
                thread(new HelloWorldConsumer(messageCount), false);
            }
        }
    }

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }

    public static class HelloWorldProducer implements Runnable {
        int messageCount;
        int messageSize;

        public HelloWorldProducer(int messageCount, int messageSize) {
            this.messageCount = messageCount;
            this.messageSize = messageSize;
        }

        public void run() {
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue("TEST.FOO");

                // Create a MessageProducer from the Session to the Topic or Queue
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                for (int i=0 ; i<1000 ; i++) {
                    // Create a messages
                    String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
                    TextMessage message = session.createTextMessage(text);

                    // Tell the producer to send the message
                    System.out.println("Sent message: "+ message.hashCode() + " : " + Thread.currentThread().getName());
                    producer.send(message);
                }

                // Clean up
                session.close();
                connection.close();
            }
            catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }
    }

    public static class HelloWorldConsumer implements Runnable, ExceptionListener {
        int messageCount;

        public HelloWorldConsumer(int messageCount) {
            this.messageCount = messageCount;
        }

        public void run() {
            try {

                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                connection.setExceptionListener(this);

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue("TEST.FOO");

                // Create a MessageConsumer from the Session to the Topic or Queue
                MessageConsumer consumer = session.createConsumer(destination);

                for (int i = 0 ; i < this.messageCount; i++) {

                    // Wait for a message
                    Message message = consumer.receive();

                    System.out.println("Thread: " + Thread.currentThread().getName() + " Received: [" + i + "] " + message);
                }

                consumer.close();
                session.close();
                connection.close();
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }

        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
        }
    }
}