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

import org.apache.commons.lang3.RandomStringUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 */
public class App {

    private static final long MILLI_PER_SECOND = 1000;

    private static void usage() {
        System.out.println("App numThread messageCount messageSize [timeoutMinutes=1]");
    }

    public static void main(String[] args) throws Exception {
        int numThreads, messageCount, messageSize, timeoutMinutes = 1;
        ExecutorService es = Executors.newCachedThreadPool();

        if (3 > args.length) {
            usage();
            return;
        }

        try {
            numThreads = new Integer(args[0]).intValue();
            messageCount = new Integer(args[1]).intValue();
            messageSize = new Integer(args[2]).intValue();
            if (args.length == 4) {
                timeoutMinutes = new Integer(args[3]).intValue();
            }
        } catch (NumberFormatException e) {
            usage();
            return;
        }

        System.out.format("            # of threads: %,8d%n", numThreads);
        System.out.format("    # of messages/thread: %,8d%n", messageCount);
        System.out.format("     total # of messages: %,8d%n", (messageCount * numThreads));
        System.out.format("            message size: %,8d%n", messageSize);
        System.out.format("           timeout (min): %,8d%n", timeoutMinutes);

        long start = System.currentTimeMillis();

        for (int i = 0; i < numThreads ; i++) {
            es.execute(new HelloWorldProducer(messageCount, messageSize));
            es.execute(new HelloWorldConsumer(messageCount));
        }

        es.shutdown();

        if (es.awaitTermination(timeoutMinutes, TimeUnit.MINUTES)) {
            long stop = System.currentTimeMillis();
            long millis = stop - start;

            System.out.format( "            milliseconds: %,8d%n", millis);
            System.out.format("       # messages/second: %,8d%n",
                    messageCount * numThreads * MILLI_PER_SECOND / millis);
        }
        else {
            System.out.format("Execution took longer than %d minute%s, aborting.%n", timeoutMinutes,
                    timeoutMinutes == 1 ? "" : "s");
            System.exit(1);
        }
    }

    public static void thread(ExecutorService es, Runnable runnable) {
        es.execute(runnable);
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

                for (int i=0 ; i<messageCount ; i++) {
                    // Create a messages
                    String text = RandomStringUtils.random(messageSize);
                    TextMessage message = session.createTextMessage(text);

                    // Tell the producer to send the message
                    // System.out.println("Sending [" + text + "]");
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

                    //System.out.println("Thread: " + Thread.currentThread().getName() + " Received: [" + i + "] ");
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