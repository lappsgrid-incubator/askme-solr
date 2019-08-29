package org.lappsgrid.askme.solr

import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.lappsgrid.askme.core.Configuration
import org.lappsgrid.rabbitmq.Message
import org.lappsgrid.rabbitmq.RabbitMQ
import org.lappsgrid.rabbitmq.topic.MessageBox
import org.lappsgrid.rabbitmq.topic.PostOffice

/**
 *
 */
@Ignore
class MainTest {
    static Configuration config
    Main app
    Object lock
    final String MAILBOX = "solr-test-mailbox"

    @BeforeClass
    static void init() {
        config = new Configuration()
    }

    @Before
    void setup() {
        lock = new Object()
        app = new Main()
        Thread.start {
            println "Running the app"
            app.run(lock)
        }
        println "Setup complete."
    }

    @After
    void teardown() {
//        app.stop()
        app = null
    }

    @Test
    void ping() {
        boolean passed = false
        println "Creating the return mailbox."
        MessageBox box = new MessageBox(config.EXCHANGE, MAILBOX, config.HOST) {

            @Override
            void recv(Message message) {
                passed = message.command == "PONG"
                synchronized (lock) {
                    lock.notifyAll()
                }
            }
        }

        println "Opening the post office."
        PostOffice po = new PostOffice(config.EXCHANGE, config.HOST)
        println "creating the message"
        Message message = new Message()
                .command("PING")
                .route(Main.config.SOLR_MBOX)
                .route(MAILBOX)
        println "Sending the message"
        po.send(message)
        println "Waiting for the lock"
        synchronized (lock) {
            lock.wait(1000)
        }
        println "Closing post office and mailbox"
        po.close()
        box.close()
        assert passed
    }
}
