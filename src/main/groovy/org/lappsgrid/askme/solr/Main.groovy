package org.lappsgrid.askme.solr

import org.lappsgrid.eager.mining.api.Query
import org.lappsgrid.rabbitmq.topic.MailBox

//import org.lappsgrid.eager.mining.core.json.Serializer
import org.lappsgrid.serialization.Serializer

import org.lappsgrid.rabbitmq.Message
import org.lappsgrid.rabbitmq.topic.MessageBox
import org.lappsgrid.rabbitmq.topic.PostOffice
import groovy.util.logging.Slf4j


@Slf4j("logger")
class Main{
    static final String MBOX = 'solr.mailbox'
    static final String WEB_MBOX = 'web.mailbox'
    static final String HOST = "rabbitmq.lappsgrid.org"
    static final String EXCHANGE = "org.lappsgrid.query"
    static final PostOffice po = new PostOffice(EXCHANGE, HOST)
    MailBox box

    Main() {
        //super(EXCHANGE, MBOX)
    }

    void run(lock) {
        box = new MailBox(EXCHANGE, MBOX, HOST) {
            @Override
            void recv(String s) {
                Message message = Serializer.parse(s, Message)
                logger.info("Received message {}", message.getId())
                if (message.getCommand() == 'EXIT' || message.getCommand() == 'STOP') {
                    shutdown(lock)
                }
                else {
                    logger.info("Generating query from Message {}", message.getId())
                    Query query = Serializer.parse(Serializer.toJson(message.body), Query)

                    logger.info("Gathering solr documents")
                    GetSolrDocuments process = new GetSolrDocuments()

                    int number_of_documents = message.getCommand().toInteger()

                    Map result = process.answer(query, message.getId(), number_of_documents)
                    message.setBody(result)

                    logger.info("Processed query from Message {}, sending documents back to web", message.getId())
                    message.setRoute([WEB_MBOX])
                    message.setCommand('solr')
                    po.send(message)

                    logger.info("Message {} with solr documents sent back to web", message.getId())
                }

            }
        }
        synchronized(lock) { lock.wait() }
        box.close()
        po.close()
        logger.info("Solr service terminated")
    }

    void shutdown(Object lock){
        logger.info('Received shutdown message, terminating Solr service')
        synchronized(lock) { lock.notify() }

    }

    //Checks if:
    // 1) message body is empty
    // 2) command is empty (as of right now, default is 10) THIS IS NOW DONE IN WEB
    // TODO: check body to see it matches query format
    boolean checkMessage(Message message) {
        if (!(message.getBody())) {
            Map error_check = [:]
            error_check.origin = "Solr"
            error_check.messageId = message.getId()
            if (!message.getBody()) {
                logger.info('ERROR: Message has empty body')
                error_check.body = 'MISSING'
            }
            logger.info('Notifying Web service of error')
            Message error_message = new Message()
            error_message.setCommand('ERROR')
            error_message.setBody(error_check)
            error_message.route('web.mailbox')
            po.send(error_message)
            return false
        }
        return true
    }
    
    static void main(String[] args) {
        logger.info("Starting Solr service, awaiting Message containing query from Web module")
        Object lock = new Object()
        Thread.start {
            new Main().run(lock)
        }
    }
}
