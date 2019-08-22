package org.lappsgrid.askme.solr

import org.lappsgrid.askme.core.api.Query
import org.lappsgrid.rabbitmq.topic.MailBox
import org.lappsgrid.serialization.Serializer
import org.lappsgrid.rabbitmq.Message
import org.lappsgrid.rabbitmq.topic.PostOffice
import groovy.util.logging.Slf4j

/**
 * TODO:
 * 1) Update imports to phase out eager (waiting on askme-core pom)
 * 2) Add exceptions / case statements to recv method?
 */

@Slf4j("logger")
class Main{
    static final String MBOX = 'solr.mailbox'
    static final String WEB_MBOX = 'web.mailbox'
    static final String HOST = "rabbitmq.lappsgrid.org"
    static final String EXCHANGE = "org.lappsgrid.query"
    static final PostOffice po = new PostOffice(EXCHANGE, HOST)
    MailBox box

    Main() {
    }

    void run(lock) {
        box = new MailBox(EXCHANGE, MBOX, HOST) {
            @Override
            void recv(String s) {
                Message message = Serializer.parse(s, Message)
                String command = message.getCommand()
                String id = message.getId()
                Object params = message.getParameters()
                if (command == 'EXIT' || command == 'STOP') {
                    logger.info('Received shutdown message, terminating Solr service')
                    synchronized(lock) { lock.notify() }
                }
                else if(command == 'PING') {
                    String origin = message.getBody()
                    logger.info('Received PING message from and sending response back to {}', origin)
                    Message response = new Message()
                    response.setBody(MBOX)
                    response.setCommand('PONG')
                    response.setRoute([origin])
                    po.send(response)
                    logger.info('Response PONG sent to {}', origin)
                }
                else {
                    logger.info('Received Message {}', id)
                    logger.info("Generating query from received Message {}", id)
                    Query query = Serializer.parse(Serializer.toJson(message.body), Query)
                    logger.info("Gathering solr documents")
                    GetSolrDocuments process = new GetSolrDocuments()
                    int number_of_documents = command.toInteger()
                    Map result = process.answer(query, id, number_of_documents)
                    logger.info("Processed query from Message {}, sending documents back to web",id)
                    Message response = new Message()
                    response.setCommand('solr')
                    response.setRoute([WEB_MBOX])
                    response.setBody(result)
                    response.setId(id)
                    response.setParameters(params)
                    po.send(response)
                    logger.info("Message {} with solr documents sent back to web", id)
                }
            }
        }
        synchronized(lock) { lock.wait() }
        po.close()
        box.close()
        logger.info("Solr service terminated")
    }
    static void main(String[] args) {
        logger.info("Starting Solr service")
        Object lock = new Object()
        Thread.start {
            new Main().run(lock)
        }
    }

    /**
     * CODE IS NOT CURRENTLY USED
     *
    //Checks if:
    // 1) message body is empty
    // 2) command is empty (as of right now, default is 10) THIS IS NOW DONE IN WEB
    // TODO: fix and integrate with new run and recv message
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


 **/
}