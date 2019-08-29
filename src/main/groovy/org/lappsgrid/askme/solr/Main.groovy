package org.lappsgrid.askme.solr

import groovy.transform.CompileStatic
import groovy.transform.TypeChecked
import org.lappsgrid.askme.core.Configuration
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
@CompileStatic
@Slf4j("logger")
class Main{
    static final Configuration config = new Configuration()

    final PostOffice po = new PostOffice(config.EXCHANGE, config.HOST)
    MailBox box

    Main() {
    }

    void run(lock) {
        box = new MailBox(config.EXCHANGE, 'solr.mailbox', config.HOST) {
            @Override
            void recv(String s) {
                Message message = Serializer.parse(s, Message)
                String command = message.getCommand()
                String id = message.getId()
                if (command == 'EXIT' || command == 'STOP') {
                    logger.info('Received shutdown message, terminating Solr service')
                    synchronized(lock) { lock.notify() }
                }
                else if(command == 'PING') {
                    logger.info('Received PING message from and sending response back to {}', message.route[0])
                    Message response = new Message()
                    response.setBody('solr.mailbox')
                    response.setCommand('PONG')
                    response.setRoute(message.route)
                    logger.info('Response PONG sent to {}', response.route[0])
                    Main.this.po.send(response)
                }
                else {
                    logger.info('Received Message {}', id)
                    logger.info("Generating query from received Message {}", id)
                    String destination = message.route[0] ?: 'the void'
                    //TODO if the query has not been set we should return an errro message
                    // as otherwise we will eventually get a NPE.
                    String json = message.get("query")
                    Query query = Serializer.parse(json, Query)
                    logger.info("Gathering solr documents for query '{}'", query.query)
                    GetSolrDocuments process = new GetSolrDocuments()
                    //FIXME The number of documents should be obtained from the params.
                    int nDocuments = 100
                    Map result = process.answer(query, id, nDocuments)
                    logger.info("Processed query from Message {}",id)
                    message.setBody(result)
                    po.send(message)
                    logger.info("Message {} with solr documents sent to {}", id, destination)
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
