package org.lappsgrid.eager.askme.solr

import org.lappsgrid.eager.mining.api.Query
import org.lappsgrid.rabbitmq.Message
import org.lappsgrid.rabbitmq.topic.MessageBox
import org.lappsgrid.rabbitmq.topic.PostOffice
import groovy.util.logging.Slf4j
import org.lappsgrid.eager.mining.core.json.Serializer


@Slf4j("logger")
class Main extends MessageBox{
    static final String MBOX = 'solr.mailbox'
    static final String WEB_MBOX = 'web.mailbox'
    static final String HOST = "rabbitmq.lappsgrid.org"
    static final String EXCHANGE = "org.lappsgrid.query"
    static final PostOffice po = new PostOffice(EXCHANGE, HOST)

    Main(){
        super(EXCHANGE, MBOX)
    }

    void recv(Message message){
        logger.info("Received message {}", message.getId())

        logger.info("Generating query from Message {}", message.getId())
        Query query = Serializer.parse(Serializer.toJson(message.body), Query)

        logger.info("Gathering solr documents")
        GetSolrDocuments process = new GetSolrDocuments()
        Map result = process.answer(query, message.getId())
        message.setBody(result)

        logger.info("Processed query from Message {}, sending documents back to web", message.getId())
        message.setRoute([WEB_MBOX])
        message.setCommand('solr')
        po.send(message)
        logger.info("Message {} with solr documents sent back to web", message.getId())

    }
    
    static void main(String[] args) {
        logger.info("Starting solr access module, awaiting message from web module")
        new Main()
    }
}
