package org.lappsgrid.eager.askme.solr

import org.apache.solr.common.SolrDocumentList
import org.lappsgrid.eager.mining.api.Query
import org.lappsgrid.rabbitmq.Message
import org.lappsgrid.rabbitmq.topic.MessageBox
import org.lappsgrid.rabbitmq.topic.PostOffice
import groovy.util.logging.Slf4j
import org.lappsgrid.eager.mining.core.json.Serializer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit


/**
 *
 */
@Slf4j("logger")
class Main extends MessageBox{
    static final String BOX = 'solr.mailbox'
    static final String WEB_MBOX = 'web.mailbox'
    static final String HOST = "rabbitmq.lappsgrid.org"
    static final String EXCHANGE = "org.lappsgrid.query"
    PostOffice po = new PostOffice(EXCHANGE, HOST)


    Main(){
        super(EXCHANGE, BOX)
    }


    void recv(Message message){
        Query query = Serializer.parse(Serializer.toJson(message.body), Query)
        logger.debug("Received message, query is: {}",query)
        logger.debug("{}", Serializer.toJson(message.body))
        GetSolrDocuments process = new GetSolrDocuments()
        logger.info("Gathering solr documents")
        Map result = process.answer(query)
        logger.info("Result.query is {}", result.query)
        message.setBody(result)
        logger.info("Processed query, sending documents back to web")
        message.setRoute([WEB_MBOX])
        message.setCommand('solr')
        po.send(message)

    }



    
    static void main(String[] args) {
        new Main()
    }
}
