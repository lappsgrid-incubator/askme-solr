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
    static final String WEB_MBOX = 'web'
    static final String HOST = "rabbitmq.lappsgrid.org"
    static final String EXCHANGE = "org.lappsgrid.query"

    Main(){
        super(EXCHANGE, BOX)
    }


    void recv(Message message){
        Query query = Serializer.parse(Serializer.toJson(message.body), Query)
        logger.info("Received message, query is: {}",query)
        GetSolrDocuments process = new GetSolrDocuments()
        logger.info("Gathering solr documents")
        SolrDocumentList documents = process.answer(query)
        String result = processSolr(documents)
        message.setBody(result)
        logger.info("Processed query, sending documents back to web")




    }

    //Want to return a document list, but can't find nlp import - need to add to pom and reinstall
    String processSolr(SolrDocumentList sdl) {
        //DocumentProcessor d = new DocumentProcessor
        return sdl.toString()
    }




    
    static void main(String[] args) {
        new Main()
    }
}
