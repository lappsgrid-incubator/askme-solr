package org.lappsgrid.eager.askme.solr

import com.sun.xml.internal.org.jvnet.fastinfoset.sax.ExtendedContentHandler
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
        GetSolrDocuments process = new GetSolrDocuments()
        message.body = process.answer(query)



    }
    
    static void main(String[] args) {
        new Main()
    }
}
