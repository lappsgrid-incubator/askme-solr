package org.lappsgrid.askme.solr

import groovy.util.logging.Slf4j
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.SolrDocumentList
import org.apache.solr.common.params.MapSolrParams
import org.lappsgrid.askme.core.api.Packet
import org.lappsgrid.askme.core.api.Query
import org.lappsgrid.askme.core.model.Document
import org.lappsgrid.askme.core.model.Section

@Slf4j("logger")
class GetSolrDocuments {

    /**
     * rows -----------> default number of solr documents returned
     * solr_address ---> web address to solr database
     * collection -----> the specific database accessed
     * fl -------------> fields saved from each document in the solr query
     */

    int rows = 10
    static final String solr_address = "http://129.114.16.34:8983/solr"
    static final String collection = 'bioqa'
    static final String fl = 'id,pmid,pmc,doi,year,title,path,abstract,body'
    final Stanford nlp
    final SolrClient solr

    GetSolrDocuments() {
        nlp = new Stanford()
        logger.info("Creating CloudSolrClient")
        solr = new CloudSolrClient.Builder([solr_address]).build()
    }

    Packet answer(Packet packet, String id) { //}, int number_of_documents) {

        logger.info("Generating answer for Message {}", id)

        Query query = packet.query

        logger.info("Generating solr parameters")
        Map solrParams = [:]
        solrParams.q = query.query
        solrParams.fl = fl
//        if(number_of_documents){
//            rows = number_of_documents
//        }
        solrParams.rows = query.count ?: 100
        MapSolrParams queryParams = new MapSolrParams(solrParams)

        logger.info("Sending query to Solr: {}", query.query)
        final QueryResponse response = solr.query(collection, queryParams)
//        solr.close()
        final SolrDocumentList documents = response.getResults()

        logger.info("Received {} documents", documents.size())
        packet.documents = documents.collect{ createDocument(it) }
        return packet
    }

    Document createDocument(SolrDocument solr){
        Document document = new Document()
        ['id', 'pmid', 'pmc', 'doi', 'year', 'path'].each { field ->
            document.setProperty(field, solr.getFieldValue(field))
        }
        document.title = nlp.process(solr.getFieldValue('title').toString())
//        document.setProperty('title', title)
        document.articleAbstract = nlp.process(solr.getFieldValue('abstract').toString())
//        document.setProperty('articleAbstract', abs)
        return document
    }


}
