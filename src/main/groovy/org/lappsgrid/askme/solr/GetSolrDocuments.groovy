package org.lappsgrid.askme.solr

import groovy.util.logging.Slf4j
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.client.solrj.request.CoreAdminRequest
import org.apache.solr.client.solrj.response.CoreAdminResponse
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.SolrDocumentList
import org.apache.solr.common.params.CoreAdminParams
import org.apache.solr.common.params.MapSolrParams
import org.apache.solr.common.util.NamedList
import org.lappsgrid.askme.core.api.Packet
import org.lappsgrid.askme.core.api.Query
import org.lappsgrid.askme.core.api.Status
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
    static final String fl = 'id,pmid,pmc,doi,year,title,path,url,abstract,body'
    final Stanford nlp
    final SolrClient solr
    String collection = 'cord_askme'

    GetSolrDocuments() {
        nlp = new Stanford()
        logger.info("Creating CloudSolrClient")
//        solr = new CloudSolrClient.Builder([solr_address]).build()
        solr = new HttpSolrClient.Builder(solr_address).build()
    }

    int changeCollection(String name) {
        CoreAdminRequest request = new CoreAdminRequest()
        request.setAction(CoreAdminParams.CoreAdminAction.STATUS)
        CoreAdminResponse response = request.process(solr)
        if (response.status != 0) {
            logger.error("Bad response from solr")
            logger.error(response.toString())
            return -1
        }
        NamedList status = response.getCoreStatus(name)
        if (status == null || status.size() == 0) {
            logger.warn("No core named {}", name)
            return -1
        }
        collection = name
        int nDocs = 0
        try {
            nDocs = status.get("index").get("numDocs")
        }
        catch (Exception e) {
            logger.error("Could not get number of docs from core {}", name, e)
        }
        return nDocs
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
        QueryResponse response = null
        try {
            response = solr.query(collection, queryParams)
        }
        catch (Exception e) {
            packet.status = Status.ERROR
            packet.message = e.message
            packet.documents = []
            return packet
        }
//        solr.close()
        final SolrDocumentList documents = response.getResults()

        logger.info("Received {} documents", documents.size())
        packet.documents = documents.collect{ createDocument(it) }
        return packet
    }

    Document createDocument(SolrDocument solr){
        Document document = new Document()
        ['id', 'pmid', 'pmc', 'doi', 'year', 'url', 'path'].each { field ->
            document.setProperty(field, solr.getFieldValue(field))
        }
        document.title = nlp.process(solr.getFieldValue('title').toString())
//        document.setProperty('title', title)
        document.articleAbstract = nlp.process(solr.getFieldValue('abstract').toString())
//        document.setProperty('articleAbstract', abs)
        return document
    }


}
