package org.lappsgrid.eager.askme.solr

import groovy.util.logging.Slf4j
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.SolrDocumentList
import org.apache.solr.common.params.MapSolrParams


import org.lappsgrid.eager.mining.api.Query

@Slf4j("logger")
class GetSolrDocuments {

    /**
     * rows -----------> default number of solr documents returned
     * solr_address ---> web address to solr database
     * collection -----> the specific database accessed
     * fl -------------> fields saved from each document in the solr query
     */

    Integer rows = 10
    final String solr_address = "http://129.114.16.34:8983/solr"
    final String collection = 'bioqa'
    final String fl = 'id,pmid,pmc,doi,year,title,path,abstract,body'


    Map answer(Query query, String id, int number_of_documents) {

        logger.info("Generating answer for Message {}", id)
        logger.info("Creating CloudSolrClient")
        SolrClient solr = new CloudSolrClient.Builder([solr_address]).build()

        logger.info("Generating solr parameters")
        Map solrParams = [:]
        solrParams.q = query.query
        solrParams.fl = fl
        rows = number_of_documents
        solrParams.rows = rows
        MapSolrParams queryParams = new MapSolrParams(solrParams)

        logger.info("Sending query to Solr")
        final QueryResponse response = solr.query(collection, queryParams)
        final SolrDocumentList documents = response.getResults()
        int n = documents.size()

        logger.info("Received {} documents", n)
        Map result = [:]
        result.query = query
        result.size = n
        result.documents = documents

        return result
    }
}
