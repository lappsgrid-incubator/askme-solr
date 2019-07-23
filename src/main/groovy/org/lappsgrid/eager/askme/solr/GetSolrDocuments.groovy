package org.lappsgrid.eager.askme.solr

import groovy.util.logging.Slf4j
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.SolrRequest
import org.apache.solr.client.solrj.SolrResponse
import org.apache.solr.client.solrj.SolrServerException
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.SolrDocumentList
import org.apache.solr.common.params.MapSolrParams
import org.apache.solr.common.params.SolrParams
import org.apache.solr.common.util.NamedList
import org.lappsgrid.eager.mining.api.Query



@Slf4j("logger")
class GetSolrDocuments {

    //DOES THIS NEED TO BE PRIVATE
    Map answer(Query query) {

        //init()
        logger.info("Generating answer.")

        logger.info("Creating CloudSolrClient")

        SolrClient solr = new CloudSolrClient.Builder(["http://129.114.16.34:8983/solr"]).build()



        logger.trace("Generating query")
        Map solrParams = [:]
        solrParams.q = query.query
        solrParams.fl = 'pmid,pmc,doi,year,title,path,abstract,body'
        solrParams.rows = 1

        MapSolrParams queryParams = new MapSolrParams(solrParams)
        String collection = 'bioqa'

        logger.trace("Sending query to Solr")
        final QueryResponse response = solr.query(collection, queryParams)
        final SolrDocumentList documents = response.getResults()
        int n = documents.size()
        logger.trace("Received {} documents", n)
        Map result = [:]
        result.query = query
        result.size = n
        result.documents = documents
        logger.info('Result: {}', result.documents)

        return result
    }

}
