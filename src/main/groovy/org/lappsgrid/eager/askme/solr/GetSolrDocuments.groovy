package org.lappsgrid.eager.askme.solr

import groovy.util.logging.Slf4j
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.SolrDocumentList
import org.apache.solr.common.params.MapSolrParams
import org.lappsgrid.eager.mining.api.Query
//import org.springframework.core.env.Environment

@Slf4j("logger")
class GetSolrDocuments {

    /**
     //Environment env
     ConfigObject config

     void set(Map map, String key) {
     String value = env.getProperty(key)
     set(map, key, value)
     }

     void set(Map map, String key, String value) {
     set(map, key.tokenize('.'), value)
     }

     void set(Map map, List parts, String value) {
     if (parts.size() == 1) {
     map[parts[0]] = value
     }
     else {
     String key = parts.remove(0)
     Map current = map[key]
     if (current == null) {
     current = [:]
     map[key] = current
     }
     set(current, parts, value)
     }
     }

     private void init() {
     config = new ConfigObject()
     Map m = [:]
     set(m, 'solr.host')
     set(m, 'solr.collection')
     set(m, 'solr.rows')
     set(m, 'galaxy.host')
     set(m, 'galaxy.key', System.getenv('GALAXY_API_KEY'))
     set(m, 'root')
     set(m, 'work.dir')
     set(m, 'question.dir')
     }
     **/
    //DOES THIS NEED TO BE PRIVATE
    public String answer(Query query) {

        //init()
        logger.debug("Generating answer.")

        logger.trace("Creating CloudSolrClient")
        //SolrClient solr = new CloudSolrClient.Builder([config.solr.host]).build()
        //SolrClient solr = new CloudSolrClient.Builder(["http://solr1.lappsgrid.org:8983/solr"]).build()
        SolrClient solr = new CloudSolrClient.Builder(["http://129.114.16.34:8983/solr"]).build()


        logger.trace("Generating query")
        Map solrParams = [:]
        solrParams.q = query.query
        solrParams.fl = 'pmid,pmc,doi,year,title,path,abstract,body'
        //solrParams.rows = config.solr.rows

        MapSolrParams queryParams = new MapSolrParams(solrParams)
        //String collection = config.solr.collection

        logger.trace("Sending query to Solr")
        //final QueryResponse response = solr.query(collection, queryParams)
        final QueryResponse response = solr.query('',queryParams)
        final SolrDocumentList documents = response.getResults()

        int n = documents.size()
        logger.trace("Received {} documents", n)
        Map result = [:]
        result.query = query
        result.size = n

        logger.trace("Received {} documents", n)
        return ""
    }

}
