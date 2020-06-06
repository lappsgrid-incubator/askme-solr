package org.lappsgrid.askme.solr

import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.client.solrj.request.CoreAdminRequest
import org.apache.solr.client.solrj.response.CoreAdminResponse
import org.apache.solr.common.params.CoreAdminParams
import org.apache.solr.common.util.NamedList
import org.junit.Ignore
import org.junit.Test
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction

/**
 *
 */
@Ignore
class CoreAdmin {

    @Test
    void cores() {
        String name = "cord_2020_05_01"
        SolrClient solr = new HttpSolrClient.Builder("http://129.114.16.34:8983/solr").build()
        CoreAdminRequest request = new CoreAdminRequest()
        request.setAction(CoreAdminParams.CoreAdminAction.STATUS)
        CoreAdminResponse response = request.process(solr)
        if (response.status != 0) {
            println "Bad status: ${response.status}"
            return
        }
        NamedList status = response.getCoreStatus(name)
        if (status == null || status.size() == 0) {
            println "No core named $name"
            return
        }
        println status.get("index").get("numDocs")
    }
}
