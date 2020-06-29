package org.lappsgrid.askme.solr

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import org.lappsgrid.askme.core.Configuration
import org.lappsgrid.askme.core.api.AskmeMessage
import org.lappsgrid.askme.core.api.Packet
import org.lappsgrid.askme.core.metrics.Tags
import org.lappsgrid.rabbitmq.Message
import org.lappsgrid.rabbitmq.topic.MailBox
import org.lappsgrid.rabbitmq.topic.PostOffice
import org.lappsgrid.serialization.Serializer

/**
 * TODO:
 * 1) Update imports to phase out eager (waiting on askme-core pom)
 * 2) Add exceptions / case statements to recv method?
 */
@CompileStatic
@Slf4j("logger")
class Main{
    static final Configuration config = new Configuration()

    final PostOffice po
    MailBox box
    Stanford nlp
    GetSolrDocuments process

    final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    Counter documentsFetched
    Counter messagesReceived
    Timer timer

    Main() {
        logger.info("Exchange: {}", config.EXCHANGE)
        logger.info("Host: {}", config.HOST)
        logger.info("Address: {}", config.SOLR_MBOX)
        try {
            po = new PostOffice(config.EXCHANGE, config.HOST)
            nlp = new Stanford()
            process = new GetSolrDocuments()
            init()
        }
        catch (Exception e) {
            logger.error("Unable to construct application.", e)
        }
    }

    void init() {

        new ClassLoaderMetrics().bindTo(registry)
        new JvmMemoryMetrics().bindTo(registry)
        new JvmGcMetrics().bindTo(registry)
        new ProcessorMetrics().bindTo(registry)
        new JvmThreadMetrics().bindTo(registry)
        documentsFetched = registry.counter("documents_fetched", "service", Tags.SOLR)
        messagesReceived = registry.counter("messages", "service", Tags.SOLR)
        timer = registry.timer("solr_query_times")
    }

    void run() {
        Object lock = new Object()
        logger.info("Running.")
        box = new MailBox(config.EXCHANGE, config.SOLR_MBOX, config.HOST) {
            @Override
            void recv(String s) {
                messagesReceived.increment()
                AskmeMessage message = Serializer.parse(s, AskmeMessage)
                String command = message.getCommand()
                String id = message.getId()
                if (command == 'EXIT' || command == 'STOP') {
                    logger.info('Received shutdown message, terminating Solr service')
                    synchronized(lock) { lock.notify() }
                }
                else if(command == 'PING') {
                    logger.info('Received PING message from and sending response back to {}', message.route[0])
                    Message response = new Message()
                    response.id = message.id
                    response.setCommand('PONG')
                    response.setRoute(message.route)
                    logger.info('Response PONG sent to {}', response.route[0])
                    Main.this.po.send(response)
                }
                else if (command == 'CORE') {
                    logger.info("CORE command received")
                    String core = message.body?.message
                    Message response = new Message()
                    response.id = message.id
                    response.setRoute(message.route)
                    if (core != null) {
                        int n = process.changeCollection(core)
                        if (n >= 0) {
                            response.command("ok")
                                .set("numDocs", Integer.toString(n))
                        }
                        else {
                            response.command("error")
                                .set("message", "Unable to switch to core $core.")

                        }
                    }
                    else {
                        message.command('error').set('message', 'No such core.')
                    }
                    Main.this.po.send(response);
                }
                else if (command == 'METRICS') {
                    Message response = new Message()
                    response.id = message.id
                    response.setCommand('ok')
                    response.body(registry.scrape())
                    response.route = message.route
                    logger.trace('Metrics sent to {}', response.route[0])
                    Main.this.po.send(response)
                }
                else {
                    logger.info('Received Message {}', id)
                    String destination = message.route[0] ?: 'the void'
                    Packet packet = (Packet) message.body
                    logger.info("Gathering solr documents for query '{}'", packet.query.query)
                    message.body = timer.recordCallable {
                        return process.answer(packet, id) //, nDocuments)
                    }
                    logger.trace("Processed query from Message {}",id)
                    if (packet.documents && packet.documents.size() > 0) {
                        documentsFetched.increment(packet.documents.size())
                    }
                    Main.this.po.send(message)
                    logger.debug("Message {} with solr documents sent to {}", id, destination)
                }
            }
        }
        synchronized(lock) { lock.wait() }
        po.close()
        box.close()
        logger.info("Solr service terminated")
    }

    static void main(String[] args) {
        logger.info("Starting Solr service")
        Thread.start {
            new Main().run()
        }
    }

    /**
     * CODE IS NOT CURRENTLY USED
     *
    //Checks if:
    // 1) message body is empty
    // 2) command is empty (as of right now, default is 10) THIS IS NOW DONE IN WEB
    // TODO: fix and integrate with new run and recv message
    boolean checkMessage(Message message) {
        if (!(message.getBody())) {
            Map error_check = [:]
            error_check.origin = "Solr"
            error_check.messageId = message.getId()
            if (!message.getBody()) {
                logger.info('ERROR: Message has empty body')
                error_check.body = 'MISSING'
            }
            logger.info('Notifying Web service of error')
            Message error_message = new Message()
            error_message.setCommand('ERROR')
            error_message.setBody(error_check)
            error_message.route('web.mailbox')
            po.send(error_message)
            return false
        }
        return true
    }


 **/
}
