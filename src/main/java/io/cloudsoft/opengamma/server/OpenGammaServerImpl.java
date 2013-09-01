package io.cloudsoft.opengamma.server;

import static java.lang.String.format;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import brooklyn.event.feed.jmx.JmxAttributePollConfig;
import brooklyn.event.feed.jmx.JmxFeed;
import com.google.common.base.Predicates;
import org.jclouds.compute.domain.OsFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.enricher.RollingTimeWindowMeanEnricher;
import brooklyn.enricher.TimeWeightedDeltaEnricher;
import brooklyn.entity.basic.SoftwareProcessImpl;
import brooklyn.entity.database.postgresql.PostgreSqlNode;
import brooklyn.entity.java.JavaAppUtils;
import brooklyn.entity.java.UsesJmx;
import brooklyn.entity.messaging.activemq.ActiveMQBroker;
import brooklyn.entity.webapp.WebAppServiceConstants;
import brooklyn.entity.webapp.WebAppServiceMethods;
import brooklyn.event.feed.http.HttpFeed;
import brooklyn.event.feed.http.HttpPollConfig;
import brooklyn.event.feed.http.HttpValueFunctions;
import brooklyn.event.feed.jmx.JmxAttributePollConfig;
import brooklyn.event.feed.jmx.JmxFeed;
import brooklyn.event.feed.jmx.JmxHelper;
import brooklyn.location.MachineProvisioningLocation;
import brooklyn.location.access.BrooklynAccessUtils;
import brooklyn.location.jclouds.templates.PortableTemplateBuilder;
import brooklyn.util.time.Duration;

import com.google.common.base.Functions;
import com.google.common.net.HostAndPort;

@SuppressWarnings("serial")
public class OpenGammaServerImpl extends SoftwareProcessImpl implements OpenGammaServer, UsesJmx {

    private static final Logger log = LoggerFactory.getLogger(OpenGammaServerImpl.class);
    
    private HttpFeed httpFeed;
    private volatile JmxFeed jmxFeed;
   //private JmxObjectNameAdapter jettyStatsHandler;
    private ActiveMQBroker broker;
    private PostgreSqlNode database;

    @Override
    public void init() {
        broker = getConfig(BROKER);
        database = getConfig(DATABASE);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class getDriverInterface() {
        return OpenGammaServerDriver.class;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Map<String,Object> obtainProvisioningFlags(MachineProvisioningLocation location) {
        Map flags = super.obtainProvisioningFlags(location);
        flags.put("templateBuilder", new PortableTemplateBuilder()
            // need a beefy machine
            .os64Bit(true)
            .minRam(4096)
            // use CENTOS for now, just because UBUNTU images in AWS are dodgy (poor java support)
            .osFamily(OsFamily.UBUNTU).osVersionMatches("12.04")
//          .osFamily(OsFamily.CENTOS)
            );
        return flags;
    }

    @Override
    protected void connectSensors() {
        super.connectSensors();

        HostAndPort hp = BrooklynAccessUtils.getBrooklynAccessibleAddress(this, getAttribute(HTTP_PORT));
        String rootUrl = "http://"+hp.getHostText()+":"+hp.getPort()+"/";
        setAttribute(ROOT_URL, rootUrl);
        
        httpFeed = HttpFeed.builder()
                .entity(this)
                .period(1000)
                .baseUri(rootUrl)
                .poll(new HttpPollConfig<Boolean>(SERVICE_UP)
                        .onSuccess(HttpValueFunctions.responseCodeEquals(200))
                        .onFailureOrException(Functions.constant(false)))
                .build();

        String jettyHttpConnector = "com.opengamma.jetty:service=HttpConnector";
        String opengammaViewHandler = "com.opengamma:type=ViewProcessor,name=ViewProcessor main";
        String opengammaCalcHandler = "com.opengamma:type=CalculationNodes,name=local";
        String connectorMbeanName = format("Catalina:type=Connector,port=%s", getAttribute(HTTP_PORT));

        jmxFeed = JmxFeed.builder()
                .entity(this)
                .period(5000, TimeUnit.MILLISECONDS)
                .pollAttribute(new JmxAttributePollConfig<Boolean>(SERVICE_UP)
                        .objectName(jettyHttpConnector)
                        .attributeName("Running")
                        .onSuccess(Functions.forPredicate(Predicates.<Object>equalTo("STARTED")))
                        .setOnFailureOrException(false))
                .pollAttribute(new JmxAttributePollConfig<Integer>(REQUEST_COUNT)
                        .objectName(jettyHttpConnector)
                        .attributeName("Requests"))
                .pollAttribute(new JmxAttributePollConfig<Integer>(TOTAL_PROCESSING_TIME)
                        .objectName(jettyHttpConnector)
                        .attributeName("ConnectionsDurationTotal"))
                .pollAttribute(new JmxAttributePollConfig<Integer>(MAX_PROCESSING_TIME)
                        .objectName(connectorMbeanName)
                        .attributeName("ConnectionsDurationMax"))
                .pollAttribute(new JmxAttributePollConfig<Integer>(ERROR_COUNT)
                        .objectName(jettyHttpConnector)
                        .attributeName("errorCount"))
                .pollAttribute(new JmxAttributePollConfig<Integer>(VIEW_PROCESSES_COUNT)
                        .objectName(opengammaViewHandler)
                        .attributeName("NumberOfViewProcesses"))
                .pollAttribute(new JmxAttributePollConfig<Integer>(CALC_JOB_COUNT)
                        .objectName(opengammaCalcHandler)
                        .attributeName("TotalJobCount"))
                .pollAttribute(new JmxAttributePollConfig<Integer>(CALC_NODE_COUNT)
                        .objectName(opengammaCalcHandler)
                        .attributeName("TotalNodeCount"))
                .build();

        JavaAppUtils.connectMXBeanSensors(this);
    }

   @Override
    protected void postStart() {
        super.postStart();
        
        // do all this after service up, to prevent warnings
        // TODO migrate JMX routines to brooklyn 6 syntax
             /*

        Map flags = MutableMap.of("period", 5000);
        JmxSensorAdapter jmx = sensorRegistry.register(new JmxSensorAdapter(flags));

        JavaAppUtils.connectMXBeanSensors(this, jmx);
        
        jettyStatsHandler = jmx.objectName("com.opengamma.jetty:service=HttpConnector");
        // have to explicitly turn on, see in postStart
        jettyStatsHandler.attribute("Running").subscribe(SERVICE_UP);
        jettyStatsHandler.attribute("Requests").subscribe(REQUEST_COUNT);
        // these two from jetty not available from opengamma bean:
//        jettyStatsHandler.attribute("requestTimeTotal").subscribe(TOTAL_PROCESSING_TIME);
//        jettyStatsHandler.attribute("responsesBytesTotal").subscribe(BYTES_SENT);
        // these two might be custom OpenGamma
        jettyStatsHandler.attribute("ConnectionsDurationTotal").subscribe(TOTAL_PROCESSING_TIME);
        jettyStatsHandler.attribute("ConnectionsDurationMax").subscribe(MAX_PROCESSING_TIME);
                        */
        WebAppServiceMethods.connectWebAppServerPolicies(this);
        JavaAppUtils.connectJavaAppServerPolicies(this);
        WebAppServiceMethods.connectWebAppServerPolicies(this);

        addEnricher(new TimeWeightedDeltaEnricher<Integer>(this,
                WebAppServiceConstants.TOTAL_PROCESSING_TIME, PROCESSING_TIME_PER_SECOND_LAST, 1));
        addEnricher(new RollingTimeWindowMeanEnricher<Double>(this,
                PROCESSING_TIME_PER_SECOND_LAST, PROCESSING_TIME_PER_SECOND_IN_WINDOW,
                WebAppServiceMethods.DEFAULT_WINDOW_DURATION));

      /*
        JmxObjectNameAdapter opengammaViewHandler = jmx.objectName("com.opengamma:type=ViewProcessor,name=ViewProcessor main");
        opengammaViewHandler.attribute("NumberOfViewProcesses").subscribe(VIEW_PROCESSES_COUNT);
        
        JmxObjectNameAdapter opengammaCalcHandler = jmx.objectName("com.opengamma:type=CalculationNodes,name=local");
        opengammaCalcHandler.attribute("TotalJobCount").subscribe(CALC_JOB_COUNT);
        opengammaCalcHandler.attribute("TotalNodeCount").subscribe(CALC_NODE_COUNT);
               */
        // job count is some temporal measure already
//        addEnricher(new RollingTimeWindowMeanEnricher<Double>(this,
//                CALC_JOB_COUNT, CALC_JOB_RATE,
//                60*1000));
        
        // If MBean is unreachable, then mark as service-down
        // TODO migrate to new syntax
        /*
        opengammaViewHandler.reachable().poll(new Function<Boolean,Void>() {
                @Override public Void apply(Boolean input) {
                    if (input != null && Boolean.FALSE.equals(input)) {
                        Boolean prev = setAttribute(SERVICE_UP, false);
                        if (Boolean.TRUE.equals(prev)) {
                            LOG.warn("Could not reach {} over JMX, marking service-down", OpenGammaServerImpl.this);
                        } else {
                            if (LOG.isDebugEnabled()) LOG.debug("Could not reach {} over JMX, service-up was previously {}", OpenGammaServerImpl.this, prev);
                        }
                    }
                    return null;
                }});
        */
        Object jettyStatsOnResult = new JmxHelper(this).operation(JmxHelper.createObjectName("com.opengamma.jetty:service=HttpConnector"),
                "setStatsOn", true);
        log.debug("result of setStatsOn for "+this+": "+jettyStatsOnResult);
    }
    
    @Override
    protected void disconnectSensors() {
        super.disconnectSensors();
        if (httpFeed != null) httpFeed.stop();
        if (jmxFeed != null) jmxFeed.stop();
    }

    /** HTTP port number for Jetty web service. */
    public Integer getHttpPort() { return getAttribute(HTTP_PORT); }

    /** HTTPS port number for Jetty web service. */
    public Integer getHttpsPort() { return getAttribute(HTTPS_PORT); }

    /** {@inheritDoc} */
    @Override
    public ActiveMQBroker getBroker() { return broker; }

    /** {@inheritDoc} */
    @Override
    public PostgreSqlNode getDatabase() { return database; }
}
