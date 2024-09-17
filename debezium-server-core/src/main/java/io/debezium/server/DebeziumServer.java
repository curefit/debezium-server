/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.health.Liveness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.arrakis.commons.dto.PipelineRunContext;
import io.arrakis.commons.enums.PipelineStep;
import io.arrakis.commons.requests.CancelPipelineStepRequest;
import io.arrakis.commons.utils.ArrakisHttpUtils;
import io.arrakis.commons.utils.ArrakisUtils;
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.format.Avro;
import io.debezium.engine.format.CloudEvents;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.JsonByteArray;
import io.debezium.engine.format.Protobuf;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.NoRecordsReadEvent;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;

/**
 * <p>The entry point of the Quarkus-based standalone server. The server is configured via Quarkus/Microprofile Configuration sources
 * and provides few out-of-the-box target implementations.</p>
 * <p>The implementation uses CDI to find all classes that implements {@link DebeziumEngine.ChangeConsumer} interface.
 * The candidate classes should be annotated with {@code @Named} annotation and should be {@code Dependent}.</p>
 * <p>The configuration option {@code debezium.consumer} provides a name of the consumer that should be used and the value
 * must match to exactly one of the implementation classes.</p>
 *
 * @author Jiri Pechanec
 *
 */
@ApplicationScoped
@Startup
public class DebeziumServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumServer.class);

    private static final String PROP_PREFIX = "debezium.";
    private static final String PROP_SOURCE_PREFIX = PROP_PREFIX + "source.";
    private static final String PROP_SINK_PREFIX = PROP_PREFIX + "sink.";
    private static final String PROP_FORMAT_PREFIX = PROP_PREFIX + "format.";
    private static final String PROP_PREDICATES_PREFIX = PROP_PREFIX + "predicates.";
    private static final String PROP_TRANSFORMS_PREFIX = PROP_PREFIX + "transforms.";
    private static final String PROP_HEADER_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "header.";
    private static final String PROP_KEY_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "key.";
    private static final String PROP_VALUE_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "value.";
    private static final String PROP_OFFSET_STORAGE_PREFIX = "offset.storage.";

    private static final String PROP_PREDICATES = PROP_PREFIX + "predicates";
    private static final String PROP_TRANSFORMS = PROP_PREFIX + "transforms";
    private static final String PROP_SINK_TYPE = PROP_SINK_PREFIX + "type";

    private static final String PROP_HEADER_FORMAT = PROP_FORMAT_PREFIX + "header";
    private static final String PROP_KEY_FORMAT = PROP_FORMAT_PREFIX + "key";
    private static final String PROP_VALUE_FORMAT = PROP_FORMAT_PREFIX + "value";
    private static final String PROP_TERMINATION_WAIT = PROP_PREFIX + "termination.wait";

    private static final String FORMAT_JSON = Json.class.getSimpleName().toLowerCase();
    private static final String FORMAT_JSON_BYTE_ARRAY = JsonByteArray.class.getSimpleName().toLowerCase();
    private static final String FORMAT_CLOUDEVENT = CloudEvents.class.getSimpleName().toLowerCase();
    private static final String FORMAT_AVRO = Avro.class.getSimpleName().toLowerCase();
    private static final String FORMAT_PROTOBUF = Protobuf.class.getSimpleName().toLowerCase();

    private static final Pattern SHELL_PROPERTY_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+_+[a-zA-Z0-9_]+$");

    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private int returnCode = 0;

    @Inject
    BeanManager beanManager;

    @Inject
    @Liveness
    ConnectorLifecycle health;

    private Bean<DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>> consumerBean;
    private CreationalContext<ChangeConsumer<ChangeEvent<Object, Object>>> consumerBeanCreationalContext;
    private DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> consumer;
    DebeziumEngine<?> engine;
    private final Properties props = new Properties();

    public boolean recordsReceivedflag = false;
    // public Instant serverStartTime;
    public Instant lastTimeRecordsReceived = null;

    @SuppressWarnings("unchecked")
    @PostConstruct
    public void start() {

        LOGGER.info("Starting SchemaRecovery");
        try {
            SchemaRecovery schemaRecovery = new SchemaRecovery();
            ConfigHolder configHolder = schemaRecovery.recoverSchema();
            LOGGER.info("Recovered config : {}", configHolder);
            // Set the ConfigHolder in ConfigHolderBean
            ConfigHolderBean.configHolder = configHolder;
        }
        catch (Exception e) {
            LOGGER.error("Error while recovering schema", e);
            Quarkus.asyncExit(1);
        }

        final Config config = loadConfigOrDie();
        final String name = config.getValue(PROP_SINK_TYPE, String.class);
        // final CommonUtils commonUtils = new CommonUtils();

        final Set<Bean<?>> beans = beanManager.getBeans(name).stream()
                .filter(x -> DebeziumEngine.ChangeConsumer.class.isAssignableFrom(x.getBeanClass()))
                .collect(Collectors.toSet());
        LOGGER.debug("Found {} candidate consumer(s)", beans.size());

        if (beans.size() == 0) {
            throw new DebeziumException("No Debezium consumer named '" + name + "' is available");
        }
        else if (beans.size() > 1) {
            throw new DebeziumException("Multiple Debezium consumers named '" + name + "' were found");
        }

        consumerBean = (Bean<DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>>) beans.iterator().next();
        consumerBeanCreationalContext = beanManager.createCreationalContext(consumerBean);
        consumer = consumerBean.create(consumerBeanCreationalContext);
        LOGGER.info("Consumer '{}' instantiated", consumer.getClass().getName());

        final Class<Any> keyFormat = (Class<Any>) getFormat(config, PROP_KEY_FORMAT);
        final Class<Any> valueFormat = (Class<Any>) getFormat(config, PROP_VALUE_FORMAT);
        final Class<Any> headerFormat = (Class<Any>) getHeaderFormat(config);

        configToProperties(config, props, PROP_SOURCE_PREFIX, "", true);
        configToProperties(config, props, PROP_FORMAT_PREFIX, "key.converter.", true);
        configToProperties(config, props, PROP_FORMAT_PREFIX, "value.converter.", true);
        configToProperties(config, props, PROP_FORMAT_PREFIX, "header.converter.", true);
        configToProperties(config, props, PROP_KEY_FORMAT_PREFIX, "key.converter.", true);
        configToProperties(config, props, PROP_VALUE_FORMAT_PREFIX, "value.converter.", true);
        configToProperties(config, props, PROP_HEADER_FORMAT_PREFIX, "header.converter.", true);
        configToProperties(config, props, PROP_SINK_PREFIX + name + ".", SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + name + ".", false);
        configToProperties(config, props, PROP_SINK_PREFIX + name + ".", PROP_OFFSET_STORAGE_PREFIX + name + ".", false);

        final Optional<String> transforms = config.getOptionalValue(PROP_TRANSFORMS, String.class);
        if (transforms.isPresent()) {
            props.setProperty("transforms", transforms.get());
            configToProperties(config, props, PROP_TRANSFORMS_PREFIX, "transforms.", true);
        }

        final Optional<String> predicates = config.getOptionalValue(PROP_PREDICATES, String.class);
        if (predicates.isPresent()) {
            props.setProperty("predicates", predicates.get());
            configToProperties(config, props, PROP_PREDICATES_PREFIX, "predicates.", true);
        }

        props.setProperty("name", config.getValue("debezium.source.database.dbname", String.class));
        LOGGER.debug("Configuration for DebeziumEngine: {}", props);

        ConfigHolder configHolder = ConfigHolderBean.configHolder;
        LOGGER.info("ConfigHolder in DebeziumServer: {}", configHolder);

        props.setProperty("schema.history.internal.file.filename", configHolder.getSchemHistoryFileName());
        props.setProperty("database.server.id", String.valueOf(ArrakisUtils.generateServerID()));
        props.setProperty("database.server.name", config.getValue("debezium.source.database.dbname", String.class)
                + "-" + configHolder.getShortPipeId());
        props.setProperty("topic.prefix", configHolder.getShortPipeId());
        props.setProperty("table.include.list", configHolder.getTableList());
        props.setProperty("offset.storage.file.filename", configHolder.getOffsetFileName());

        engine = DebeziumEngine.create(keyFormat, valueFormat, headerFormat)
                .using(props)
                .using((DebeziumEngine.ConnectorCallback) health)
                .using((DebeziumEngine.CompletionCallback) health)
                .notifying(consumer)
                .build();

        executor.execute(() -> {
            try {
                engine.run();
            }
            finally {
                Quarkus.asyncExit(returnCode);
            }
        });
        LOGGER.info("Engine executor started");

    }

    private boolean stopConnectorThroughAPI() {
        String ARRAKIS_URL = "arrakis.backend.url";
        String PIPE_INFO_API = "/api/v1/pipes/id/";
        String ARRAKIS_PIPE_ID = "arrakis.pipeline.id";
        try {
            final Config config = ConfigProvider.getConfig();
            PipelineRunContext pipe = ArrakisHttpUtils.getPipeInfoApi(config.getValue(ARRAKIS_URL, String.class)
                    + PIPE_INFO_API
                    + config.getValue(ARRAKIS_PIPE_ID, String.class));
            String stopConnectorUrl = "/api/v1/pipes/" + pipe.getPipelineName() + "/steps/stop";
            CancelPipelineStepRequest cancelPipelineStepRequest = new CancelPipelineStepRequest();
            cancelPipelineStepRequest.setPipelineStep(PipelineStep.SOURCE_TO_STAGING);
            return ArrakisHttpUtils.stopPipelineStep(config.getValue(ARRAKIS_URL, String.class)
                    + stopConnectorUrl, cancelPipelineStepRequest);
        }
        catch (Exception e) {
            LOGGER.error("Error while stopping connector through API", e);
            return false;
        }
    }

    private void configToProperties(Config config, Properties props, String oldPrefix, String newPrefix, boolean overwrite) {
        for (String name : config.getPropertyNames()) {
            String updatedPropertyName = null;
            if (SHELL_PROPERTY_NAME_PATTERN.matcher(name).matches()) {
                updatedPropertyName = name.replace("_", ".").toLowerCase();
            }
            if (updatedPropertyName != null && updatedPropertyName.startsWith(oldPrefix)) {
                String finalPropertyName = newPrefix + updatedPropertyName.substring(oldPrefix.length());
                if (overwrite || !props.containsKey(finalPropertyName)) {
                    props.setProperty(finalPropertyName, config.getValue(name, String.class));
                }
            }
            else if (name.startsWith(oldPrefix)) {
                String finalPropertyName = newPrefix + name.substring(oldPrefix.length());
                if (overwrite || !props.containsKey(finalPropertyName)) {
                    props.setProperty(finalPropertyName, config.getConfigValue(name).getValue());
                }
            }
        }
    }

    private Class<?> getFormat(Config config, String property) {
        final String formatName = config.getOptionalValue(property, String.class).orElse(FORMAT_JSON);
        if (FORMAT_JSON.equals(formatName)) {
            return Json.class;
        }
        if (FORMAT_JSON_BYTE_ARRAY.equals(formatName)) {
            return JsonByteArray.class;
        }
        else if (FORMAT_CLOUDEVENT.equals(formatName)) {
            return CloudEvents.class;
        }
        else if (FORMAT_AVRO.equals(formatName)) {
            return Avro.class;
        }
        else if (FORMAT_PROTOBUF.equals(formatName)) {
            return Protobuf.class;
        }
        throw new DebeziumException("Unknown format '" + formatName + "' for option " + "'" + property + "'");
    }

    private Class<?> getHeaderFormat(Config config) {
        final String formatName = config.getOptionalValue(PROP_HEADER_FORMAT, String.class).orElse(FORMAT_JSON);
        if (FORMAT_JSON.equals(formatName)) {
            return Json.class;
        }
        else if (FORMAT_JSON_BYTE_ARRAY.equals(formatName)) {
            return JsonByteArray.class;
        }
        throw new DebeziumException("Unknown format '" + formatName + "' for option " + "'" + PROP_HEADER_FORMAT + "'");
    }

    public void stop(@Observes ShutdownEvent event) {
        try {
            LOGGER.info("Received request to stop the engine");
            final Config config = ConfigProvider.getConfig();
            engine.close();
            executor.shutdown();
            executor.awaitTermination(config.getOptionalValue(PROP_TERMINATION_WAIT, Integer.class).orElse(10), TimeUnit.SECONDS);
        }
        catch (Exception e) {
            LOGGER.error("Exception while shutting down Debezium", e);
        }
        consumerBean.destroy(consumer, consumerBeanCreationalContext);
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) {
        if (!event.isSuccess()) {
            returnCode = 1;
        }
        try {
            final Config config = ConfigProvider.getConfig();
            engine.close();
            executor.shutdownNow();
            executor.awaitTermination(config.getOptionalValue(PROP_TERMINATION_WAIT, Integer.class).orElse(10), TimeUnit.SECONDS);
            stopConnectorThroughAPI();
        }
        catch (Exception e) {
            LOGGER.error("Exception while shutting down Debezium", e);
        }
    }

    void noRecordsRead(@Observes NoRecordsReadEvent event) {
        LOGGER.info("Observed NoRecordsReadEvent: {}", event.getMessage());
        try {
            LOGGER.info("Received request to stop the engine");
            engine.close();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
            stopConnectorThroughAPI();
        }
        catch (Exception e) {
            LOGGER.error("Exception while shutting down Debezium", e);
        }
    }

    private Config loadConfigOrDie() {
        final Config config = ConfigProvider.getConfig();
        // Check config and exit if we cannot load mandatory option.
        try {
            config.getValue(PROP_SINK_TYPE, String.class);
        }
        catch (NoSuchElementException e) {
            final String configFile = Paths.get(System.getProperty("user.dir"), "conf", "application.properties").toString();
            // CHECKSTYLE IGNORE check FOR NEXT 2 LINES
            System.err.println(String.format("Failed to load mandatory config value '%s'. Please check you have a correct Debezium server config in %s or required "
                    + "properties are defined via system or environment variables.", PROP_SINK_TYPE, configFile));
            Quarkus.asyncExit();
        }
        return config;
    }

    /**
     * For test purposes only
     */
    DebeziumEngine.ChangeConsumer<?> getConsumer() {
        return consumer;
    }

    public Properties getProps() {
        return props;
    }
}
