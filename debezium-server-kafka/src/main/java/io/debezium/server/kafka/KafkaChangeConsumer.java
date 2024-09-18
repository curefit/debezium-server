/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import io.arrakis.commons.utils.ArrakisHttpUtils;
import io.arrakis.connectors.mysql.state.MySQLOffset;
import io.arrakis.connectors.mysql.utils.MySQLBatchExitLogic;
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.Header;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.ConfigHolderBean;
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.server.DebeziumServer;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStoppedEvent;

/**
 * An implementation of the {@link DebeziumEngine.ChangeConsumer} interface that publishes change event messages to Kafka.
 */
@Named("kafka")
@Dependent
public class KafkaChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.kafka.";
    private static final String PROP_PREFIX_PRODUCER = PROP_PREFIX + "producer.";
    private KafkaProducer<Object, Object> producer;

    public boolean hasTargetReached = false;

    private final Config config = ConfigProvider.getConfig();

    @Inject
    @CustomConsumerBuilder
    Instance<KafkaProducer<Object, Object>> customKafkaProducer;

    @Inject
    Event<ConnectorCompletedEvent> connectorCompletedEventEvent;

    @Inject
    Event<ConnectorStoppedEvent> connectorStoppedEvent;

    // @Inject
    // ConfigHolderBean configHolderBean;
    //
    @Inject
    DebeziumServer debeziumServer;

    @PostConstruct
    void start() {
        if (customKafkaProducer.isResolvable()) {
            producer = customKafkaProducer.get();
            LOGGER.info("Obtained custom configured KafkaProducer '{}'", producer);
            return;
        }

        final Config config = ConfigProvider.getConfig();
        producer = new KafkaProducer<>(getConfigSubset(config, PROP_PREFIX_PRODUCER));
        LOGGER.info("consumer started...");
    }

    @PreDestroy
    void stop() {
        LOGGER.info("consumer destroyed...");
        if (producer != null) {
            try {
                producer.close(Duration.ofSeconds(5));
                LOGGER.info("Closed producer");
                if (!hasTargetReached) {
                    LOGGER.info("Always store the state when signalled for close!");
                    storeState(config);
                    connectorStoppedEvent.fire(new ConnectorStoppedEvent());
                }
            }
            catch (Throwable t) {
                LOGGER.warn("Could not close producer", t);
            }
        }
    }

    public void storeState(Config config) {
        LOGGER.info("Storing state");

        String stateUrl = config.getOptionalValue("arrakis.backend.url", String.class).orElse("http://localhost:9094")
                + "/api/v1/pipes/" + config.getValue("arrakis.pipeline.id", String.class) + "/state";

        MySQLOffset mySQLOffset = new MySQLOffset(Path.of(ConfigHolderBean.configHolder.getOffsetFileName()),
                config.getOptionalValue("debezium.source.database.dbname", String.class));
        Map<String, String> stateMap = mySQLOffset.read();
        JsonNode state = mySQLOffset.serialize(stateMap);
        try {
            System.out.println("Saving state: " + state);
            ArrakisHttpUtils.updateState(stateUrl, state);
        }
        catch (Exception e) {
            LOGGER.error("Failed to save state: " + e.getMessage(), e);
        }
    }

    @Override
    public void handleBatch(final List<ChangeEvent<Object, Object>> records,
                            final RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        debeziumServer.recordsReceivedflag = true;
        debeziumServer.lastTimeRecordsReceived = Instant.now();
        LOGGER.info("Received records for processing {} at {}", true, Instant.now());

        final CountDownLatch latch = new CountDownLatch(records.size());

        MySQLBatchExitLogic mySqlBatchExitLogic = new MySQLBatchExitLogic();

        for (ChangeEvent<Object, Object> record : records) {
            try {

                Headers headers = convertKafkaHeaders(record);

                producer.send(new ProducerRecord<>(record.destination(), null, null, record.key(), record.value(), headers), (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Failed to send record to {}:", record.destination(), exception);
                        throw new DebeziumException(exception);
                    }
                    else {
                        LOGGER.trace("Sent message with offset: {}", metadata.offset());
                        latch.countDown();
                    }
                });
                committer.markProcessed(record);

                if (ConfigHolderBean.configHolder.pipelineType.equals("BATCH")) {
                    hasTargetReached = mySqlBatchExitLogic.reachedTarget(record.toString(),
                            ConfigHolderBean.configHolder.getTargetFileName(),
                            ConfigHolderBean.configHolder.getTargetPosition());
                }

            }
            catch (Exception e) {
                throw new DebeziumException(e);
            }
        }

        latch.await();
        committer.markBatchFinished();

        if (hasTargetReached) {
            LOGGER.info("Pipeline is in BATCH mode. Signalling engine shutdown as the connector has reached target position.");
            storeState(config);
            connectorCompletedEventEvent.fire(new ConnectorCompletedEvent(true, "Connector has reached target position", null));
        }
        // debeziumServer.recordsReceivedflag = false;
    }

    private Headers convertKafkaHeaders(ChangeEvent<Object, Object> record) {
        List<Header<Object>> headers = record.headers();
        Headers kafkaHeaders = new RecordHeaders();
        for (Header<Object> header : headers) {
            kafkaHeaders.add(header.getKey(), getBytes(header.getValue()));
        }
        return kafkaHeaders;
    }
}
