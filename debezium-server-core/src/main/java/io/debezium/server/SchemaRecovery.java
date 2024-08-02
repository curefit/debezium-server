package io.debezium.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.arrakis.commons.dto.PipelineRunContext;
import io.arrakis.commons.exceptions.ArrakisException;
import io.arrakis.commons.utils.ArrakisHttpUtils;
import io.arrakis.commons.utils.ArrakisUtils;
import io.arrakis.connectors.mysql.MySQLConfig;
import io.arrakis.connectors.mysql.MySQLDebeziumProperties;
import io.arrakis.connectors.mysql.schemahistory.MySQLSchemaHistory;
import io.arrakis.connectors.mysql.schemahistory.MySQLSchemaHistoryStorage;
import io.arrakis.connectors.mysql.state.MySQLOffset;
import io.arrakis.connectors.mysql.state.MySQLOffsetUtils;
import io.arrakis.connectors.mysql.state.MySQLStateAttributes;
import io.arrakis.connectors.mysql.utils.MySQLUtils;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.quarkus.runtime.Quarkus;

public class SchemaRecovery {
    private final Logger LOGGER = LoggerFactory.getLogger(SchemaRecovery.class);

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private DebeziumEngine<ChangeEvent<String, String>> engine = null;

    private final BlockingQueue<ChangeEvent<String, String>> queue = new LinkedBlockingQueue<>(10);

    private static final ConfigHolder configHolder = new ConfigHolder();

    private ExecutorService runDebeziumEngine(Path schemaHistoryPath,
                                              Path offsetFilePath,
                                              PipelineRunContext pipelineRunContext)
            throws ArrakisException {

        Properties props = MySQLDebeziumProperties.getMySQLDebeziumPropertiesForSchemaRecovery(pipelineRunContext,
                schemaHistoryPath.toString(), offsetFilePath.toString());

        LOGGER.info("Starting Debezium engine in schema recovery mode to capture the latest schema changes");

        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .using(new OffsetCommitPolicy.AlwaysCommitOffsetPolicy())
                .notifying(e -> {
                    if (e.value() != null) {
                        try {
                            queue.put(e);
                        }
                        catch (final InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(ex);
                        }
                    }
                })
                .using((success, message, error) -> {
                    LOGGER.info("Debezium engine shutdown. Engine terminated successfully : {}", success);
                    LOGGER.info(message);
                    // If debezium has not shutdown correctly, it can indicate an error with the connector configuration
                    if (!success) {
                        LOGGER.error("Debezium engine shutdown. Engine terminated successfully : {}", success);
                        LOGGER.error(message);
                        if (error != null) {
                            LOGGER.error("Error during debezium engine shutdown", error);
                        }
                        LOGGER.error("Schema recovery failed! Shutting down the application.");
                        Quarkus.asyncExit();
                    }
                })
                .build();

        executor.execute(engine);

        return executor;

    }

    private boolean closeOnSuccessOrFailure(Instant engineStartTime) throws IOException, InterruptedException {

        boolean hasClosed = false;
        Duration initialWaitingDuration = Duration.ofMinutes(1L);

        while (!hasClosed) {
            final ChangeEvent<String, String> event = queue.poll(10, TimeUnit.SECONDS);

            if (event == null) {
                LOGGER.info("No record is returned, waiting for 1 minute before closing the engine");

                if (Duration.between(engineStartTime, Instant.now()).compareTo(initialWaitingDuration) > 0) {
                    LOGGER.error("No record is returned even after {} minutes of waiting, closing the engine", initialWaitingDuration.toMinutes());
                    if (engine != null) {
                        engine.close();
                    }
                    executor.shutdown();
                    hasClosed = true;
                    break;
                }
                continue;
            }

            LOGGER.info("A record is returned, closing the engine since the state is constructed");
            if (engine != null) {
                engine.close();
            }
            executor.shutdown();
            hasClosed = true;
            break;
        }

        return hasClosed;
    }

    public ConfigHolder recoverSchema() throws IOException, InterruptedException, SQLException, ArrakisException {

        final Config config = ConfigProvider.getConfig();
        final ObjectMapper objectMapper = new ObjectMapper();

        String ARRAKIS_URL = "arrakis.backend.url";
        String ARRAKIS_PIPE_ID = "arrakis.pipeline.id";
        String PIPE_INFO_API = "/api/v1/pipes/id/";

        PipelineRunContext pipe = ArrakisHttpUtils.getPipeInfoApi(config.getValue(ARRAKIS_URL, String.class)
                + PIPE_INFO_API
                + config.getValue(ARRAKIS_PIPE_ID, String.class));

        LOGGER.info("Pipe info: " + pipe.toString());

        String shotPipeId = ArrakisUtils.uuidToShort(UUID.fromString(config.getValue(ARRAKIS_PIPE_ID, String.class)));

        configHolder.setPipelineType(pipe.pipelineType);
        configHolder.setShortPipeId(shotPipeId);

        MySQLConfig mysqlConfig = objectMapper.readValue(pipe.sourceConfig.toString(),
                MySQLConfig.class);

        String tableIncludeList = MySQLUtils.getTableIncludeList(pipe.getMappedEvents(),
                mysqlConfig.getDatabaseName());

        configHolder.setTableList(tableIncludeList);

        MySQLOffsetUtils mySqlOffsetUtils = new MySQLOffsetUtils(mysqlConfig);

        LOGGER.info("Extracting latest Offset from MySQL for target position and filename: ");
        List<MySQLStateAttributes> targetMySqlOffsetState = mySqlOffsetUtils.getStateAttributesFromDB();
        configHolder.setTargetPosition(targetMySqlOffsetState.get(0).getBinlogPosition());
        configHolder.setTargetFileName(targetMySqlOffsetState.get(0).getBinlogFilename());
        LOGGER.info("Target Offset extracted from MySQL : {}", targetMySqlOffsetState);

        if (pipe.state == null || pipe.state.isEmpty() || pipe.state.get("mysql_cdc_offset").isEmpty()) {

            // Step 1: Extract latest Offset from MySQL and Save it in a file
            // ToDo: We can probably leave it up to the debezium engine to handle this

            LOGGER.error("Pipe state is null");

            final Path cdcWorkingDir;
            try {
                cdcWorkingDir = Files.createTempDirectory(Path.of("/tmp"), "cdc-state-offset");
            }
            catch (final IOException e) {
                throw new RuntimeException(e);
            }
            final Path cdcOffsetFilePath = cdcWorkingDir.resolve("offset.dat");
            LOGGER.info("Offset file: {}", cdcOffsetFilePath);

            // Step 2: Create Schema History Storage empty file

            MySQLSchemaHistoryStorage schemaHistoryStorage = MySQLSchemaHistoryStorage.initializeDBHistory(
                    new MySQLSchemaHistory<>(Optional.empty(), false),
                    true);

            // Step 3: Return empty offset and schema files with target pos and filename

            configHolder.setSchemHistoryFileName(schemaHistoryStorage.getPath().toString());
            configHolder.setOffsetFileName(cdcOffsetFilePath.toString());

            return configHolder;

        }
        else {

            // Step 1: Extract Offset from backend, validate if the state exists in the server
            LOGGER.info("Pipe state is found {} ", pipe.state);

            LOGGER.info("Validate if the offset is still valid in MySQL");
            JsonNode offset = pipe.state.get("mysql_cdc_offset");

            Map<String, String> offsetMap = mySqlOffsetUtils.offsetAsMap(offset, mysqlConfig.getDatabaseName());

            JsonNode offsetAsJsonNode = null;
            offsetAsJsonNode = objectMapper.readTree(offsetMap.values().iterator().next());

            assert offsetAsJsonNode != null;

            if (mySqlOffsetUtils.savedOffsetStillValid(offsetAsJsonNode.get("file").asText())) {

                // Step 2: If state is valid persist into a file and return the file path

                MySQLOffset mySQLOffset = MySQLOffset.initializeState(pipe.state.get("mysql_cdc_offset"), Optional.of(mysqlConfig.getDatabaseName()));

                LOGGER.info("Created Offset file {}", mySQLOffset.getOffsetFilePath());
                LOGGER.info("Persisted latest offset to: {}", mySQLOffset.getOffsetFilePath());

                // Step 3: Create Schema History Storage empty file

                MySQLSchemaHistoryStorage schemaHistoryStorage = MySQLSchemaHistoryStorage.initializeDBHistory(
                        new MySQLSchemaHistory<>(Optional.empty(), false),
                        true);

                // Step 4: Start Debezium Engine with the Offset and Schema History Storage

                executor = runDebeziumEngine(schemaHistoryStorage.getPath(),
                        mySQLOffset.getOffsetFilePath(),
                        pipe);

                if (closeOnSuccessOrFailure(Instant.now())) {
                    configHolder.setSchemHistoryFileName(schemaHistoryStorage.getPath().toString());
                    configHolder.setOffsetFileName(mySQLOffset.getOffsetFilePath().toString());
                    configHolder.setServerName(mysqlConfig.getDatabaseName());
                    return configHolder;
                }
                return null;
            }
        }
        return null;
    }

}
