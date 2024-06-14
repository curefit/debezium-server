package io.debezium.arrakis.utils.mysql;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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

import io.arrakis.commons.enums.PipelineType;
import io.arrakis.commons.pojos.MySQLConfigPoJo;
import io.arrakis.commons.responses.GetPipeByIdResponse;
import io.arrakis.commons.utils.PipelineUtils;
import io.debezium.arrakis.utils.CommonUtils;
import io.debezium.arrakis.utils.ConfigHolder;
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

    private GetPipeByIdResponse getPipeInfoApi(String url) throws IOException, InterruptedException, RuntimeException {
        // Create HttpClient
        HttpClient client = HttpClient.newHttpClient();
        LOGGER.info("Getting pipe info from: " + url);
        // Create HttpRequest
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("accept", "application/json")
                .build();

        // Send request and get response
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // Check if the response status is 200 (OK)
        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to get response: "
                    + response.statusCode()
                    + " " + response.body()
                    + " for url: " + url);
        }

        // Convert response body to JsonNode
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(response.body(), GetPipeByIdResponse.class);
    }

    private Path createOffsetFile() {
        final Path cdcWorkingDir;
        try {
            cdcWorkingDir = Files.createTempDirectory(Path.of("/tmp"), "cdc-state-offset");
        }
        catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return cdcWorkingDir;
    }

    private ExecutorService runDebeziumEngine(Path offsetFilePath,
                                              Path schemaHistoryPath,
                                              String dbName,
                                              String password,
                                              String host,
                                              String username,
                                              int port) {

        final Config config = ConfigProvider.getConfig();
        final CommonUtils commonUtils = new CommonUtils();

        Properties props = new Properties();

        props.setProperty("name", dbName);
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        props.setProperty("snapshot.mode", "schema_only_recovery");
        props.setProperty("schema.history.internal.store.only.captured.tables.ddl", "false");
        props.setProperty("max.queue.size", "8192");
        props.setProperty("topic.prefix", dbName + "-" + configHolder.getShortPipeId());
        props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename", schemaHistoryPath.toString());
        props.setProperty("binary.handling.mode", "base64");
        props.setProperty("offset.storage.file.filename", offsetFilePath.toString());
        props.setProperty("decimal.handling.mode", "string");
        props.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.setProperty("database.server.id", String.valueOf(commonUtils.generateServerID()));
        props.setProperty("database.server.name", dbName + "-" + configHolder.getShortPipeId());
        props.setProperty("max.queue.size.in.bytes", "268435456");
        props.setProperty("errors.retry.delay.max.ms", "300");
        props.setProperty("offset.flush.timeout.ms", "5000");
        props.setProperty("database.password", password);
        props.setProperty("database.user", username);
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("offset.flush.interval.ms", "1000");
        props.setProperty("database.ssl.mode", "PREFERRED");
        props.setProperty("database.hostname", host);
        props.setProperty("database.include.list", dbName);

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

    public ConfigHolder recoverSchema() throws IOException, InterruptedException, SQLException {

        final Config config = ConfigProvider.getConfig();
        final ObjectMapper objectMapper = new ObjectMapper();

        String ARRAKIS_URL = "arrakis.backend.url";
        String ARRAKIS_PIPE_ID = "arrakis.pipeline.id";
        String PIPE_INFO_API = "/api/v1/pipes/id/";

        GetPipeByIdResponse pipe = getPipeInfoApi("http://" + config.getValue(ARRAKIS_URL, String.class)
                + ":9094"
                + PIPE_INFO_API
                + config.getValue(ARRAKIS_PIPE_ID, String.class));

        LOGGER.info("Pipe info: " + pipe.toString());

        String shotPipeId = PipelineUtils.uuidToShort(UUID.fromString(config.getValue(ARRAKIS_PIPE_ID, String.class)));

        configHolder.setPipelineType(pipe.pipelineType);
        configHolder.setShortPipeId(shotPipeId);

        MySQLConfigPoJo configPoJo = objectMapper.readValue(pipe.configuration.toString(),
                MySQLConfigPoJo.class);

        MySqlOffset mySqlOffset = new MySqlOffset(configPoJo.getUsername(),
                configPoJo.getPassword(),
                configPoJo.getHost(),
                configPoJo.getPort());

        LOGGER.info("Extracting latest Offset from MySQL for target position and filename: ");
        MysqlDebeziumStateAttributes targetMySqlOffsetState = mySqlOffset.getStateAttributesFromDB();
        configHolder.setTargetPosition(targetMySqlOffsetState.getBinlogPosition());
        configHolder.setTargetFileName(targetMySqlOffsetState.getBinlogFilename());
        LOGGER.info("Target Offset extracted from MySQL : {}", targetMySqlOffsetState.format("arrakis", Instant.now()));

        if (pipe.state == null || pipe.state.isEmpty()) {

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

            SchemaHistoryStorage schemaHistoryStorage = SchemaHistoryStorage.initializeDBHistory(
                    new SchemaHistory<>(Optional.empty(), false),
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

            Map<String, String> offsetMap = mySqlOffset.offsetAsMap(offset, configPoJo.getDatabase());

            JsonNode offsetAsJsonNode = null;
            offsetAsJsonNode = objectMapper.readTree(offsetMap.values().iterator().next());

            assert offsetAsJsonNode != null;

            if (mySqlOffset.savedOffsetStillValid(offsetAsJsonNode.get("file").asText())) {

                // Step 2: If state is valid persist into a file and return the file path

                OffsetManager offsetManager = OffsetManager.initializeState(pipe.state.get("mysql_cdc_offset"),
                        configPoJo.getDatabase().isEmpty() ? Optional.empty() : Optional.of(configPoJo.getDatabase()));
                LOGGER.info("Created Offset file {}", offsetManager.getOffsetFilePath());
                LOGGER.info("Persisted latest offset to: {}", offsetManager.getOffsetFilePath());

                // Step 3: Create Schema History Storage empty file

                SchemaHistoryStorage schemaHistoryStorage = SchemaHistoryStorage.initializeDBHistory(
                        new SchemaHistory<>(Optional.empty(), false),
                        true);

                // Step 4: Start Debezium Engine with the Offset and Schema History Storage

                executor = runDebeziumEngine(offsetManager.getOffsetFilePath(),
                        schemaHistoryStorage.getPath(),
                        configPoJo.getDatabase(),
                        configPoJo.getPassword(),
                        configPoJo.getHost(),
                        configPoJo.getUsername(),
                        configPoJo.getPort());

                if (closeOnSuccessOrFailure(Instant.now())) {
                    configHolder.setSchemHistoryFileName(schemaHistoryStorage.getPath().toString());
                    configHolder.setOffsetFileName(offsetManager.getOffsetFilePath().toString());
                    configHolder.setServerName(configPoJo.getDatabase());
                    return configHolder;
                }
                return null;
            }
        }
        return null;
    }

}
