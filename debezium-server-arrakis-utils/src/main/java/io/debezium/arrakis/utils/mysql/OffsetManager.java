package io.debezium.arrakis.utils.mysql;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.DATABASE_NAME;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.util.SafeObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.common.OffsetReader;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlOffsetContext.Loader;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.embedded.KafkaConnectUtil;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;

public class OffsetManager {

    private final Path offsetFilePath;
    private final Optional<String> dbName;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetManager.class);

    private static final BiFunction<String, String, String> SQL_SERVER_STATE_MUTATION = (key, databaseName) -> key.substring(0, key.length() - 2)
            + ",\"database\":\"" + databaseName + "\"" + key.substring(key.length() - 2);

    Map<String, String> INTERNAL_CONVERTER_CONFIG = Map.of(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, Boolean.FALSE.toString());

    public OffsetManager(final Path offsetFilePath, final Optional<String> dbName) {
        this.offsetFilePath = offsetFilePath;
        this.dbName = dbName;
    }

    public Path getOffsetFilePath() {
        return offsetFilePath;
    }

    private Map<String, String> updateStateForDebezium2_1(final Map<String, String> mapAsString) {
        final Map<String, String> updatedMap = new LinkedHashMap<>();
        if (mapAsString.size() > 0) {
            final String key = mapAsString.keySet().stream().collect(Collectors.toList()).get(0);
            final int i = key.indexOf('[');
            final int i1 = key.lastIndexOf(']');

            if (i == 0 && i1 == key.length() - 1) {
                // The state is Debezium 2.1 compatible. No need to change anything.
                return mapAsString;
            }

            final String newKey = dbName.isPresent() ? SQL_SERVER_STATE_MUTATION.apply(key.substring(i, i1 + 1), dbName.get()) : key.substring(i, i1 + 1);
            final String value = mapAsString.get(key);
            updatedMap.put(newKey, value);
        }
        return updatedMap;
    }

    private static String byteBufferToString(final ByteBuffer byteBuffer) {
        return new String(byteBuffer.array(), StandardCharsets.UTF_8);
    }

    private static ByteBuffer stringToByteBuffer(final String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    public FileOffsetBackingStore getFileOffsetBackingStore(final Properties properties) {
        final FileOffsetBackingStore fileOffsetBackingStore = KafkaConnectUtil.fileOffsetBackingStore();
        final Map<String, String> propertiesMap = Configuration.from(properties).asMap();
        propertiesMap.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        propertiesMap.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        fileOffsetBackingStore.configure(new StandaloneConfig(propertiesMap));
        fileOffsetBackingStore.start();
        return fileOffsetBackingStore;
    }

    public JsonConverter getKeyConverter() {
        final JsonConverter keyConverter = new JsonConverter();
        keyConverter.configure(INTERNAL_CONVERTER_CONFIG, true);
        return keyConverter;
    }

    public JsonConverter getValueConverter() {
        final JsonConverter valueConverter = new JsonConverter();
        valueConverter.configure(INTERNAL_CONVERTER_CONFIG, false);
        return valueConverter;
    }

    public OffsetStorageReaderImpl getOffsetStorageReader(final FileOffsetBackingStore fileOffsetBackingStore, final Properties properties) {
        return new OffsetStorageReaderImpl(fileOffsetBackingStore, properties.getProperty("name"), getKeyConverter(),
                getValueConverter());
    }

    public Optional<OffsetManager> savedOffset(final JsonNode cdcOffset) {
        if (Objects.isNull(cdcOffset)) {
            return Optional.empty();
        }

        OffsetManager offsetManager = initializeState(cdcOffset, Optional.empty());
        return Optional.of(offsetManager);
    }

    public Optional<MysqlDebeziumStateAttributes> parseSavedOffset(final Properties properties) {
        FileOffsetBackingStore fileOffsetBackingStore = null;
        OffsetStorageReaderImpl offsetStorageReader = null;

        try {
            fileOffsetBackingStore = getFileOffsetBackingStore(properties);
            offsetStorageReader = getOffsetStorageReader(fileOffsetBackingStore, properties);

            final MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(Configuration.from(properties));
            final MySqlOffsetContext.Loader loader = new MySqlOffsetContext.Loader(connectorConfig);
            final Set<Partition> partitions = Collections.singleton(new MySqlPartition(connectorConfig.getLogicalName(), properties.getProperty(DATABASE_NAME.name())));

            final OffsetReader<Partition, MySqlOffsetContext, Loader> offsetReader = new OffsetReader<>(offsetStorageReader,
                    loader);
            final Map<Partition, MySqlOffsetContext> offsets = offsetReader.offsets(partitions);

            return extractStateAttributes(partitions, offsets);
        }
        finally {
            LOGGER.info("Closing offsetStorageReader and fileOffsetBackingStore");
            if (offsetStorageReader != null) {
                offsetStorageReader.close();
            }

            if (fileOffsetBackingStore != null) {
                fileOffsetBackingStore.stop();
            }
        }
    }

    public Map<String, String> read() {
        final Map<ByteBuffer, ByteBuffer> raw = load();

        return raw.entrySet().stream().collect(Collectors.toMap(
                e -> byteBufferToString(e.getKey()),
                e -> byteBufferToString(e.getValue())));
    }

    public JsonNode serialize(final Map<String, String> offset) {

        final ObjectMapper objectMapper = new ObjectMapper();

        final Map<String, Object> state = new HashMap<>();
        state.put("mysql_cdc_offset", offset);

        return objectMapper.valueToTree(state);
    }

    /**
     * See FileOffsetBackingStore#load - logic is mostly borrowed from here. duplicated because this
     * method is not public. Reduced the try catch block to only the read operation from original code
     * to reduce errors when reading the file.
     */
    @SuppressWarnings("unchecked")
    private Map<ByteBuffer, ByteBuffer> load() {
        Object obj;
        try (final SafeObjectInputStream is = new SafeObjectInputStream(Files.newInputStream(offsetFilePath))) {
            // todo (cgardens) - we currently suppress a security warning for this line. use of readObject from
            // untrusted sources is considered unsafe. Since the source is controlled by us in this case it
            // should be safe. That said, changing this implementation to not use readObject would remove some
            // headache.
            obj = is.readObject();
        }
        catch (final NoSuchFileException | EOFException e) {
            // NoSuchFileException: Ignore, may be new.
            // EOFException: Ignore, this means the file was missing or corrupt
            return Collections.emptyMap();
        }
        catch (final IOException | ClassNotFoundException e) {
            throw new ConnectException(e);
        }

        if (!(obj instanceof HashMap))
            throw new ConnectException("Expected HashMap but found " + obj.getClass());
        final Map<byte[], byte[]> raw = (Map<byte[], byte[]>) obj;
        final Map<ByteBuffer, ByteBuffer> data = new HashMap<>();
        for (final Map.Entry<byte[], byte[]> mapEntry : raw.entrySet()) {
            final ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey()) : null;
            final ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue()) : null;
            data.put(key, value);
        }

        return data;
    }

    private Optional<MysqlDebeziumStateAttributes> extractStateAttributes(final Set<Partition> partitions,
                                                                          final Map<Partition, MySqlOffsetContext> offsets) {
        boolean found = false;
        for (final Partition partition : partitions) {
            final MySqlOffsetContext mySqlOffsetContext = offsets.get(partition);

            if (mySqlOffsetContext != null) {
                found = true;
                LOGGER.info("Found previous partition offset {}: {}", partition, mySqlOffsetContext.getOffset());
            }
        }

        if (!found) {
            LOGGER.info("No previous offsets found");
            return Optional.empty();
        }

        final Offsets<Partition, MySqlOffsetContext> of = Offsets.of(offsets);
        final MySqlOffsetContext previousOffset = of.getTheOnlyOffset();

        return Optional.of(new MysqlDebeziumStateAttributes(previousOffset.getSource().binlogFilename(), previousOffset.getSource().binlogPosition(),
                Optional.ofNullable(previousOffset.gtidSet())));

    }

    public void persist(final JsonNode cdcState) {
        final Map<String, String> mapAsString = cdcState != null ? objectMapper.convertValue(cdcState, Map.class) : Collections.emptyMap();

        final Map<String, String> updatedMap = updateStateForDebezium2_1(mapAsString);

        final Map<ByteBuffer, ByteBuffer> mappedAsStrings = updatedMap.entrySet().stream().collect(Collectors.toMap(
                e -> stringToByteBuffer(e.getKey()),
                e -> stringToByteBuffer(e.getValue())));

        FileUtils.deleteQuietly(offsetFilePath.toFile());
        save(mappedAsStrings);
    }

    private void save(final Map<ByteBuffer, ByteBuffer> data) {
        try (final ObjectOutputStream os = new ObjectOutputStream(Files.newOutputStream(offsetFilePath))) {
            final Map<byte[], byte[]> raw = new HashMap<>();
            for (final Map.Entry<ByteBuffer, ByteBuffer> mapEntry : data.entrySet()) {
                final byte[] key = (mapEntry.getKey() != null) ? mapEntry.getKey().array() : null;
                final byte[] value = (mapEntry.getValue() != null) ? mapEntry.getValue().array() : null;
                raw.put(key, value);
            }
            os.writeObject(raw);
        }
        catch (final IOException e) {
            throw new ConnectException(e);
        }
    }

    public static OffsetManager initializeState(final JsonNode cdcState, final Optional<String> dbName) {
        final Path cdcWorkingDir;
        try {
            cdcWorkingDir = Files.createTempDirectory(Path.of("/tmp"), "cdc-state-offset");
        }
        catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final Path cdcOffsetFilePath = cdcWorkingDir.resolve("offset.dat");

        final OffsetManager offsetManager = new OffsetManager(cdcOffsetFilePath, dbName);

        offsetManager.persist(cdcState);
        return offsetManager;
    }

}
