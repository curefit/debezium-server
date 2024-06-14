package io.debezium.arrakis.utils.mysql;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MysqlDebeziumStateAttributes {
    private final String binlogFilename;
    private final long binlogPosition;
    private final Optional<String> gtidSet;

    public MysqlDebeziumStateAttributes(String binlogFilename, long binlogPosition, Optional<String> gtidSet) {
        this.binlogFilename = binlogFilename;
        this.binlogPosition = binlogPosition;
        this.gtidSet = gtidSet;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }

    public Optional<String> getGtidSet() {
        return gtidSet;
    }

    public JsonNode format(final String dbName, final Instant time) {
        final String key = "[\"" + dbName + "\",{\"server\":\"" + dbName + "\"}]";
        final String gtidSet = getGtidSet().isPresent() ? ",\"gtids\":\"" + getGtidSet().get() + "\"" : "";
        final String value = "{\"transaction_id\":null,\"ts_sec\":" + time.getEpochSecond() + ",\"file\":\"" + getBinlogFilename() + "\",\"pos\":"
                + getBinlogPosition()
                + gtidSet + "}";

        final Map<String, String> result = new HashMap<>();
        result.put(key, value);

        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode jsonNode = objectMapper.valueToTree(result);

        return jsonNode;
    }

    @Override
    public String toString() {
        return "MysqlDebeziumStateAttributes{" +
                "binlogFilename='" + binlogFilename + '\'' +
                ", binlogPosition=" + binlogPosition +
                ", gtidSet=" + gtidSet +
                '}';
    }
}
