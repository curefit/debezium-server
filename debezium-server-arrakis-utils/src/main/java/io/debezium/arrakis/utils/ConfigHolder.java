package io.debezium.arrakis.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@AllArgsConstructor
@NoArgsConstructor
public class ConfigHolder {

    @JsonProperty("target_position")
    public long targetPosition;

    @JsonProperty("target_filename")
    public String targetFileName;

    @JsonProperty("schem_history_file_name")
    public String schemHistoryFileName;

    @JsonProperty("offset_file_name")
    public String offsetFileName;

    @JsonProperty("short_pipe_id")
    public String shortPipeId;

    @JsonProperty("pipeline_type")
    public String pipelineType;

    @JsonProperty("server_name")
    public String serverName;

}
