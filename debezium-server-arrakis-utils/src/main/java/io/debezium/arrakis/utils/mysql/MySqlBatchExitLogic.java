package io.debezium.arrakis.utils.mysql;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MySqlBatchExitLogic {

    final Pattern FILE_PATTERN = Pattern.compile("\"file\"\\s*:\\s*\"([^\"]+)\"");
    final Pattern POS_PATTERN = Pattern.compile("\"pos\"\\s*:\\s*(\\d+)");
    final Pattern SNAPSHOT_PATTERN = Pattern.compile("\"snapshot\"\\s*:\\s*\"([^\"]+)\"");

    public boolean reachedTarget(String record, String targetFileName, long targetPosition) {

        Matcher fileMatcher = FILE_PATTERN.matcher(record);
        Matcher posMatcher = POS_PATTERN.matcher(record);
        Matcher snapshotMatcher = SNAPSHOT_PATTERN.matcher(record);

        String file = fileMatcher.find() ? fileMatcher.group(1) : "";
        long pos = posMatcher.find() ? Long.parseLong(posMatcher.group(1)) : -1;
        String snapshot = snapshotMatcher.find() ? snapshotMatcher.group(1) : "";

        if (Objects.equals(snapshot, "last")) {
            return true;
        }
        if (Objects.equals(snapshot, "false")) {
            assert !Objects.equals(file, "");
            boolean isEventPositionAfter = file.compareTo(targetFileName) > 0 || (file.compareTo(
                    targetFileName) == 0
                    && pos >= targetPosition);
            if (isEventPositionAfter) {
                return true;
            }
            return false;
        }
        return false;
    }

}
