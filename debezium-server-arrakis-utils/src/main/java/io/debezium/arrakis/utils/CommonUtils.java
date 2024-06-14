package io.debezium.arrakis.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonUtils {

    private final Logger LOGGER = LoggerFactory.getLogger(CommonUtils.class);

    public int generateServerID() {
        final int min = 5400;
        final int max = 6400;

        final int serverId = (int) Math.floor(Math.random() * (max - min + 1) + min);
        LOGGER.info("Randomly generated Server ID : " + serverId);
        return serverId;
    }

}
