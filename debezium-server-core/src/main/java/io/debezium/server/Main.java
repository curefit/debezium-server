/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.io.IOException;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.arrakis.utils.ConfigHolder;
import io.debezium.arrakis.utils.mysql.SchemaRecovery;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;

@QuarkusMain
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String... args) throws InterruptedException, IOException, SQLException {

        SchemaRecovery schemaRecovery = new SchemaRecovery();
        ConfigHolder configHolder = schemaRecovery.recoverSchema();
        LOGGER.info("Recovered schema : {}", configHolder);

        // Set the ConfigHolder in ConfigHolderBean
        ConfigHolderBean.configHolder = configHolder;

        Quarkus.run(args);
    }

}
