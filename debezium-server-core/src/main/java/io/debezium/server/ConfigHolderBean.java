package io.debezium.server;

import jakarta.enterprise.context.ApplicationScoped;

import io.debezium.arrakis.utils.ConfigHolder;

@ApplicationScoped
public class ConfigHolderBean {

    public static ConfigHolder configHolder;

}
