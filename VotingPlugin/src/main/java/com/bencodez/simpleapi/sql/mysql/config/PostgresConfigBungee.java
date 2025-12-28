package com.bencodez.simpleapi.sql.mysql.config;

import net.md_5.bungee.config.Configuration;

/**
 * Simple Postgres wrapper mirroring the existing MySQL bungee config loader.
 */
public class PostgresConfigBungee extends MysqlConfigBungee {

    public PostgresConfigBungee(Configuration config) {
        super(config);
    }
}
