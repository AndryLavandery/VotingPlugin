package com.bencodez.simpleapi.sql.postgresql.config;

import org.bukkit.configuration.ConfigurationSection;

import com.bencodez.simpleapi.sql.mysql.DbType;
import com.bencodez.simpleapi.sql.mysql.config.MysqlConfigSpigot;

/**
 * Spigot configuration wrapper for PostgreSQL connections. Mirrors the MySQL
 * configuration reader but forces the DbType to POSTGRESQL so downstream
 * connection managers build PostgreSQL URLs and drivers.
 */
public class PostgresConfigSpigot extends MysqlConfigSpigot {

    public PostgresConfigSpigot(ConfigurationSection section) {
        super(section);
        setDbType(DbType.POSTGRESQL);
    }
}
