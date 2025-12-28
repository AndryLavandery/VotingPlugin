package com.bencodez.simpleapi.sql.mysql.config;

import com.bencodez.simpleapi.file.velocity.VelocityYMLFile;
import com.bencodez.simpleapi.sql.mysql.DbType;

import ninja.leaping.configurate.ConfigurationNode;

public class PostgresConfigVelocity extends MysqlConfig {

        public PostgresConfigVelocity(String prePath, VelocityYMLFile config) {
                load(prePath, config);
        }

        public PostgresConfigVelocity(VelocityYMLFile config) {
                load("", config);
        }

        private void load(String prePath, VelocityYMLFile config) {
                // --- Basic Table Info ---
                setTablePrefix(config.getString(node(config, prePath, "Prefix"), ""));
                String name = config.getString(node(config, prePath, "Name"), "");
                if (name != null && !name.isEmpty()) {
                        setTableName(name);
                }

                // --- Connection Info ---
                setHostName(config.getString(node(config, prePath, "Host"), ""));
                setPort(config.getInt(node(config, prePath, "Port"), 0));
                setUser(config.getString(node(config, prePath, "Username"), ""));
                setPass(config.getString(node(config, prePath, "Password"), ""));
                setDatabase(config.getString(node(config, prePath, "Database"), ""));

                // --- Pool Settings ---
                setLifeTime(config.getLong(node(config, prePath, "MaxLifeTime"), -1));
                setMaxThreads(config.getInt(node(config, prePath, "MaxConnections"), 1));
                if (getMaxThreads() < 1) {
                        setMaxThreads(1);
                }

                // Recommended tuning defaults
                setMinimumIdle(config.getInt(node(config, prePath, "MinimumIdle"), 2)); // 2
                setIdleTimeoutMs(config.getLong(node(config, prePath, "IdleTimeoutMs"), 10 * 60_000L)); // 10 min
                setKeepaliveMs(config.getLong(node(config, prePath, "KeepaliveMs"), 5 * 60_000L)); // 5 min
                setValidationMs(config.getLong(node(config, prePath, "ValidationMs"), 5_000L)); // 5 s
                setLeakDetectMs(config.getLong(node(config, prePath, "LeakDetectMs"), 20_000L)); // 20 s
                setConnectionTimeout(config.getInt(node(config, prePath, "ConnectionTimeout"), 50_000)); // 50 s

                // --- Driver / DB Selection ---
                setDbType(DbType.POSTGRESQL);

                String driver = config.getString(node(config, prePath, "Driver"), "");
                if (driver == null || driver.isEmpty()) {
                        setDriver("org.postgresql.Driver");
                } else {
                        setDriver(driver);
                }

                // --- Driver / Behavior Options ---
                setUseSSL(config.getBoolean(node(config, prePath, "UseSSL"), false));
                setPublicKeyRetrieval(false);
                setUseMariaDB(false);

                // --- Additional Settings ---
                setLine(config.getString(node(config, prePath, "Line"), ""));
                setDebug(config.getBoolean(node(config, prePath, "Debug"), false));
                setPoolName(config.getString(node(config, prePath, "PoolName"), ""));
        }

        /**
         * Returns a valid config node. - If prePath is empty/null -> root-level key -
         * Otherwise -> prePath.key
         */
        private ConfigurationNode node(VelocityYMLFile config, String prePath, String key) {
                if (prePath == null || prePath.isEmpty()) {
                        return config.getNode(key);
                }
                return config.getNode(prePath, key);
        }
}
