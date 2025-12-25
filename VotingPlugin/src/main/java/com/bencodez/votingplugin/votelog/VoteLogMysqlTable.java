package com.bencodez.votingplugin.votelog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.sql.Types;

import com.bencodez.simpleapi.sql.mysql.AbstractSqlTable;
import com.bencodez.simpleapi.sql.mysql.DbType;
import com.bencodez.simpleapi.sql.mysql.MySQL;
import com.bencodez.simpleapi.sql.mysql.config.MysqlConfig;
import com.bencodez.simpleapi.sql.mysql.queries.Query;

/**
 * Vote log table usable from BOTH proxy and backend.
 *
 * LOG ONLY table (not the offline queue).
 *
 * Records the "main reward triggers" (not every command executed).
 *
 * Columns: - id (auto increment primary key) - vote_id (correlation id / vote
 * uuid; NOT UNIQUE; may repeat for other events) - vote_time (LONG millis,
 * explicit time event happened) - event (VOTE_RECEIVED default; includes
 * reward-trigger events) - context (which reward/config fired + any extra info
 * like TimeType, ex: "TopVoter:MONTH") - status (IMMEDIATE/CACHED) -
 * cached_total (int, proxy only snapshot; can be 0 on backend) - service -
 * player_uuid - player_name
 */
public abstract class VoteLogMysqlTable extends AbstractSqlTable {

	public enum VoteLogEvent {
		// Core
		VOTE_RECEIVED,

		// Reward triggers
		ALL_SITES_REWARD, ALMOST_ALL_SITES_REWARD, FIRST_VOTE_REWARD, FIRST_VOTE_TODAY_REWARD, CUMULATIVE_REWARD,
		MILESTONE_REWARD, VOTE_STREAK_REWARD, TOP_VOTER_REWARD
	}

	public enum VoteLogStatus {
		IMMEDIATE, CACHED
	}

        private final boolean debug;

        public abstract void logSevere1(String string);

        public abstract void logInfo1(String string);

        public abstract void debug1(SQLException e);

        public VoteLogMysqlTable(String tableName, MysqlConfig config, boolean debug) {
                super(resolveTableName(tableName, config), config, debug);
                this.debug = debug;

                initializeSchema();
        }

        public VoteLogMysqlTable(String tableName, MySQL existingMysql, MysqlConfig config, boolean debug) {
                super(resolveTableName(tableName, config), existingMysql, existingMysql.getConnectionManager().getDbType());
                this.debug = debug;

                initializeSchema();
        }

        public String getName() {
                return getTableName();
        }

        private static String resolveTableName(String tableName, MysqlConfig config) {
                if (config.hasTableNameSet()) {
                        tableName = config.getTableName();
                }
                if (config.getTablePrefix() != null) {
                        tableName = config.getTablePrefix() + tableName;
                }
                return tableName;
        }

        @Override
        public void logSevere(String msg) {
                logSevere1(msg);
        }

        @Override
        public void logInfo(String msg) {
                logInfo1(msg);
        }

        @Override
        public void debug(Throwable t) {
                if (t instanceof SQLException) {
                        debug1((SQLException) t);
                } else if (debug) {
                        t.printStackTrace();
                }
        }

        @Override
        public String getPrimaryKeyColumn() {
                return "id";
        }

        @Override
        public String buildCreateTableSql(DbType dbType) {
                StringBuilder sb = new StringBuilder();

                if (dbType == DbType.POSTGRESQL) {
                        sb.append("CREATE TABLE IF NOT EXISTS ").append(qi(getTableName())).append(" (")
                                        .append(qi("id")).append(" BIGSERIAL PRIMARY KEY, ")
                                        .append(qi("vote_id")).append(" ").append(bestUuidType()).append(", ")
                                        .append(qi("vote_time")).append(" BIGINT NOT NULL DEFAULT 0, ")
                                        .append(qi("player_uuid")).append(" ").append(bestUuidType()).append(", ")
                                        .append(qi("player_name")).append(" VARCHAR(16), ")
                                        .append(qi("service")).append(" VARCHAR(64), ")
                                        .append(qi("event")).append(" VARCHAR(64) NOT NULL DEFAULT '")
                                        .append(VoteLogEvent.VOTE_RECEIVED.name()).append("', ")
                                        .append(qi("context")).append(" VARCHAR(128), ")
                                        .append(qi("status")).append(" VARCHAR(16), ")
                                        .append(qi("cached_total")).append(" INT NOT NULL DEFAULT 0").append(");");
                } else {
                        sb.append("CREATE TABLE IF NOT EXISTS ").append(qi(getTableName())).append(" (")
                                        .append(qi("id")).append(" BIGINT NOT NULL AUTO_INCREMENT, ")
                                        .append(qi("vote_id")).append(" VARCHAR(36), ")
                                        .append(qi("vote_time")).append(" BIGINT NOT NULL DEFAULT 0, ")
                                        .append(qi("player_uuid")).append(" VARCHAR(37), ")
                                        .append(qi("player_name")).append(" VARCHAR(16), ")
                                        .append(qi("service")).append(" VARCHAR(64), ")
                                        .append(qi("event")).append(" VARCHAR(64) NOT NULL DEFAULT '")
                                        .append(VoteLogEvent.VOTE_RECEIVED.name()).append("', ")
                                        .append(qi("context")).append(" VARCHAR(128), ")
                                        .append(qi("status")).append(" VARCHAR(16), ")
                                        .append(qi("cached_total")).append(" INT NOT NULL DEFAULT 0, ")
                                        .append("PRIMARY KEY (").append(qi("id")).append(")")
                                        .append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");
                }

                return sb.toString();
        }

        private void initializeSchema() {
                try {
                        new Query(mysql, buildCreateTableSql(getDbType())).executeUpdate();
                } catch (SQLException e) {
                        debug1(e);
                }

                ensureColumns();
                ensureIndexes();
        }

        public List<String> getDistinctServices(int days, int limit) {
                if (limit <= 0) {
                        limit = 45;
                }

                boolean useCutoff = days > 0;
                long cutoff = useCutoff ? System.currentTimeMillis() - (days * 24L * 60L * 60L * 1000L) : 0;

                String sql = "SELECT DISTINCT " + qi("service") + " FROM " + qi(getTableName()) + " WHERE " + qi("service")
                                + " IS NOT NULL AND " + qi("service") + " != '' " + (useCutoff ? "AND " + qi("vote_time")
                                                + " >= ? " : "") + "ORDER BY " + qi("service") + " ASC LIMIT ?;";

                try (Connection conn = mysql.getConnectionManager().getConnection();
                                PreparedStatement ps = conn.prepareStatement(sql)) {

                        int idx = 1;
                        if (useCutoff) {
                                ps.setLong(idx++, cutoff);
                        }
                        ps.setInt(idx, limit);

                        try (ResultSet rs = ps.executeQuery()) {
                                List<String> out = new java.util.ArrayList<>();
                                while (rs.next()) {
                                        out.add(rs.getString("service"));
                                }
                                return out;
                        }
                } catch (SQLException e) {
                        debug1(e);
                        return java.util.Collections.emptyList();
                }
        }

	public List<VoteLogEntry> getRecentAll(int days, int limit) {
		if (limit <= 0) {
			limit = 10;
		}

                boolean useCutoff = days > 0;
                long cutoff = useCutoff ? System.currentTimeMillis() - (days * 24L * 60L * 60L * 1000L) : 0;

                String sql = "SELECT " + qi("vote_id") + ", " + qi("vote_time") + ", " + qi("player_uuid") + ", "
                                + qi("player_name") + ", " + qi("service") + ", " + qi("event") + ", " + qi("context") + ", "
                                + qi("status") + ", " + qi("cached_total") + " FROM " + qi(getTableName())
                                + (useCutoff ? " WHERE " + qi("vote_time") + " >= ? " : "") + "ORDER BY " + qi("vote_time")
                                + " DESC, " + qi("id") + " DESC LIMIT ?;";

                if (useCutoff) {
                        return query(sql, new Object[] { cutoff, limit });
                }
                return query(sql, new Object[] { limit });
        }

	public List<VoteLogEntry> getRecentByEvent(VoteLogEvent event, int days, int limit) {
		if (event == null) {
			return (days > 0) ? getRecentAll(days, limit) : getRecent(limit);
		}
		if (limit <= 0) {
			limit = 10;
		}

                boolean useCutoff = days > 0;
                long cutoff = useCutoff ? System.currentTimeMillis() - (days * 24L * 60L * 60L * 1000L) : 0;

                String sql = "SELECT " + qi("vote_id") + ", " + qi("vote_time") + ", " + qi("player_uuid") + ", "
                                + qi("player_name") + ", " + qi("service") + ", " + qi("event") + ", " + qi("context") + ", "
                                + qi("status") + ", " + qi("cached_total") + " FROM " + qi(getTableName()) + " WHERE "
                                + qi("event") + "=? " + (useCutoff ? "AND " + qi("vote_time") + " >= ? " : "") + "ORDER BY "
                                + qi("vote_time") + " DESC, " + qi("id") + " DESC LIMIT ?;";

                if (useCutoff) {
                        return query(sql, new Object[] { event.name(), cutoff, limit });
                }
                return query(sql, new Object[] { event.name(), limit });
        }

	public List<VoteLogEntry> getRecent(int days, VoteLogEvent eventFilter, int limit) {
		if (limit <= 0) {
			limit = 10;
		}

                boolean useCutoff = days > 0;
                long cutoff = useCutoff ? System.currentTimeMillis() - (days * 24L * 60L * 60L * 1000L) : 0;
                String sql = "SELECT " + qi("vote_id") + ", " + qi("vote_time") + ", " + qi("player_uuid") + ", "
                                + qi("player_name") + ", " + qi("service") + ", " + qi("event") + ", " + qi("context") + ", "
                                + qi("status") + ", " + qi("cached_total") + " FROM " + qi(getTableName()) + " WHERE 1=1 "
                                + (eventFilter != null ? "AND " + qi("event") + "=? " : "")
                                + (useCutoff ? "AND " + qi("vote_time") + " >= ? " : "") + "ORDER BY " + qi("vote_time")
                                + " DESC, " + qi("id") + " DESC LIMIT ?";

                if (eventFilter != null && useCutoff) {
                        return query(sql, new Object[] { eventFilter.name(), cutoff, limit });
                } else if (eventFilter != null) {
                        return query(sql, new Object[] { eventFilter.name(), limit });
                } else if (useCutoff) {
                        return query(sql, new Object[] { cutoff, limit });
                }
                return query(sql, new Object[] { limit });
        }

        private void ensureColumns() {
                synchronized (this) {
                        addColumnIfMissing("vote_id", voteIdColumnDefinition());
                        addColumnIfMissing("vote_time", "BIGINT NOT NULL DEFAULT 0");
                        addColumnIfMissing("player_uuid", bestUuidType());
                        addColumnIfMissing("player_name", "VARCHAR(16)");
                        addColumnIfMissing("service", "VARCHAR(64)");
                        addColumnIfMissing("event",
                                        "VARCHAR(64) NOT NULL DEFAULT '" + VoteLogEvent.VOTE_RECEIVED.name() + "'");
                        addColumnIfMissing("context", "VARCHAR(128)");
                        addColumnIfMissing("status", "VARCHAR(16)");
                        addColumnIfMissing("cached_total", "INT NOT NULL DEFAULT 0");
                }
        }

        private void ensureIndexes() {
                String table = qi(getTableName());
                String[] definitions = { "(" + qi("vote_time") + ")", "(" + qi("player_uuid") + "," + qi("vote_time") + ")",
                                "(" + qi("service") + "," + qi("vote_time") + ")",
                                "(" + qi("status") + "," + qi("vote_time") + ")",
                                "(" + qi("event") + "," + qi("vote_time") + ")",
                                "(" + qi("context") + "," + qi("vote_time") + ")",
                                "(" + qi("event") + "," + qi("context") + "," + qi("vote_time") + ")",
                                "(" + qi("vote_id") + ")", "(" + qi("vote_id") + "," + qi("vote_time") + ")" };
                String[] names = { "idx_vote_time", "idx_uuid_time", "idx_service_time", "idx_status_time", "idx_event_time",
                                "idx_context_time", "idx_event_context_time", "idx_vote_id", "idx_vote_id_time" };

                for (int i = 0; i < names.length; i++) {
                        if (getDbType() == DbType.POSTGRESQL) {
                                try {
                                        new Query(mysql, "CREATE INDEX IF NOT EXISTS " + qi(names[i]) + " ON " + table + " "
                                                        + definitions[i] + ";").executeUpdate();
                                } catch (SQLException e) {
                                        debug1(e);
                                }
                        } else {
                                createIndexIgnoreDuplicate(names[i], definitions[i]);
                        }
                }
        }

        private void createIndexIgnoreDuplicate(String indexName, String cols) {
                String sql = "CREATE INDEX " + qi(indexName) + " ON " + qi(getTableName()) + " " + cols + ";";
                try {
                        new Query(mysql, sql).executeUpdate();
                } catch (SQLException e) {
                        if (isDuplicateIndex(e)) {
                                return;
                        }
                        debug1(e);
                }
        }

        private boolean isDuplicateIndex(SQLException e) {
                if (e == null) {
                        return false;
                }
                return e.getErrorCode() == 1061 || "42P07".equalsIgnoreCase(e.getSQLState());
        }

        private boolean columnExists(String column) {
                String schemaFilter = getDbType() == DbType.POSTGRESQL ? "table_schema = current_schema()"
                                : "TABLE_SCHEMA = DATABASE()";
                String sql = "SELECT 1 FROM information_schema.columns WHERE " + schemaFilter
                                + " AND table_name = ? AND column_name = ? LIMIT 1;";

                try (Connection conn = mysql.getConnectionManager().getConnection();
                                PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, getTableName());
                        ps.setString(2, column);

                        try (ResultSet rs = ps.executeQuery()) {
                                return rs.next();
                        }
                } catch (SQLException e) {
                        debug1(e);
                        return true;
                }
        }

        private void addColumnIfMissing(String column, String definition) {
                if (columnExists(column)) {
                        return;
                }

                String sql = "ALTER TABLE " + qi(getTableName()) + " ADD COLUMN " + qi(column) + " " + definition + ";";
                try {
                        new Query(mysql, sql).executeUpdate();
                } catch (SQLException e) {
                        debug1(e);
                }
        }

        private String voteIdColumnDefinition() {
                return getDbType() == DbType.POSTGRESQL ? bestUuidType() : "VARCHAR(36)";
        }

        private String selectColumns() {
                return qi("vote_id") + ", " + qi("vote_time") + ", " + qi("player_uuid") + ", " + qi("player_name") + ", "
                                + qi("service") + ", " + qi("event") + ", " + qi("context") + ", " + qi("status") + ", "
                                + qi("cached_total");
        }

	private static String escape(String in) {
		if (in == null) {
			return "";
		}
		return in.replace("\\", "\\\\").replace("'", "''");
	}

	private static String safeStr(String s) {
		return s == null ? "" : s;
	}

	public static String withTimeType(String baseContext, String timeType) {
		baseContext = safeStr(baseContext);
		timeType = safeStr(timeType);
		if (baseContext.isEmpty()) {
			return timeType.isEmpty() ? "" : timeType;
		}
		if (timeType.isEmpty()) {
			return baseContext;
		}
		return baseContext + ":" + timeType;
	}

	public String logVote(UUID voteUUID, VoteLogStatus status, String service, String playerUuid, String playerName,
			long voteTimeMillis, int proxyCachedTotal) {

		return logEvent(voteUUID, VoteLogEvent.VOTE_RECEIVED, null, status, service, playerUuid, playerName,
				voteTimeMillis, proxyCachedTotal);
	}

        public String logEvent(UUID voteUUID, VoteLogEvent event, String context, VoteLogStatus status, String service,
                        String playerUuid, String playerName, long timeMillis, int proxyCachedTotal) {

                String voteId = voteUUID != null ? voteUUID.toString() : null;

		if (event == null) {
			event = VoteLogEvent.VOTE_RECEIVED;
		}
		if (status == null) {
			status = VoteLogStatus.IMMEDIATE;
		}
                if (service == null) {
                        service = "";
                }

                String sql = "INSERT INTO " + qi(getTableName()) + " (" + qi("vote_id") + ", " + qi("vote_time") + ", "
                                + qi("player_uuid") + ", " + qi("player_name") + ", " + qi("service") + ", " + qi("event")
                                + ", " + qi("context") + ", " + qi("status") + ", " + qi("cached_total") + ") VALUES (?,?,?,?,?,?,?,?,?);";

                try (Connection conn = mysql.getConnectionManager().getConnection();
                                PreparedStatement ps = conn.prepareStatement(sql)) {
                        int idx = 1;
                        setUuidParam(ps, idx++, voteId);
                        ps.setLong(idx++, timeMillis);
                        setUuidParam(ps, idx++, playerUuid);
                        ps.setString(idx++, escape(playerName));
                        ps.setString(idx++, escape(service));
                        ps.setString(idx++, event.name());
                        setStringOrNull(ps, idx++, context);
                        ps.setString(idx++, status.name());
                        ps.setInt(idx, proxyCachedTotal);

                        ps.executeUpdate();
                } catch (SQLException e) {
                        debug1(e);
                }

                return voteId;
        }

        public String logEvent(UUID voteUUID, VoteLogEvent event, String context, String playerUuid, String playerName,
                        long timeMillis) {

                String voteId = voteUUID != null ? voteUUID.toString() : null;

                if (event == null) {
                        event = VoteLogEvent.VOTE_RECEIVED;
                }

                String sql = "INSERT INTO " + qi(getTableName()) + " (" + qi("vote_id") + ", " + qi("vote_time") + ", "
                                + qi("player_uuid") + ", " + qi("player_name") + ", " + qi("service") + ", " + qi("event")
                                + ", " + qi("context") + ", " + qi("status") + ", " + qi("cached_total") + ") VALUES (?,?,?,?,?,?,?,?,?);";

                try (Connection conn = mysql.getConnectionManager().getConnection();
                                PreparedStatement ps = conn.prepareStatement(sql)) {
                        int idx = 1;
                        setUuidParam(ps, idx++, voteId);
                        ps.setLong(idx++, timeMillis);
                        setUuidParam(ps, idx++, playerUuid);
                        ps.setString(idx++, escape(playerName));
                        ps.setString(idx++, "");
                        ps.setString(idx++, event.name());
                        setStringOrNull(ps, idx++, context);
                        ps.setString(idx++, "");
                        ps.setInt(idx, 0);

                        ps.executeUpdate();
                } catch (SQLException e) {
                        debug1(e);
                }

                return voteId;
        }

        private void setUuidParam(PreparedStatement ps, int idx, String uuid) throws SQLException {
                if (uuid == null || uuid.isEmpty()) {
                        ps.setNull(idx, Types.VARCHAR);
                        return;
                }

                if (getDbType() == DbType.POSTGRESQL) {
                        ps.setObject(idx, UUID.fromString(uuid));
                } else {
                        ps.setString(idx, uuid);
                }
        }

        private void setStringOrNull(PreparedStatement ps, int idx, String value) throws SQLException {
                if (value == null || value.isEmpty()) {
                        ps.setNull(idx, Types.VARCHAR);
                } else {
                        ps.setString(idx, escape(value));
                }
        }

	public String logEventNow(UUID voteUUID, VoteLogEvent event, String context, String playerUuid, String playerName) {
		return logEvent(voteUUID, event, context, playerUuid, playerName, System.currentTimeMillis());
	}

	public void logAllSitesReward(UUID voteUUID, String playerUuid, String playerName, long timeMillis,
			String rewardKey) {
		logEvent(voteUUID, VoteLogEvent.ALL_SITES_REWARD, safeStr(rewardKey), playerUuid, playerName, timeMillis);
	}

	public void logAlmostAllSitesReward(UUID voteUUID, String playerUuid, String playerName, long timeMillis,
			String rewardKey) {
		logEvent(voteUUID, VoteLogEvent.ALMOST_ALL_SITES_REWARD, safeStr(rewardKey), playerUuid, playerName,
				timeMillis);
	}

	public void logFirstVoteReward(UUID voteUUID, String playerUuid, String playerName, long timeMillis,
			String rewardKey) {
		logEvent(voteUUID, VoteLogEvent.FIRST_VOTE_REWARD, safeStr(rewardKey), playerUuid, playerName, timeMillis);
	}

	public void logFirstVoteTodayReward(UUID voteUUID, String playerUuid, String playerName, long timeMillis,
			String rewardKey) {
		logEvent(voteUUID, VoteLogEvent.FIRST_VOTE_TODAY_REWARD, safeStr(rewardKey), playerUuid, playerName,
				timeMillis);
	}

	public void logCumulativeReward(UUID voteUUID, String playerUuid, String playerName, long timeMillis,
			String cumulativeKeyWithTimeType) {
		logEvent(voteUUID, VoteLogEvent.CUMULATIVE_REWARD, safeStr(cumulativeKeyWithTimeType), playerUuid, playerName,
				timeMillis);
	}

	/**
	 * Milestone reward: include reset time type if applicable (ex:
	 * "Milestone50:MONTH").
	 */
	public void logMilestoneReward(UUID voteUUID, String playerUuid, String playerName, long timeMillis,
			String milestoneKeyWithTimeType) {
		logEvent(voteUUID, VoteLogEvent.MILESTONE_REWARD, safeStr(milestoneKeyWithTimeType), playerUuid, playerName,
				timeMillis);
	}

	public void logVoteStreakReward(UUID voteUUID, String playerUuid, String playerName, long timeMillis,
			String streakKeyWithTimeType) {
		logEvent(voteUUID, VoteLogEvent.VOTE_STREAK_REWARD, safeStr(streakKeyWithTimeType), playerUuid, playerName,
				timeMillis);
	}

	public void logTopVoterReward(UUID voteUUID, String playerUuid, String playerName, long timeMillis,
			String topVoterKeyWithTimeType) {
		logEvent(voteUUID, VoteLogEvent.TOP_VOTER_REWARD, safeStr(topVoterKeyWithTimeType), playerUuid, playerName,
				timeMillis);
	}

        public void purgeOlderThanDays(int days, int batchSize) {
                if (days <= 0) {
                        return;
                }
                if (batchSize <= 0) {
                        batchSize = 5000;
                }

                long cutoff = System.currentTimeMillis() - (days * 24L * 60L * 60L * 1000L);

                String table = qi(getTableName());
                String sql = "DELETE FROM " + table + " WHERE " + qi("id") + " IN (SELECT " + qi("id") + " FROM "
                                + table + " WHERE " + qi("vote_time") + " < ? ORDER BY " + qi("vote_time") + " ASC LIMIT ?);";

                try (Connection conn = mysql.getConnectionManager().getConnection();
                                PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setLong(1, cutoff);
                        ps.setInt(2, batchSize);
                        ps.executeUpdate();
                } catch (SQLException e) {
                        debug1(e);
                }
        }

        public VoteLogEntry getByVoteId(String voteId) {
                String sql = "SELECT " + selectColumns() + " FROM " + qi(getTableName()) + " WHERE " + qi("vote_id")
                                + "=? ORDER BY " + qi("vote_time") + " DESC, " + qi("id") + " DESC LIMIT 1;";
                List<VoteLogEntry> rows = query(sql, new Object[] { voteId });
                return rows.isEmpty() ? null : rows.get(0);
        }

        public List<VoteLogEntry> getByVoteIdAll(String voteId, int days, int limit) {
                if (limit <= 0) {
			limit = 10;
		}
                boolean useCutoff = days > 0;
                long cutoff = useCutoff ? System.currentTimeMillis() - (days * 24L * 60L * 60L * 1000L) : 0;

                String sql = "SELECT " + selectColumns() + " FROM " + qi(getTableName()) + " WHERE " + qi("vote_id") + "=? "
                                + (useCutoff ? "AND " + qi("vote_time") + " >= ? " : "") + "ORDER BY " + qi("vote_time")
                                + " DESC, " + qi("id") + " DESC LIMIT ?;";

                if (useCutoff) {
                        return query(sql, new Object[] { voteId, cutoff, limit });
                }
                return query(sql, new Object[] { voteId, limit });
        }

        public List<VoteLogEntry> getRecent(int limit) {
                if (limit <= 0) {
                        limit = 10;
                }
                String sql = "SELECT " + selectColumns() + " FROM " + qi(getTableName()) + " ORDER BY " + qi("vote_time")
                                + " DESC, " + qi("id") + " DESC LIMIT ?;";
                return query(sql, new Object[] { limit });
        }

	public List<VoteLogEntry> getByService(String service, int days, int limit) {
		return getByService(service, null, days, limit);
	}

	public List<VoteLogEntry> getByServiceVotesOnly(String service, int days, int limit) {
		return getByService(service, VoteLogEvent.VOTE_RECEIVED, days, limit);
	}

	public List<VoteLogEntry> getByService(String service, VoteLogEvent event, int days, int limit) {
                if (limit <= 0) {
                        limit = 10;
                }
                boolean useCutoff = days > 0;
                long cutoff = useCutoff ? System.currentTimeMillis() - (days * 24L * 60L * 60L * 1000L) : 0;

                String sql = "SELECT " + selectColumns() + " FROM " + qi(getTableName()) + " WHERE " + qi("service") + "=? "
                                + (event != null ? "AND " + qi("event") + "=? " : "")
                                + (useCutoff ? "AND " + qi("vote_time") + " >= ? " : "") + "ORDER BY " + qi("vote_time")
                                + " DESC, " + qi("id") + " DESC LIMIT ?;";

                if (event != null && useCutoff) {
                        return query(sql, new Object[] { service, event.name(), cutoff, limit });
                } else if (event != null) {
                        return query(sql, new Object[] { service, event.name(), limit });
                } else if (useCutoff) {
                        return query(sql, new Object[] { service, cutoff, limit });
                }
                return query(sql, new Object[] { service, limit });
        }

	public List<VoteLogEntry> getByPlayerName(String playerName, int days, int limit) {
		return getByPlayerName(playerName, null, days, limit);
	}

	public List<VoteLogEntry> getByPlayerNameVotesOnly(String playerName, int days, int limit) {
		return getByPlayerName(playerName, VoteLogEvent.VOTE_RECEIVED, days, limit);
	}

	public List<VoteLogEntry> getByPlayerName(String playerName, VoteLogEvent event, int days, int limit) {
                if (limit <= 0) {
                        limit = 10;
                }
                boolean useCutoff = days > 0;
                long cutoff = useCutoff ? System.currentTimeMillis() - (days * 24L * 60L * 60L * 1000L) : 0;

                String sql = "SELECT " + selectColumns() + " FROM " + qi(getTableName()) + " WHERE " + qi("player_name")
                                + "=? " + (event != null ? "AND " + qi("event") + "=? " : "")
                                + (useCutoff ? "AND " + qi("vote_time") + " >= ? " : "") + "ORDER BY " + qi("vote_time")
                                + " DESC, " + qi("id") + " DESC LIMIT ?;";

                if (event != null && useCutoff) {
                        return query(sql, new Object[] { playerName, event.name(), cutoff, limit });
                } else if (event != null) {
                        return query(sql, new Object[] { playerName, event.name(), limit });
                } else if (useCutoff) {
                        return query(sql, new Object[] { playerName, cutoff, limit });
                }
                return query(sql, new Object[] { playerName, limit });
        }

	public VoteLogCounts getCounts(int days) {
		return getCounts(days, VoteLogEvent.VOTE_RECEIVED);
	}

        public VoteLogCounts getCounts(int days, VoteLogEvent eventFilter) {
                boolean useCutoff = days > 0;
                long cutoff = useCutoff ? System.currentTimeMillis() - (days * 24L * 60L * 60L * 1000L) : 0;

                String sql = "SELECT COUNT(*) AS total, " + "SUM(CASE WHEN " + qi("status") + " = 'IMMEDIATE' THEN 1 ELSE 0 END) AS immediate, "
                                + "SUM(CASE WHEN " + qi("status") + " = 'CACHED' THEN 1 ELSE 0 END) AS cached FROM "
                                + qi(getTableName()) + " WHERE 1=1 " + (eventFilter != null ? "AND " + qi("event") + "=? " : "")
                                + (useCutoff ? "AND " + qi("vote_time") + " >= ? " : "") + ";";

		try (Connection conn = mysql.getConnectionManager().getConnection();
				PreparedStatement ps = conn.prepareStatement(sql)) {

			int idx = 1;
			if (eventFilter != null) {
				ps.setString(idx++, eventFilter.name());
			}
			if (useCutoff) {
				ps.setLong(idx++, cutoff);
			}

			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					long total = rs.getLong("total");
					long immediate = rs.getLong("immediate");
					long cached = rs.getLong("cached");
					return new VoteLogCounts(total, immediate, cached);
				}
			}
		} catch (SQLException e) {
			debug1(e);
		}

		return new VoteLogCounts(0, 0, 0);
	}

	public long getUniqueVoters(int days) {
		return getUniqueVoters(days, VoteLogEvent.VOTE_RECEIVED);
	}

        public long getUniqueVoters(int days, VoteLogEvent eventFilter) {
                boolean useCutoff = days > 0;
                long cutoff = useCutoff ? System.currentTimeMillis() - (days * 24L * 60L * 60L * 1000L) : 0;

                String sql = "SELECT COUNT(DISTINCT " + qi("player_uuid") + ") AS uniques FROM " + qi(getTableName())
                                + " WHERE 1=1 " + (eventFilter != null ? "AND " + qi("event") + "=? " : "")
                                + (useCutoff ? "AND " + qi("vote_time") + " >= ? " : "") + ";";

		try (Connection conn = mysql.getConnectionManager().getConnection();
				PreparedStatement ps = conn.prepareStatement(sql)) {

			int idx = 1;
			if (eventFilter != null) {
				ps.setString(idx++, eventFilter.name());
			}
			if (useCutoff) {
				ps.setLong(idx++, cutoff);
			}

			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getLong("uniques");
				}
			}
		} catch (SQLException e) {
			debug1(e);
		}
		return 0;
	}

	public List<ServiceCount> getTopServices(int days, int limit) {
		return getTopServices(days, limit, VoteLogEvent.VOTE_RECEIVED);
	}

	public List<ServiceCount> getTopServices(int days, int limit, VoteLogEvent eventFilter) {
                if (limit <= 0) {
                        limit = 10;
                }

                boolean useCutoff = days > 0;
                long cutoff = useCutoff ? System.currentTimeMillis() - (days * 24L * 60L * 60L * 1000L) : 0;

                String sql = "SELECT " + qi("service") + ", COUNT(*) AS votes FROM " + qi(getTableName()) + " WHERE 1=1 "
                                + (eventFilter != null ? "AND " + qi("event") + "=? " : "")
                                + (useCutoff ? "AND " + qi("vote_time") + " >= ? " : "") + "GROUP BY " + qi("service") + " "
                                + "ORDER BY votes DESC LIMIT ?;";

		try (Connection conn = mysql.getConnectionManager().getConnection();
				PreparedStatement ps = conn.prepareStatement(sql)) {

			int idx = 1;
			if (eventFilter != null) {
				ps.setString(idx++, eventFilter.name());
			}
                        if (useCutoff) {
                                ps.setLong(idx++, cutoff);
                        }
                        ps.setInt(idx, limit);

                        ResultSet rs = ps.executeQuery();
			List<ServiceCount> out = Collections.synchronizedList(new java.util.ArrayList<>());
			while (rs.next()) {
				out.add(new ServiceCount(rs.getString("service"), rs.getLong("votes")));
			}
			rs.close();
			return out;
		} catch (SQLException e) {
			debug1(e);
			return java.util.Collections.emptyList();
		}
	}

	private List<VoteLogEntry> query(String sql, Object[] params) {
		try (Connection conn = mysql.getConnectionManager().getConnection();
				PreparedStatement ps = conn.prepareStatement(sql)) {

			for (int i = 0; i < params.length; i++) {
				Object p = params[i];
				if (p instanceof Integer) {
					ps.setInt(i + 1, (Integer) p);
				} else if (p instanceof Long) {
					ps.setLong(i + 1, (Long) p);
				} else if (p instanceof String) {
					ps.setString(i + 1, (String) p);
				} else if (p instanceof Timestamp) {
					ps.setTimestamp(i + 1, (Timestamp) p);
				} else {
					ps.setObject(i + 1, p);
				}
			}

			ResultSet rs = ps.executeQuery();
			List<VoteLogEntry> out = Collections.synchronizedList(new java.util.ArrayList<>());

			while (rs.next()) {
				out.add(new VoteLogEntry(rs.getString("vote_id"), rs.getLong("vote_time"), rs.getString("player_uuid"),
						rs.getString("player_name"), rs.getString("service"), rs.getString("event"),
						rs.getString("context"), rs.getString("status"), rs.getInt("cached_total")));
			}
			rs.close();
			return out;
		} catch (SQLException e) {
			debug1(e);
			return java.util.Collections.emptyList();
		}
	}

	public static class VoteLogEntry {
		public final String voteId;
		public final long voteTime;
		public final String playerUuid;
		public final String playerName;
		public final String service;
		public final String event;
		public final String context;
		public final String status;
		public final int proxyCachedTotal;

		public VoteLogEntry(String voteId, long voteTime, String playerUuid, String playerName, String service,
				String event, String context, String status, int proxyCachedTotal) {
			this.voteId = voteId;
			this.voteTime = voteTime;
			this.playerUuid = playerUuid;
			this.playerName = playerName;
			this.service = service;
			this.event = event;
			this.context = context;
			this.status = status;
			this.proxyCachedTotal = proxyCachedTotal;
		}
	}

	public static class VoteLogCounts {
		public final long total;
		public final long immediate;
		public final long cached;

		public VoteLogCounts(long total, long immediate, long cached) {
			this.total = total;
			this.immediate = immediate;
			this.cached = cached;
		}
	}

	public static class ServiceCount {
		public final String service;
		public final long votes;

		public ServiceCount(String service, long votes) {
			this.service = service;
			this.votes = votes;
		}
	}
}
