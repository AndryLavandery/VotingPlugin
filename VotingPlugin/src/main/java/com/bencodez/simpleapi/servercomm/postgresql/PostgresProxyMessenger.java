package com.bencodez.simpleapi.servercomm.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.sql.DataSource;

public class PostgresProxyMessenger {
	private final String tableName;
	private final DataSource dataSource;
	private final Consumer<PostgresBackendMessenger.BackendMessage> onMessage;
	private final ExecutorService listener = Executors.newSingleThreadExecutor();

	private volatile boolean running = true;
	private long lastSeenId = 0;

	public PostgresProxyMessenger(String tableName, DataSource dataSource,
			Consumer<PostgresBackendMessenger.BackendMessage> onMessage) throws SQLException {
		this.tableName = tableName;
		this.dataSource = dataSource;
		this.onMessage = onMessage;
		ensureSchema();
		startListener();
	}

	private void ensureSchema() throws SQLException {
		String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + "_message_queue ("
				+ "id BIGSERIAL PRIMARY KEY, " + "source VARCHAR(36) NOT NULL, "
				+ "destination VARCHAR(36) NOT NULL, " + "created_at TIMESTAMP NOT NULL DEFAULT NOW(), "
				+ "payload TEXT NOT NULL)";
		String idx = "CREATE INDEX IF NOT EXISTS " + tableName + "_message_dest_idx ON " + tableName
				+ "_message_queue (destination, id)";
		try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
			stmt.execute(ddl);
			stmt.execute(idx);
		}
	}

	private void startListener() {
		listener.submit(() -> {
			while (running) {
				try {
					fetchBatch().forEach(onMessage);
					Thread.sleep(200);
				} catch (SQLException e) {
					e.printStackTrace();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException ignored) {
						Thread.currentThread().interrupt();
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					running = false;
				}
			}
		});
	}

	private List<PostgresBackendMessenger.BackendMessage> fetchBatch() throws SQLException {
		String sql = "SELECT id, source, payload FROM " + tableName
				+ "_message_queue WHERE destination = 'proxy' AND id > ? ORDER BY id";
		List<PostgresBackendMessenger.BackendMessage> results = new ArrayList<>();
		try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setLong(1, lastSeenId);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					long id = rs.getLong("id");
					String from = rs.getString("source");
					String payload = rs.getString("payload");
					results.add(new PostgresBackendMessenger.BackendMessage(id, from, payload));
					lastSeenId = id;
				}
			}
		}

		for (PostgresBackendMessenger.BackendMessage msg : results) {
			deleteMessageById(msg.id);
		}
		return results;
	}

	private void deleteMessageById(long id) throws SQLException {
		String delSql = "DELETE FROM " + tableName + "_message_queue WHERE id = ?";
		try (Connection conn = dataSource.getConnection(); PreparedStatement del = conn.prepareStatement(delSql)) {
			del.setLong(1, id);
			del.executeUpdate();
		}
	}

	public void sendToBackend(String destination, String payload) throws SQLException {
		String insertSql = "INSERT INTO " + tableName
				+ "_message_queue (source, destination, payload) VALUES ('proxy', ?, ?)";
		try (Connection conn = dataSource.getConnection(); PreparedStatement ins = conn.prepareStatement(insertSql)) {
			ins.setString(1, destination);
			ins.setString(2, payload);
			ins.executeUpdate();
		}
	}

	public void shutdown() {
		running = false;
		listener.shutdownNow();
		try {
			listener.awaitTermination(2, TimeUnit.SECONDS);
		} catch (InterruptedException ignored) {
			Thread.currentThread().interrupt();
		}
	}
}
