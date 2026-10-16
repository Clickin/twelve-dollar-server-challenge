package challenge;

import org.sqlite.SQLiteConfig;
import java.sql.*;

/** One sqlite-jdbc connection with every statement prepared up front; confined to one thread. */
final class SqliteJdbc implements Database.Backend {
    private final Connection connection;
    private final PreparedStatement health, feed, find, create, exists, like, begin, beginRead, commit, rollback;
    private final byte[][] rows = new byte[20][];

    SqliteJdbc(String path, String feedSql, String findSql, String likeSql) throws SQLException {
        SQLiteConfig sqlite = new SQLiteConfig();
        sqlite.setSynchronous(SQLiteConfig.SynchronousMode.NORMAL);
        sqlite.setBusyTimeout(1000);
        sqlite.enforceForeignKeys(true);
        sqlite.setCacheSize(-8192);
        sqlite.setPragma(SQLiteConfig.Pragma.MMAP_SIZE, "268435456");
        // Lab toggle: the connection is confined to one thread, so SQLite's own mutexes are redundant.
        if (Boolean.getBoolean("sqlite.nomutex")) sqlite.setOpenMode(org.sqlite.SQLiteOpenMode.NOMUTEX);
        connection = DriverManager.getConnection("jdbc:sqlite:" + path, sqlite.toProperties());
        try {
            try (var statement = connection.createStatement(); var rs = statement.executeQuery("PRAGMA journal_mode=WAL")) {
                if (!rs.next() || !"wal".equalsIgnoreCase(rs.getString(1))) throw new SQLException("WAL required");
            }
            health = connection.prepareStatement("SELECT 1");
            feed = connection.prepareStatement(feedSql);
            find = connection.prepareStatement(findSql);
            create = connection.prepareStatement("INSERT INTO posts(user_id,body) VALUES (?,?) RETURNING id,created_at");
            exists = connection.prepareStatement("SELECT 1 FROM posts WHERE id=?");
            like = connection.prepareStatement(likeSql);
            begin = connection.prepareStatement("BEGIN IMMEDIATE");
            beginRead = connection.prepareStatement("BEGIN");
            commit = connection.prepareStatement("COMMIT");
            rollback = connection.prepareStatement("ROLLBACK");
        } catch (SQLException e) { connection.close(); throw e; }
    }

    @Override public boolean health() {
        try (var rs = health.executeQuery()) { return rs.next() && rs.getInt(1) == 1; }
        catch (SQLException e) { throw new IllegalStateException(e); }
    }
    @Override public byte[] feed() {
        if (Database.FEED_ROWS) return feedRows();
        try (var rs = feed.executeQuery()) {
            if (!rs.next()) throw new SQLException("Feed returned no row");
            return rs.getBytes(1);
        } catch (SQLException e) { throw new IllegalStateException(e); }
    }
    private byte[] feedRows() {
        int count = 0, length = Database.FEED_OPEN.length + 2;
        try (var rs = feed.executeQuery()) {
            while (rs.next()) {
                byte[] row = rs.getBytes(1);
                rows[count++] = row;
                length += row.length;
            }
        } catch (SQLException e) { throw new IllegalStateException(e); }
        byte[] out = new byte[length + Math.max(count - 1, 0)];
        System.arraycopy(Database.FEED_OPEN, 0, out, 0, Database.FEED_OPEN.length);
        int at = Database.FEED_OPEN.length;
        for (int i = 0; i < count; i++) {
            if (i > 0) out[at++] = ',';
            System.arraycopy(rows[i], 0, out, at, rows[i].length);
            at += rows[i].length;
            rows[i] = null;
        }
        out[at++] = ']'; out[at] = '}';
        return out;
    }
    @Override public byte[] find(long id) {
        try {
            find.setLong(1, id);
            try (var rs = find.executeQuery()) {
                if (!rs.next()) throw new ApiException(404, "post not found");
                return rs.getBytes(1);
            }
        } catch (SQLException e) { throw new IllegalStateException(e); }
    }
    @Override public Responses.Post create(JwtVerifier.User user, String body) {
        try {
            create.setLong(1, user.id()); create.setString(2, body);
            try (var rs = create.executeQuery()) {
                if (!rs.next()) throw new SQLException("INSERT returned no row");
                var post = new Responses.Post(rs.getLong(1), body, rs.getString(2), user.username(), 0);
                if (rs.next()) throw new SQLException("INSERT returned multiple rows");
                return post;
            }
        } catch (SQLException e) { throw new IllegalStateException(e); }
    }
    @Override public boolean like(long user, long post) {
        try {
            like.setLong(1, user); like.setLong(2, post);
            if (like.executeUpdate() != 0) return false;
            exists.setLong(1, post);
            try (var rs = exists.executeQuery()) {
                if (!rs.next()) throw new ApiException(404, "post not found");
            }
            return true;
        } catch (SQLException e) { throw new IllegalStateException(e); }
    }
    @Override public void begin(boolean write) throws SQLException { (write ? begin : beginRead).executeUpdate(); }
    @Override public void commit() throws SQLException { commit.executeUpdate(); }
    @Override public void rollback() throws SQLException { rollback.executeUpdate(); }
    @Override public void close() throws SQLException { connection.close(); }
}
