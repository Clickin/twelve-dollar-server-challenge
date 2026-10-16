package challenge;

import io.micronaut.context.annotation.Context;
import jakarta.annotation.PreDestroy;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Context
public final class Database {
    static final String FEED_JSON = """
        SELECT CAST('{"posts":[' || coalesce(group_concat(post ORDER BY created_at DESC,id DESC), '') || ']}' AS BLOB) FROM (
            SELECT json_object('id',p.id,'body',p.body,'created_at',p.created_at,'author',u.username,
                'like_count',(SELECT count(*) FROM likes l WHERE l.post_id=p.id)) AS post,p.created_at,p.id
            FROM posts p JOIN users u ON u.id=p.user_id ORDER BY p.created_at DESC,p.id DESC LIMIT 20
        )
        """;
    static final String POST_JSON = """
        SELECT CAST(json_object('post',json_object('id',p.id,'body',p.body,'created_at',p.created_at,
            'author',u.username,'like_count',(SELECT count(*) FROM likes l WHERE l.post_id=p.id))) AS BLOB)
        FROM posts p JOIN users u ON u.id=p.user_id WHERE p.id=?
        """;
    // Lab toggle: one JSON object per row in index order, framed in Java, instead of an ORDER BY aggregate.
    static final boolean FEED_ROWS = Boolean.getBoolean("feed.rows");
    static final String FEED_ROWS_JSON = """
        SELECT CAST(json_object('id',p.id,'body',p.body,'created_at',p.created_at,'author',u.username,
            'like_count',(SELECT count(*) FROM likes l WHERE l.post_id=p.id)) AS BLOB)
        FROM posts p JOIN users u ON u.id=p.user_id ORDER BY p.created_at DESC,p.id DESC LIMIT 20
        """;
    static final byte[] FEED_OPEN = "{\"posts\":[".getBytes(StandardCharsets.US_ASCII);
    static final String LIKE_SQL = """
        INSERT INTO likes(user_id,post_id) SELECT ?,id FROM posts WHERE id=?
        ON CONFLICT(user_id,post_id) DO NOTHING
        """;
    // Lab toggles: -Dsqlite.driver=ffm calls a separately built libsqlite3 (-Dsqlite.ffm.lib) through FFM;
    // -Dread.inline=true serves GET /feed and /posts/{id} on the calling event loop from its own read connection.
    static final boolean FFM = "ffm".equals(System.getProperty("sqlite.driver"));
    static final boolean READ_INLINE = Boolean.getBoolean("read.inline");

    /** One SQLite connection and its prepared statements, used by a single thread. */
    interface Backend extends DbExecutor.Transactions, AutoCloseable {
        boolean health();
        byte[] feed();
        byte[] find(long id);
        Responses.Post create(JwtVerifier.User user, String body);
        boolean like(long user, long post);
    }

    private final DbExecutor executor;
    private final Path path;
    private final Backend writer;
    private final List<Backend> readers = new ArrayList<>();
    private final ThreadLocal<Backend> reader = ThreadLocal.withInitial(this::openReader);

    public Database(DbExecutor executor) throws Exception {
        this.executor = executor;
        path = Path.of(System.getenv().getOrDefault("SQLITE_PATH", "feed.db")).toAbsolutePath();
        if (!Files.isRegularFile(path)) throw new IllegalArgumentException("SQLITE_PATH must point to an existing seed copy: " + path);
        writer = open();
        if (FFM) LoggerFactory.getLogger(Database.class).info("SQLite {} through FFM", SqliteFfm.version());
        executor.start(writer);
    }
    private Backend open() throws Exception {
        String feedSql = FEED_ROWS && !FFM ? FEED_ROWS_JSON : FEED_JSON;
        return FFM ? new SqliteFfm(path.toString(), FEED_JSON, POST_JSON, LIKE_SQL) : new SqliteJdbc(path.toString(), feedSql, POST_JSON, LIKE_SQL);
    }
    private Backend openReader() {
        try {
            Backend backend = open();
            synchronized (readers) { readers.add(backend); }
            return backend;
        } catch (Exception e) { throw new IllegalStateException(e); }
    }

    // The FIFO owner calls these methods. Its futures publish results after the batch commits.
    public boolean health() { return writer.health(); }
    public byte[] feed() { return writer.feed(); }
    public byte[] find(long id) { return writer.find(id); }
    public Responses.Post create(JwtVerifier.User user, String body) { return writer.create(user, body); }
    public boolean like(long user, long post) { return writer.like(user, post); }

    // Inline reads: each autocommit statement sees only committed data on this thread's own connection.
    public byte[] feedInline() { return reader.get().feed(); }
    public byte[] findInline(long id) { return reader.get().find(id); }

    @PreDestroy public void close() throws Exception {
        executor.close();
        synchronized (readers) { for (Backend backend : readers) backend.close(); }
        writer.close();
    }
}
