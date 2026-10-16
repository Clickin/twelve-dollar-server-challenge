package challenge;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;

/**
 * Lab comparison: the same statements and transaction ownership as the JDBC path, but calling a
 * separately built libsqlite3 through FFM. Accessors that never block use critical downcalls;
 * {@code sqlite3_step} does not, because COMMIT and checkpoints can block on I/O.
 */
final class SqliteFfm implements Database.Backend {
    private static final int OK = 0, ROW = 100, DONE = 101;
    private static final int OPEN_READWRITE = 0x2, OPEN_NOMUTEX = 0x8000, OPEN_EXRESCODE = 0x02000000, PREPARE_PERSISTENT = 0x1;
    private static final MemorySegment TRANSIENT = MemorySegment.ofAddress(-1L);
    private static final Linker LINKER = Linker.nativeLinker();
    private static final Linker.Option CRITICAL = Linker.Option.critical(false);
    private static final SymbolLookup LIB = SymbolLookup.libraryLookup(
        java.nio.file.Path.of(System.getProperty("sqlite.ffm.lib", "libsqlite3.so")), Arena.global());

    private static MethodHandle fn(String name, FunctionDescriptor descriptor, Linker.Option... options) {
        return LINKER.downcallHandle(LIB.findOrThrow(name), descriptor, options);
    }
    private static final MethodHandle libversion = fn("sqlite3_libversion", FunctionDescriptor.of(ADDRESS));
    private static final MethodHandle open = fn("sqlite3_open_v2", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT, ADDRESS));
    private static final MethodHandle exec = fn("sqlite3_exec", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS));
    private static final MethodHandle errmsg = fn("sqlite3_errmsg", FunctionDescriptor.of(ADDRESS, ADDRESS));
    private static final MethodHandle prepare = fn("sqlite3_prepare_v3", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS, ADDRESS));
    private static final MethodHandle step = fn("sqlite3_step", FunctionDescriptor.of(JAVA_INT, ADDRESS));
    private static final MethodHandle reset = fn("sqlite3_reset", FunctionDescriptor.of(JAVA_INT, ADDRESS), CRITICAL);
    private static final MethodHandle bindInt64 = fn("sqlite3_bind_int64", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_LONG), CRITICAL);
    private static final MethodHandle bindText = fn("sqlite3_bind_text", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS), CRITICAL);
    private static final MethodHandle columnInt64 = fn("sqlite3_column_int64", FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_INT), CRITICAL);
    private static final MethodHandle columnBlob = fn("sqlite3_column_blob", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_INT), CRITICAL);
    private static final MethodHandle columnText = fn("sqlite3_column_text", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_INT), CRITICAL);
    private static final MethodHandle columnBytes = fn("sqlite3_column_bytes", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT), CRITICAL);
    private static final MethodHandle changes = fn("sqlite3_changes", FunctionDescriptor.of(JAVA_INT, ADDRESS), CRITICAL);

    private final MemorySegment db;
    private final MemorySegment health, feed, find, create, exists, like, begin, beginRead, commit, rollback;
    private MemorySegment text = Arena.global().allocate(2048);

    SqliteFfm(String path, String feedSql, String findSql, String likeSql) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ADDRESS);
            check((int) open.invokeExact(arena.allocateFrom(path), out, OPEN_READWRITE | OPEN_NOMUTEX | OPEN_EXRESCODE, MemorySegment.NULL), null);
            db = out.get(ADDRESS, 0);
            for (String pragma : new String[] {"PRAGMA synchronous=NORMAL", "PRAGMA busy_timeout=1000", "PRAGMA foreign_keys=ON",
                    "PRAGMA cache_size=-8192", "PRAGMA mmap_size=268435456"})
                check((int) exec.invokeExact(db, arena.allocateFrom(pragma), MemorySegment.NULL, MemorySegment.NULL, MemorySegment.NULL));
            MemorySegment wal = prepare(arena, "PRAGMA journal_mode=WAL");
            boolean walMode = (int) step.invokeExact(wal) == ROW && "wal".equalsIgnoreCase(text(wal, 0));
            int finalized = (int) finalizeStatement.invokeExact(wal);
            if (!walMode) throw new IllegalStateException("WAL required");
            health = prepare(arena, "SELECT 1");
            feed = prepare(arena, feedSql);
            find = prepare(arena, findSql);
            create = prepare(arena, "INSERT INTO posts(user_id,body) VALUES (?,?) RETURNING id,created_at");
            exists = prepare(arena, "SELECT 1 FROM posts WHERE id=?");
            like = prepare(arena, likeSql);
            begin = prepare(arena, "BEGIN IMMEDIATE");
            beginRead = prepare(arena, "BEGIN");
            commit = prepare(arena, "COMMIT");
            rollback = prepare(arena, "ROLLBACK");
        } catch (Throwable t) { throw rethrow(t); }
    }

    static String version() {
        try { return ((MemorySegment) libversion.invokeExact()).reinterpret(64).getString(0); }
        catch (Throwable t) { throw rethrow(t); }
    }

    @Override public boolean health() {
        try { return step(health) == ROW; } finally { reset(health); }
    }
    @Override public byte[] feed() {
        try {
            if (step(feed) != ROW) throw new IllegalStateException("Feed returned no row");
            return blob(feed, 0);
        } finally { reset(feed); }
    }
    @Override public byte[] find(long id) {
        try {
            bind(find, 1, id);
            int rc = step(find);
            if (rc == DONE) throw new ApiException(404, "post not found");
            check(rc, ROW);
            return blob(find, 0);
        } finally { reset(find); }
    }
    @Override public Responses.Post create(JwtVerifier.User user, String body) {
        try {
            bind(create, 1, user.id());
            byte[] utf8 = body.getBytes(StandardCharsets.UTF_8);
            if (text.byteSize() < utf8.length) text = Arena.global().allocate(utf8.length);
            MemorySegment.copy(utf8, 0, text, JAVA_BYTE, 0, utf8.length);
            check((int) bindText.invokeExact(create, 2, text, utf8.length, TRANSIENT));
            check(step(create), ROW);
            var post = new Responses.Post((long) columnInt64.invokeExact(create, 0), body, text(create, 1), user.username(), 0);
            check(step(create), DONE);
            return post;
        } catch (Throwable t) { throw rethrow(t); }
        finally { reset(create); }
    }
    @Override public boolean like(long user, long post) {
        try {
            bind(like, 1, user); bind(like, 2, post);
            check(step(like), DONE);
            if ((int) changes.invokeExact(db) != 0) return false;
        } catch (Throwable t) { throw rethrow(t); }
        finally { reset(like); }
        try {
            bind(exists, 1, post);
            int rc = step(exists);
            if (rc == DONE) throw new ApiException(404, "post not found");
            check(rc, ROW);
            return true;
        } finally { reset(exists); }
    }

    @Override public void begin(boolean write) { run(write ? begin : beginRead); }
    @Override public void commit() { run(commit); }
    @Override public void rollback() { run(rollback); }

    private static final MethodHandle closeV2 = fn("sqlite3_close_v2", FunctionDescriptor.of(JAVA_INT, ADDRESS));
    private static final MethodHandle finalizeStatement = fn("sqlite3_finalize", FunctionDescriptor.of(JAVA_INT, ADDRESS));
    @Override public void close() {
        try {
            for (MemorySegment statement : new MemorySegment[] {health, feed, find, create, exists, like, begin, beginRead, commit, rollback})
                if (statement != null) { int ignored = (int) finalizeStatement.invokeExact(statement); }
            int ignored = (int) closeV2.invokeExact(db);
        } catch (Throwable t) { throw rethrow(t); }
    }

    private void run(MemorySegment statement) {
        try { check(step(statement), DONE); } finally { reset(statement); }
    }
    private MemorySegment prepare(Arena arena, String sql) throws Throwable {
        MemorySegment out = arena.allocate(ADDRESS), source = arena.allocateFrom(sql);
        check((int) prepare.invokeExact(db, source, (int) source.byteSize(), PREPARE_PERSISTENT, out, MemorySegment.NULL));
        return out.get(ADDRESS, 0);
    }
    private int step(MemorySegment statement) {
        try { return (int) step.invokeExact(statement); } catch (Throwable t) { throw rethrow(t); }
    }
    private static void reset(MemorySegment statement) {
        try { int ignored = (int) reset.invokeExact(statement); } catch (Throwable t) { throw rethrow(t); }
    }
    private void bind(MemorySegment statement, int index, long value) {
        try { check((int) bindInt64.invokeExact(statement, index, value)); } catch (Throwable t) { throw rethrow(t); }
    }
    private static byte[] blob(MemorySegment statement, int column) {
        try {
            MemorySegment data = (MemorySegment) columnBlob.invokeExact(statement, column);
            int length = (int) columnBytes.invokeExact(statement, column);
            return data.reinterpret(length).toArray(JAVA_BYTE);
        } catch (Throwable t) { throw rethrow(t); }
    }
    private static String text(MemorySegment statement, int column) throws Throwable {
        MemorySegment data = (MemorySegment) columnText.invokeExact(statement, column);
        int length = (int) columnBytes.invokeExact(statement, column);
        return new String(data.reinterpret(length).toArray(JAVA_BYTE), StandardCharsets.UTF_8);
    }
    private void check(int rc) { check(rc, OK); }
    private void check(int rc, int expected) {
        if (rc == expected) return;
        String message;
        try { message = db == null ? "open failed" : ((MemorySegment) errmsg.invokeExact(db)).reinterpret(1024).getString(0); }
        catch (Throwable t) { message = "unknown"; }
        throw new IllegalStateException("SQLite error " + rc + ": " + message);
    }
    private static void check(int rc, Object ignored) { if (rc != OK) throw new IllegalStateException("sqlite3_open_v2 failed: " + rc); }
    private static RuntimeException rethrow(Throwable t) {
        if (t instanceof RuntimeException r) return r;
        if (t instanceof Error e) throw e;
        return new IllegalStateException(t);
    }
}
