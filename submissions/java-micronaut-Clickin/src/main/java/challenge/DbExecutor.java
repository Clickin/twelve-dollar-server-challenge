package challenge;

import jakarta.inject.Singleton;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.function.Supplier;

@Singleton
public final class DbExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(DbExecutor.class);
    static final int CAPACITY = 4096, BATCH_SIZE = 256;
    // Lab toggle: read-only batches of several requests share one deferred read transaction.
    static final boolean READ_TX = Boolean.getBoolean("db.readtx");
    private static final Task<Void> STOP = new Task<>(false, () -> null);
    private final ArrayBlockingQueue<Task<?>> queue = new ArrayBlockingQueue<>(CAPACITY);
    private final Object admission = new Object();
    private boolean accepting;
    private Thread worker;
    private Transactions tx;

    /** Transaction control on the connection owned by the worker thread. */
    interface Transactions {
        void begin(boolean write) throws Exception;
        void commit() throws Exception;
        void rollback() throws Exception;
    }

    void start(Connection connection) throws SQLException {
        PreparedStatement begin = connection.prepareStatement("BEGIN IMMEDIATE"), beginRead = connection.prepareStatement("BEGIN"),
            commit = connection.prepareStatement("COMMIT"), rollback = connection.prepareStatement("ROLLBACK");
        start(new Transactions() {
            public void begin(boolean write) throws SQLException { (write ? begin : beginRead).executeUpdate(); }
            public void commit() throws SQLException { commit.executeUpdate(); }
            public void rollback() throws SQLException { rollback.executeUpdate(); }
        });
    }

    void start(Transactions transactions) {
        tx = transactions;
        synchronized (admission) {
            accepting = true;
            worker = Thread.ofPlatform().name("db-owner").start(this::run);
        }
    }

    private static final class Task<T> {
        final boolean write;
        final Supplier<T> action;
        final CompletableFuture<T> future = new CompletableFuture<>();
        T value;
        ApiException error;
        Task(boolean write, Supplier<T> action) { this.write = write; this.action = action; }
        void execute() {
            try { value = action.get(); }
            catch (ApiException e) {
                if (e.status >= 500) throw e;
                error = e;
            }
        }
        void complete() {
            if (error == null) future.complete(value);
            else future.completeExceptionally(error);
        }
    }

    public <T> CompletableFuture<T> read(Supplier<T> action) { return submit(false, action); }
    public <T> CompletableFuture<T> write(Supplier<T> action) { return submit(true, action); }
    private <T> CompletableFuture<T> submit(boolean write, Supplier<T> action) {
        var task = new Task<>(write, action);
        synchronized (admission) {
            if (!accepting || !queue.offer(task))
                task.future.completeExceptionally(new ApiException(503, "internal server error"));
        }
        return task.future;
    }

    private void run() {
        var batch = new ArrayList<Task<?>>(BATCH_SIZE);
        try {
            boolean stopping = false;
            while (!stopping) {
                batch.add(queue.take());
                // Only collect work already waiting: no timer or minimum batch size.
                queue.drainTo(batch, BATCH_SIZE - 1);
                if (batch.getLast() == STOP) {
                    batch.removeLast();
                    stopping = true;
                }
                boolean writes = false;
                for (var task : batch) writes |= task.write;
                boolean grouped = writes || (READ_TX && batch.size() > 1);
                boolean transaction = false;
                boolean usable = true;
                try {
                    if (grouped) { tx.begin(writes); transaction = true; }
                    for (var task : batch) task.execute();
                    if (grouped) { tx.commit(); transaction = false; }
                    // Reads in this batch may have seen its writes; publish neither before COMMIT.
                    for (var task : batch) task.complete();
                } catch (Throwable failure) {
                    if (transaction) {
                        try { tx.rollback(); }
                        catch (Exception uncertain) {
                            failure.addSuppressed(uncertain);
                            usable = false;
                        }
                    }
                    LOG.error("SQLite batch failed", failure);
                    for (var task : batch)
                        task.future.completeExceptionally(new ApiException(500, "internal server error"));
                    if (!usable || failure instanceof Error) break;
                }
                batch.clear();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            synchronized (admission) { accepting = false; }
            Task<?> task;
            while ((task = queue.poll()) != null)
                task.future.completeExceptionally(new ApiException(503, "internal server error"));
        }
    }

    @PreDestroy public void close() {
        boolean interrupted = false;
        boolean stop;
        synchronized (admission) {
            stop = accepting;
            accepting = false;
        }
        if (stop) {
            while (true) {
                try { queue.put(STOP); break; }
                catch (InterruptedException e) { interrupted = true; }
            }
        }
        if (worker != null) {
            while (worker.isAlive()) {
                try { worker.join(); }
                catch (InterruptedException e) { interrupted = true; }
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }
}
