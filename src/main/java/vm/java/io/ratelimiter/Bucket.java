package vm.java.io.ratelimiter;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Bucket {
    private final AtomicLong tokens;
    private final ReentrantLock getTokenLock;
    private final long maxSize;
    protected long lastUpdatedTs;

    public Bucket(long maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("Bucket can not be created with zero max size.");
        }
        this.maxSize = maxSize;
        this.tokens = new AtomicLong(this.maxSize);
        this.getTokenLock = new ReentrantLock(true);
        this.lastUpdatedTs = System.currentTimeMillis();
    }

    public Optional<String> getToken() {
        boolean lockAcquired = false;
        try {
            if (tokens.get() > 0) {
                lockAcquired = getTokenLock.tryLock(10, TimeUnit.SECONDS);
                if (lockAcquired && tokens.get() > 0) {
                    tokens.decrementAndGet();
                    return Optional.of(UUID.randomUUID().toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (lockAcquired) {
                getTokenLock.unlock();
            }
        }
        return Optional.empty();
    }

    protected long getLastUpdatedTs() {
        return lastUpdatedTs;
    }

    protected AtomicLong getTokens() {
        return tokens;
    }

    protected void updateTokens(long count) {
        this.tokens.set(Math.min(maxSize, tokens.get() + count));
        this.lastUpdatedTs = System.currentTimeMillis();
    }

    protected long getMaxSize() {
        return this.maxSize;
    }
}
