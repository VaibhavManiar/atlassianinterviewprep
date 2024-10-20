package vm.java.io.ratelimiter;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class FixedWindowRateLimiter implements RateLimiter {

    private final long windowSizeInMillis;
    private final long bucketSize;
    private final Map<Long, Bucket> clientBuckets;
    private final ReentrantLock refreshLock;

    public FixedWindowRateLimiter(long windowSizeInMillis, long bucketSize) {

        if(windowSizeInMillis <= 0 || bucketSize <= 0) {
            throw new IllegalArgumentException("Rate Limiter can not be created with zero window size or bucket size.");
        }

        this.clientBuckets = new ConcurrentHashMap<>();
        this.windowSizeInMillis = windowSizeInMillis;
        this.bucketSize = bucketSize;
        this.refreshLock = new ReentrantLock(true);
    }

    @Override
    public Optional<String> tryAcquire(Request request) {
        Bucket bucket = clientBuckets.computeIfAbsent(request.getClientId(), k -> new Bucket(bucketSize));
        this.refresh(bucket);
        return bucket.getToken();
    }


    private void refresh(Bucket bucket) {
        long tokensToAdd = getTokensToAdd(bucket);
        System.out.println("Tokens To Be Added: " + tokensToAdd);
        if (tokensToAdd > 0 && refreshLock.tryLock()) {
            try {
                bucket.updateTokens(tokensToAdd);
            } finally {
                this.refreshLock.unlock();
            }
        }
    }

    /**
     * Return number of tokens to be added in sliding window.
     * @return
     */
    private long getTokensToAdd(Bucket bucket) {
        long now = System.currentTimeMillis();
        long timeDiff = now - bucket.getLastUpdatedTs();
        return windowSizeInMillis - timeDiff <= 0 ? bucket.getMaxSize() : 0;
    }

    public long getWindowSizeInMillis() {
        return windowSizeInMillis;
    }

    public long getBucketSize() {
        return bucketSize;
    }
}
