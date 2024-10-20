package vm.java.io.ratelimiter;

import java.util.Optional;

public class RateLimiters implements RateLimiter {

    private final FixedWindowRateLimiter fixedWindowRateLimiter;
    private final SlidingWindowRateLimiter slidingWindowRateLimiter;

    public RateLimiters(long windowSizeInMillis, long bucketSize) {

        if(windowSizeInMillis <= 0 || bucketSize <= 0) {
            throw new IllegalArgumentException("Rate Limiter can not be created with zero window size or bucket size.");
        }
        fixedWindowRateLimiter = new FixedWindowRateLimiter(windowSizeInMillis, bucketSize);
        slidingWindowRateLimiter = new SlidingWindowRateLimiter(windowSizeInMillis, bucketSize);
    }

    @Override
    public Optional<String> tryAcquire(Request request) {
        if(RateLimiterType.FIXED_WINDOW.equals(request.getRateLimiterType())) {
            fixedWindowRateLimiter.tryAcquire(request);
        } else if (RateLimiterType.SLIDING_WINDOW.equals(request.getRateLimiterType())){
            slidingWindowRateLimiter.tryAcquire(request);
        }
        return Optional.empty();
    }
}
