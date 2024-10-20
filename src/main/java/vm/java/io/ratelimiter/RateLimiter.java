package vm.java.io.ratelimiter;

import java.util.Optional;

public interface RateLimiter {
    Optional<String> tryAcquire(Request request);
}
