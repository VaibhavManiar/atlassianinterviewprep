package vm.java.io.ratelimiter;

public class Request {
    private final long clientId;
    private final long id;
    private final RateLimiterType rateLimiterType;

    public Request(long clientId, long id, RateLimiterType type) {
        if(clientId <= 0 || id <= 0) {
            throw new IllegalArgumentException("Client Id and Request Id can not be less than equals to zero");
        }
        this.clientId = clientId;
        this.id = id;
        this.rateLimiterType = type;
    }

    public Request(long clientId, long id) {
        if(clientId <= 0 || id <= 0) {
            throw new IllegalArgumentException("Client Id and Request Id can not be less than equals to zero");
        }
        this.clientId = clientId;
        this.id = id;
        this.rateLimiterType = RateLimiterType.FIXED_WINDOW;
    }

    public long getClientId() {
        return clientId;
    }

    public long getId() {
        return id;
    }

    public RateLimiterType getRateLimiterType() {
        return rateLimiterType;
    }
}
