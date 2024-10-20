package vm.java.io.costexplorer;

import java.time.LocalDate;

public class CustomerProductSubscription {
    private final long customerId;
    private final long productId;
    private final LocalDate startDate;
    private LocalDate endDate;
    private boolean enabled;
    private final Subscription subscription;

    public CustomerProductSubscription(long customerId, LocalDate startDate, Subscription subscription) {
        this.customerId = customerId;
        this.productId = subscription.getProductId();
        this.startDate = startDate;
        this.enabled = true;
        this.subscription = subscription;
    }

    public long getCustomerId() {
        return customerId;
    }

    public long getProductId() {
        return productId;
    }

    public LocalDate getStartDate() {
        return startDate;
    }

    public LocalDate getEndDate() {
        return endDate;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public int startMonth() {
        return startDate.getMonth().getValue();
    }

    public int startYear() {
        return this.startDate.getYear();
    }

    public int endMonth() {
        if (this.enabled) {
            return -1;
        }
        return endDate.getMonth().getValue();
    }

    public int endYear() {
        if (this.enabled) {
            return -1;
        }
        return this.endDate.getYear();
    }

    public void disable() {
        this.enabled = false;
        this.endDate = LocalDate.now();
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
