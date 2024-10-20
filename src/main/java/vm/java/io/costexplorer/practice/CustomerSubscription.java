package vm.java.io.costexplorer.practice;

import java.time.LocalDate;

public class CustomerSubscription {
    private final long customerId;
    private final Subscription subscription;
    private final LocalDate startDate;
    private boolean enabled;
    private LocalDate endDate;

    public CustomerSubscription(long customerId, Subscription subscription) {
        this.customerId = customerId;
        this.subscription = subscription;
        this.startDate = LocalDate.now();
        this.enabled = true;
        this.endDate = null;
    }

    public long getCustomerId() {
        return customerId;
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public LocalDate getStartDate() {
        return startDate;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void disable() {
        this.enabled = false;
        this.endDate = LocalDate.now();
    }

    public LocalDate getEndDate() {
        return endDate;
    }

    public long getProductId() {
        return this.subscription.getProductId();
    }

    public double getMonthlyCost() {
        return this.subscription.getMonthlyCost();
    }
}
