package vm.java.io.costexplorer.practice;

public class Subscription {
    private final long id;
    private final long productId;
    private double monthlyCost;
    private final SubscriptionType type;

    public Subscription(long id, long productId, double monthlyCost, SubscriptionType type) {
        this.id = id;
        this.productId = productId;
        this.monthlyCost = monthlyCost;
        this.type = type;
    }

    public long getId() {
        return id;
    }

    public long getProductId() {
        return productId;
    }

    public double getMonthlyCost() {
        return monthlyCost;
    }

    public double getYearlyCost() {
        return monthlyCost * 12;
    }

    public void setMonthlyCost(double monthlyCost) {
        this.monthlyCost = monthlyCost;
    }

    public SubscriptionType getType() {
        return type;
    }
}
