package vm.java.io.costexplorer;

public class MonthlySubscriptionCost {
    private final long customerId;
    private final long productId;
    private final int month;
    private final double cost;

    public MonthlySubscriptionCost(long customerId, long productId, int month, double cost) {
        this.customerId = customerId;
        this.productId = productId;
        this.month = month;
        this.cost = cost;
    }

    public long getCustomerId() {
        return customerId;
    }

    public long getProductId() {
        return productId;
    }

    public int getMonth() {
        return month;
    }

    public double getCost() {
        return cost;
    }
}
