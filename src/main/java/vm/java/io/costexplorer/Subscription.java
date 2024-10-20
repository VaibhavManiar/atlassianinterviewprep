package vm.java.io.costexplorer;

public class Subscription {
    private final long productId;
    private final SubscriptionType type;
    private double monthlyCost;
    private double yearlyCostSubsidy;

    public Subscription(long productId, SubscriptionType type, double monthlyCost, double yearlyCostSubsidy) {
        this.productId = productId;
        this.type = type;
        this.monthlyCost = monthlyCost;
        this.yearlyCostSubsidy = yearlyCostSubsidy;
    }

    public long getProductId() {
        return productId;
    }

    public SubscriptionType getType() {
        return type;
    }

    public double getMonthlyCost() {
        return monthlyCost;
    }

    public double getYearlyCost() {
        return (monthlyCost * 12) - yearlyCostSubsidy;
    }

    public double getYearlyCostSubsidy() {
        return yearlyCostSubsidy;
    }

    public void setMonthlyCost(double monthlyCost) {
        this.monthlyCost = monthlyCost;
    }

    public void setYearlyCostSubsidy(double yearlyCostSubsidy) {
        this.yearlyCostSubsidy = yearlyCostSubsidy;
    }
}
