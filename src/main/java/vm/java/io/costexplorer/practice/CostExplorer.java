package vm.java.io.costexplorer.practice;

import java.util.List;

public interface CostExplorer {
    List<Double> getMonthlySubscriptionCost(long customerId);

    Double getYearlySubscriptionCost(long customerId);
}
