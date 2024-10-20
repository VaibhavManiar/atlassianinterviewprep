package vm.java.io.costexplorer;

import java.util.List;

public interface CostExplorer {
    List<Double> monthlyCostList(long customerId);
    double annualCost(long customerId);
}
