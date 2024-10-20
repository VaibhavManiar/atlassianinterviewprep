package vm.java.io.costexplorer.practice;

import java.time.LocalDate;
import java.time.Year;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CostExplorerImpl implements CostExplorer {

    private final SubscriptionService subscriptionService;
    private final Map<Long,List<Double>> customerMonthlyExpense;

    public CostExplorerImpl() {
        this.subscriptionService = new SubscriptionServiceImpl();
        this.customerMonthlyExpense = new ConcurrentHashMap<>();
    }

    @Override
    public List<Double> getMonthlySubscriptionCost(long customerId) {
        LocalDate firstDayOfCurrentYear = LocalDate.of(Year.now().getValue(), 1, 1);

        List<CustomerSubscription> toBeConsideredSubscriptions =
                this.subscriptionService.getCustomerSubscriptions(customerId).stream()
                        .filter(customerSubscription -> customerSubscription.isEnabled() ||
                                (customerSubscription.getEndDate().isAfter(firstDayOfCurrentYear.minusDays(1))))
                        .toList();

        List<Double> monthlyCost = new ArrayList<>();
        for (int inx = 0; inx < 12; inx++) {
            int currMonth = inx + 1;
            monthlyCost.add(toBeConsideredSubscriptions.stream().filter(customerSubscription ->
                            (customerSubscription.isEnabled() && customerSubscription.getStartDate().getMonth().getValue() >= currMonth) ||
                            (customerSubscription.getEndDate().getMonth().getValue() >= currMonth))
                    .map(CustomerSubscription::getMonthlyCost)
                    .mapToDouble(val -> val).sum());
        }
        return monthlyCost;
    }

    @Override
    public Double getYearlySubscriptionCost(long customerId) {
        return this.getMonthlySubscriptionCost(customerId).stream().mapToDouble(val -> val).sum();
    }
}
