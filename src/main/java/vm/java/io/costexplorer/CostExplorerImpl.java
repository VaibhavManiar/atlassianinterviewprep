package vm.java.io.costexplorer;

import java.time.LocalDate;
import java.time.Year;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CostExplorerImpl implements CostExplorer {

    private final SubscriptionService subscriptionService;

    public CostExplorerImpl() {
        this.subscriptionService = new SubscriptionServiceImpl();
    }

    @Override
    public List<Double> monthlyCostList(long customerId) {
        LocalDate firstDayOfCurrentYear = LocalDate.of(Year.now().getValue(), 1, 1);
        List<CustomerProductSubscription> subscriptionToBeConsidered =
                this.subscriptionService.getCustomerSubscriptions(customerId).stream()
                        .filter(customerProductSubscription ->
                            // Either subscription is enabled
                            customerProductSubscription.isEnabled() ||

                            // OR end date is after first day of year
                            (!customerProductSubscription.isEnabled() &&
                                    (customerProductSubscription.getEndDate().isAfter(firstDayOfCurrentYear) ||
                                     customerProductSubscription.getEndDate().isEqual(firstDayOfCurrentYear))))
                        .toList();

        // If there is no subscriptions
        if(subscriptionToBeConsidered.isEmpty()) {
            return new ArrayList<>();
        }

        List<Double> monthlyCost = new ArrayList<>();
        for (int month = 1; month <= 12; month++) {
            int currMonth = month;
            double currMonthCost = subscriptionToBeConsidered.stream()
                    .filter(productSubscription ->
                        // If enabled subscription and start month of subscription is after or equals to current month
                        (productSubscription.isEnabled() && productSubscription.getStartDate().getMonth().getValue() >= currMonth) ||

                        // If disabled subscription and end month of subscription is after or equals to current month
                        (!productSubscription.isEnabled() && productSubscription.getEndDate().getMonth().getValue() >= currMonth)
                    )

                    .map(CustomerProductSubscription::getSubscription)
                    .collect(Collectors.summarizingDouble(Subscription::getMonthlyCost)).getSum();
            monthlyCost.add(currMonth - 1, currMonthCost);
        }
        return monthlyCost;
    }


    @Override
    public double annualCost(long customerId) {
        return this.monthlyCostList(customerId).stream().mapToDouble(val -> val).sum();
    }
}
