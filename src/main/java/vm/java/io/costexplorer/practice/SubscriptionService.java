package vm.java.io.costexplorer.practice;

import java.util.List;

public interface SubscriptionService {
    void subscribe(long customerId, Subscription subscription);
    List<CustomerSubscription> getCustomerSubscriptions(long customerId);
}
