package vm.java.io.costexplorer;

import java.time.LocalDate;
import java.util.List;

public interface SubscriptionService {
    void subscibe(long customerId, Subscription subscription, LocalDate startDate);

    void unsubscribe(long customerId, long productId);

    List<CustomerProductSubscription> getCustomerSubscriptions(Long customerId);
}
