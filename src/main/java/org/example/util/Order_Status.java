package org.example.util;

public class Order_Status {

    public static status.customer.email.Order_Status toCustomerStatus( org.order.status.Order_Status sourceStatus) {
        switch (sourceStatus) {
            case SUBMIT: return status.customer.email.Order_Status.SUBMIT;
            case OPEN: return status.customer.email.Order_Status.OPEN;
            case DOWN_TO_ROUTING: return status.customer.email.Order_Status.DOWN_TO_ROUTING;
            case LABEL: return status.customer.email.Order_Status.LABEL;
            case ROUTED: return status.customer.email.Order_Status.ROUTED;
            case SHIPPED: return status.customer.email.Order_Status.SHIPPED;
            case CANCEL: return status.customer.email.Order_Status.CANCEL;
            default: throw new IllegalArgumentException("Unexpected value: " + sourceStatus);
        }
    }

    public static status.enterprise.email.Order_Status toEnterpriseStatus( org.order.status.Order_Status sourceStatus) {
        switch (sourceStatus) {
            case SUBMIT: return status.enterprise.email.Order_Status.SUBMIT;
            case OPEN: return status.enterprise.email.Order_Status.OPEN;
            case DOWN_TO_ROUTING: return status.enterprise.email.Order_Status.DOWN_TO_ROUTING;
            case LABEL: return status.enterprise.email.Order_Status.LABEL;
            case ROUTED: return status.enterprise.email.Order_Status.ROUTED;
            case SHIPPED: return status.enterprise.email.Order_Status.SHIPPED;
            case CANCEL: return status.enterprise.email.Order_Status.CANCEL;
            default: throw new IllegalArgumentException("Unexpected value: " + sourceStatus);
        }
    }

    public static status.error.email.Order_Status toErrorStatus( org.order.status.Order_Status sourceStatus) {
        switch (sourceStatus) {
            case SUBMIT: return status.error.email.Order_Status.SUBMIT;
            case OPEN: return status.error.email.Order_Status.OPEN;
            case DOWN_TO_ROUTING: return status.error.email.Order_Status.DOWN_TO_ROUTING;
            case LABEL: return status.error.email.Order_Status.LABEL;
            case ROUTED: return status.error.email.Order_Status.ROUTED;
            case SHIPPED: return status.error.email.Order_Status.SHIPPED;
            case CANCEL: return status.error.email.Order_Status.CANCEL;
            default: throw new IllegalArgumentException("Unexpected value: " + sourceStatus);
        }
    }
}
