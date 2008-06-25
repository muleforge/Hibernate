package org.mule.transport.hibernate;

import org.mule.api.MuleContext;
import org.mule.api.transaction.Transaction;
import org.mule.api.transaction.TransactionException;
import org.mule.api.transaction.TransactionFactory;

public class HibernateTransactionFactory implements TransactionFactory {

    public Transaction beginTransaction(MuleContext muleContext) throws TransactionException {
        Transaction tx = new HibernateTransaction();
        tx.begin();
        return tx;
    }

    public boolean isTransacted() {
        return true;
    }

}
