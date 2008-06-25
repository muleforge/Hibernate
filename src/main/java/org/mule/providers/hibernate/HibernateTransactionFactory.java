package org.mule.providers.hibernate;

import org.mule.umo.TransactionException;
import org.mule.umo.UMOTransaction;
import org.mule.umo.UMOTransactionFactory;

/**
 * TODO
 */
public class HibernateTransactionFactory implements UMOTransactionFactory
{

    /*
     * (non-Javadoc)
     * 
     * @see org.mule.umo.UMOTransactionFactory#beginTransaction()
     */
    public UMOTransaction beginTransaction() throws TransactionException
    {
        UMOTransaction tx = new HibernateTransaction();
        tx.begin();
        return tx;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.mule.umo.UMOTransactionFactory#isTransacted()
     */
    public boolean isTransacted()
    {
        return true;
    }

}
