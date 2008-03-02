package org.mule.providers.hibernate;

import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.mule.config.i18n.CoreMessages;
import org.mule.transaction.AbstractSingleResourceTransaction;
import org.mule.transaction.IllegalTransactionStateException;
import org.mule.umo.TransactionException;

public class HibernateTransaction extends AbstractSingleResourceTransaction {

	private Transaction sessionTx;
	
	public void bindResource(Object key, Object resource) throws TransactionException {
		if (! (resource instanceof Session)) 
			throw new IllegalTransactionStateException(CoreMessages.transactionCanOnlyBindToResources("org.hibernate.Session"));
		
		try {
			Session s = (Session) resource;
			s.setFlushMode(FlushMode.COMMIT);
			sessionTx = s.beginTransaction();
		} catch (HibernateException e) {
            throw new TransactionException(CoreMessages.transactionCommitFailed(), e);
        }
		
		super.bindResource(key, resource);
	}
	
	//@Override
	protected void doBegin() throws TransactionException {
		// called _before_ bindResource
	}

	//@Override
	protected void doCommit() throws TransactionException {
		try {
			Session s = (Session) resource;
			sessionTx.commit();
			
            s.close();
        } catch (HibernateException e) {
            throw new TransactionException(CoreMessages.transactionCommitFailed(), e);
        }
	}

	//@Override
	protected void doRollback() throws TransactionException {
		try {
			Session s = (Session) resource;
			sessionTx.rollback();
			
            s.close();
        } catch (HibernateException e) {
            throw new TransactionException(CoreMessages.transactionCommitFailed(), e);
        }
	}

}
