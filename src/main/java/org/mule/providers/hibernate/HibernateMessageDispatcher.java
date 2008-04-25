package org.mule.providers.hibernate;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.mule.providers.AbstractMessageDispatcher;
import org.mule.transaction.TransactionCoordination;
import org.mule.umo.UMOEvent;
import org.mule.umo.UMOMessage;
import org.mule.umo.UMOTransaction;
import org.mule.umo.endpoint.UMOImmutableEndpoint;

public class HibernateMessageDispatcher extends AbstractMessageDispatcher {

	private HibernateConnector connector;
	
	public HibernateMessageDispatcher(UMOImmutableEndpoint endpoint) {
		super(endpoint);
		this.connector = (HibernateConnector) endpoint.getConnector();
	}

	//@Override
	protected void doConnect() throws Exception {
	}

	//@Override
	protected void doDisconnect() throws Exception {
	}


	//@Override
	protected void doDispose() {
	}

	//@Override
	protected UMOMessage doReceive(long timeout) throws Exception {
		throw new UnsupportedOperationException("doReceive(timeout), use HibernateMessageReceiver");
	}

	//@Override
	protected UMOMessage doSend(UMOEvent event) throws Exception {
		doDispatch(event);
        return event.getMessage();
	}

	//@Override
	protected void doDispatch(UMOEvent event) throws Exception {
        if (logger.isDebugEnabled())
            logger.debug("Dispatch event: " + event);
   
        UMOImmutableEndpoint endpoint = event.getEndpoint();
        String writeStmt = connector.createSenderParameter(endpoint);

        Object payload = event.getTransformedMessage();
        
        UMOTransaction tx = TransactionCoordination.getInstance().getTransaction();
        
        Session session = null;
        
        Transaction stx = null;
    	try {
        	session = connector.getSession();
        	if (tx == null)
        		stx = session.beginTransaction();
        	
        	if (writeStmt.equals("merge")) {
        		connector.getSessionMerge().merge(session, payload);
        	} else if (writeStmt.equals("delete")) {
        		session.delete(payload);
        	} else {
        		connector.executeUpdate(session, writeStmt, payload);
        	}
        	session.flush();
        	if (tx == null) 
        		stx.commit();
        	
            logger.debug("Event dispatched succesfully");
        } catch (Exception e) {
        	logger.debug("Error dispatching event: " + e.getMessage(), e);
        	if (tx == null && stx != null)
        		stx.rollback();
        	throw e;
        } finally {
        	if (tx == null)
        		connector.closeSession(session);
        }
	}

}
