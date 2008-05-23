package org.mule.transport.hibernate;

import org.hibernate.Session;
import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.endpoint.OutboundEndpoint;
import org.mule.api.transaction.Transaction;
import org.mule.transaction.TransactionCoordination;
import org.mule.transport.AbstractMessageDispatcher;

public class HibernateMessageDispatcher extends AbstractMessageDispatcher {

	private HibernateConnector connector;
	
	public HibernateMessageDispatcher(OutboundEndpoint endpoint) {
		super(endpoint);
		this.connector = (HibernateConnector) endpoint.getConnector();
	}

	@Override
	protected void doConnect() throws Exception {
	}

	@Override
	protected void doDisconnect() throws Exception {
	}


	@Override
	protected void doDispose() {
	}

	@Override
	protected MuleMessage doSend(MuleEvent event) throws Exception {
		doDispatch(event);
        return event.getMessage();
	}

	@Override
	protected void doDispatch(MuleEvent event) throws Exception {
        if (logger.isDebugEnabled())
            logger.debug("Dispatch event: " + event);
   
        String writeStmt = event.getEndpoint().getEndpointURI().getAddress();

        Object payload = event.transformMessage();
        
        Transaction tx = TransactionCoordination.getInstance().getTransaction();
        
        Session session = null;
        
        org.hibernate.Transaction stx = null;
    	try {
        	session = connector.getSession();
        	if (tx == null)
        		stx = session.beginTransaction();
        	
        	if (writeStmt.equals("merge")) {
        		connector.getSessionMerge().merge(session, payload);
        	} else if (writeStmt.equals("delete")) {
        		connector.getSessionDelete().delete(session, payload);
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
