package org.mule.providers.hibernate;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.hibernate.Session;
import org.mule.impl.MuleMessage;
import org.mule.providers.ConnectException;
import org.mule.providers.TransactedPollingMessageReceiver;
import org.mule.transaction.TransactionCoordination;
import org.mule.umo.UMOComponent;
import org.mule.umo.UMOMessage;
import org.mule.umo.UMOTransaction;
import org.mule.umo.endpoint.UMOEndpoint;
import org.mule.umo.lifecycle.InitialisationException;
import org.mule.umo.provider.UMOConnector;
import org.mule.umo.provider.UMOMessageAdapter;

public class HibernateMessageReceiver extends TransactedPollingMessageReceiver {

	private HibernateConnector hibernateConnector;
	private String readStmt;
	private boolean singleMessage;
	private String ackStmt;
	private boolean singleAck;
	private boolean ackIsDelete;
	
	public HibernateMessageReceiver(UMOConnector connector,
            UMOComponent component,
            UMOEndpoint endpoint,
            String readStmt, boolean singleMessage,
            String ackStmt, boolean singleAck,
            long pollingFrequency) throws InitialisationException {
		super(connector, component, endpoint, pollingFrequency);
		this.hibernateConnector = (HibernateConnector) connector;
		this.readStmt = readStmt;
		this.singleMessage = singleMessage;
		this.ackStmt = ackStmt;
		this.singleAck = singleAck;
		
		if (logger.isDebugEnabled())
			logger.debug("singleMessage = "+singleMessage+" ; singleAck = "+singleAck);
		if (ackStmt != null) 
			ackIsDelete = ackStmt.equals("delete");
		
		if (this.singleMessage && this.singleAck && ackIsDelete)
			throw new IllegalArgumentException("Cannot delete a single message in a single ack");
		
		if (! this.singleMessage && this.singleAck)
			throw new IllegalArgumentException("Cannot acknowledge non-single message in a single ack");
		
		
	}
	
	//@SuppressWarnings("unchecked")
	//@Override
	protected List/*<Object>*/ getMessages() throws Exception {
		 Session session = null;
		 try {
			 try {
				 session = this.hibernateConnector.getSession();
			 } catch (Exception e) {
				 throw new ConnectException(e, this);
			 }
			 
			 List messages = session.createQuery(readStmt).list();
			 if (singleMessage)
				 return Collections.singletonList((Object) messages);
			 else
				 return messages;
		 } finally {
			 this.hibernateConnector.closeSession(session);
		 }

	}

	//@SuppressWarnings("unchecked")
	//@Override
	protected void processMessage(Object message) throws Exception {
		Session session = null;
        UMOTransaction tx = TransactionCoordination.getInstance().getTransaction();
        try {
            session = hibernateConnector.getSession();
            UMOMessageAdapter msgAdapter = connector.getMessageAdapter(message);
            UMOMessage umoMessage = new MuleMessage(msgAdapter);
            if (ackStmt != null) {
            	if (ackIsDelete) {
            		if (logger.isDebugEnabled())
            			logger.debug("processMessage::delete "+message);
            		if (singleAck) {
            			session.delete(message);
            		} else {
            			//for (Object m : (List<Object>) message)
            			for (Iterator iter = ((List) message).iterator(); iter.hasNext();)
            				session.delete(iter.next());
            		}
            	} else {
            		if (logger.isDebugEnabled())
            			logger.debug("processMessage::update "+message);
            		
            		if (singleAck) {
            			hibernateConnector.executeUpdate(session, ackStmt, message);
            		} else {
            			//for (Object m : (List<Object>) message)
            			for (Iterator iter = ((List) message).iterator(); iter.hasNext();)
            				hibernateConnector.executeUpdate(session, ackStmt, iter.next());
            		}
            	}
            }
            routeMessage(umoMessage, tx, tx != null || endpoint.isSynchronous());
        } catch (Exception e) {
            if (tx != null) 
                tx.setRollbackOnly();
            throw e;
        } finally {
            if (tx == null) {
            	session.flush();
            	hibernateConnector.closeSession(session);
            }
        }
		
	}

	//@Override
	protected void doConnect() throws Exception {
		 Session session = null;
		 try {
			 session = this.hibernateConnector.getSession();
		 } catch (Exception e) {
			 throw new ConnectException(e, this);
		 } finally {
			 this.hibernateConnector.closeSession(session);
		 }
		
	}

	//@Override
	protected void doDisconnect() throws Exception {
		// TODO Auto-generated method stub
		
	}

	//@Override
	protected void doDispose() {
		// TODO Auto-generated method stub
		
	}

}
