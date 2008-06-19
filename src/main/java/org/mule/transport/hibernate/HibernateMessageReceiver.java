package org.mule.transport.hibernate;

import java.util.Collections;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.mule.DefaultMuleMessage;
import org.mule.api.MuleMessage;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.lifecycle.CreateException;
import org.mule.api.service.Service;
import org.mule.api.transaction.Transaction;
import org.mule.api.transport.Connector;
import org.mule.api.transport.MessageAdapter;
import org.mule.transaction.TransactionCoordination;
import org.mule.transport.ConnectException;
import org.mule.transport.TransactedPollingMessageReceiver;

public class HibernateMessageReceiver extends TransactedPollingMessageReceiver {

	private HibernateConnector hibernateConnector;
	private String readStmt;
	private boolean singleMessage;
	private String ackStmt;
	private boolean singleAck;
	private boolean ackIsDelete;
	private HibernateSessionQuery sessionQuery, sessionAck;
	public static final String MAX_RESULTS = "maxResults";
	public static final String SINGLE_ACK = "singleAck";
	public static final String ACK = "ack";
	public static final String SINGLE_MESSAGE = "singleMessage";
	public static final String POLLING_FREQUENCY = "pollingFrequency";
	public static final Object CREATE_QUERY = "createQuery";
	public static final Object CREATE_ACK = "createAck";
	
	public HibernateMessageReceiver(Connector connector,
            Service service,
            InboundEndpoint endpoint,
            HibernateSessionQuery sessionQuery, String readStmt, boolean singleMessage, 
            HibernateSessionQuery sessionAck, String ackStmt, boolean singleAck,
            long pollingFrequency) throws CreateException {
		super(connector, service, endpoint);
		setFrequency(pollingFrequency);
		this.hibernateConnector = (HibernateConnector) connector;
		this.readStmt = readStmt;
		this.singleMessage = singleMessage;
		this.ackStmt = ackStmt;
		this.singleAck = singleAck;
		this.sessionQuery = sessionQuery;
		this.sessionAck = sessionAck;
		
		if (logger.isDebugEnabled())
			logger.debug("singleMessage = "+singleMessage+" ; singleAck = "+singleAck);
		if (ackStmt != null) 
			ackIsDelete = ackStmt.equals("delete");
		
		if (this.singleMessage && this.singleAck && ackIsDelete)
			throw new IllegalArgumentException("Cannot delete a single message in a single ack");
		
		if (! this.singleMessage && this.singleAck)
			throw new IllegalArgumentException("Cannot acknowledge non-single message in a single ack");
		
		
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected List<Object> getMessages() throws Exception {
		 Session session = null;
		 try {
			 try {
				 session = this.hibernateConnector.getSession();
			 } catch (Exception e) {
				 throw new ConnectException(e, this);
			 }
			 
			 Query q = sessionQuery.createSelectQuery(session, readStmt);

			 List messages = q.list();

			 if (singleMessage)
				 return Collections.singletonList((Object) messages);
			 else
				 return messages;
		 } finally {
			 Transaction tx = TransactionCoordination.getInstance().getTransaction();
			 if (tx == null) 
				 this.hibernateConnector.closeSession(session);
		 }

	}

	@SuppressWarnings("unchecked")
	@Override
	protected void processMessage(Object message) throws Exception {
		Session session = null;
        Transaction tx = TransactionCoordination.getInstance().getTransaction();
        try {
            session = hibernateConnector.getSession();
            MessageAdapter msgAdapter = connector.getMessageAdapter(message);
            MuleMessage umoMessage = new DefaultMuleMessage(msgAdapter);
            if (ackStmt != null) {
            	if (ackIsDelete) {
            		if (logger.isDebugEnabled())
            			logger.debug("processMessage::delete "+message);
            		if (singleAck) {
            			hibernateConnector.getSessionDelete().delete(session, message);
            		} else {
            			for (Object m : (List<Object>) message)
            				hibernateConnector.getSessionDelete().delete(session, m);
            		}
            	} else {
            		if (logger.isDebugEnabled())
            			logger.debug("processMessage::update "+message);
            		
            		if (!singleMessage || singleAck) {
            			sessionAck.createUpdateQuery(session, ackStmt, message).executeUpdate();
            		} else {
            			for (Object m : (List<Object>) message)
            				sessionAck.createUpdateQuery(session, ackStmt, m).executeUpdate();
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

	@Override
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

	@Override
	protected void doDisconnect() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void doDispose() {
		// TODO Auto-generated method stub
		
	}

}
