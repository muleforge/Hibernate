package org.mule.transport.hibernate;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.mule.api.MuleException;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.service.Service;
import org.mule.api.transaction.Transaction;
import org.mule.api.transaction.TransactionException;
import org.mule.api.transport.MessageReceiver;
import org.mule.transaction.TransactionCoordination;
import org.mule.transaction.XaTransaction;
import org.mule.transaction.XaTransaction.MuleXaObject;
import org.mule.transport.AbstractConnector;
import org.mule.util.MapUtils;



public class HibernateConnector extends AbstractConnector {

	public static final String HIBERNATE_PROTOCOL = "hibernate";
	
	private SessionFactory sessionFactory;
	
	private HibernateSessionMerge sessionMerge;
	private HibernateSessionDelete sessionDelete;
	
	@Override
	protected void doInitialise() throws InitialisationException {}
	
	@Override
	protected void doConnect() throws Exception {}

	@Override
	protected void doDisconnect() throws Exception {}

	@Override
	protected void doDispose() {}

	@Override
	protected void doStart() throws MuleException {}

	@Override
	protected void doStop() throws MuleException {}

	public String getProtocol() {
		return HIBERNATE_PROTOCOL;
	}

	public SessionFactory getSessionFactory() {
		return sessionFactory;
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public HibernateSessionMerge getSessionMerge() {
		/*
		 * not synchronized, so we may return different objects unless it was passed via config, which is good enough 
		 */
		if (sessionMerge == null) {
			sessionMerge = new HibernateSessionMerge() {
				public Object merge(Session session, Object payload) {
					return session.merge(payload);
				}
			};
		}
		
		return sessionMerge;
	}

	public void setSessionMerge(HibernateSessionMerge sessionMerge) {
		this.sessionMerge = sessionMerge;
	}

	public HibernateSessionDelete getSessionDelete() {
		/*
		 * not synchronized, so we may return different objects unless it was passed via config, which is good enough 
		 */
		if (sessionDelete == null) {
			sessionDelete = new HibernateSessionDelete() {
				public void delete(Session session, Object payload) {
					session.delete(payload);
				}
			};
		}
		
		return sessionDelete;
	}
	
	public void setSessionDelete(HibernateSessionDelete sessionDelete) {
		this.sessionDelete = sessionDelete;
	}
	
	@Override
	protected MessageReceiver createReceiver(Service service, InboundEndpoint endpoint) throws Exception {
		return getServiceDescriptor().createMessageReceiver(this, service, endpoint, createReceiverParameters(endpoint));
    }
	
	private Object[] createReceiverParameters(InboundEndpoint endpoint) {
		
		String readQuery = endpoint.getEndpointURI().getAddress();
		Boolean singleMessage = MapUtils.getBoolean(endpoint.getProperties(), HibernateMessageReceiver.SINGLE_MESSAGE, Boolean.FALSE);
		String ackUpdate = (String) endpoint.getProperty(HibernateMessageReceiver.ACK);
		Boolean singleAck = MapUtils.getBoolean(endpoint.getProperties(), HibernateMessageReceiver.SINGLE_ACK, Boolean.FALSE);
		
		if (logger.isDebugEnabled())
			logger.debug("read query = '"+readQuery+"' ; ack update = '"+ackUpdate+"'");
		
		
		
		HibernateSessionQuery sessionQuery = (HibernateSessionQuery) MapUtils.getObject(endpoint.getProperties(), HibernateMessageReceiver.CREATE_QUERY);
		HibernateSessionDefaultQuery defaultQuery = null;
		if (sessionQuery == null) {
			defaultQuery = new HibernateSessionDefaultQuery();
		
			int maxResults = MapUtils.getInteger(endpoint.getProperties(), HibernateMessageReceiver.MAX_RESULTS, -1 /* anything <= 0 means infinite */);
			defaultQuery.setMaxResults(maxResults);
			
			sessionQuery = defaultQuery;
		}
		
		HibernateSessionQuery sessionAck = (HibernateSessionQuery) MapUtils.getObject(endpoint.getProperties(), HibernateMessageReceiver.CREATE_ACK);
        if (sessionAck == null) {
        	sessionAck = defaultQuery != null ? defaultQuery : new HibernateSessionDefaultQuery();
        }
		
		Long pollingFrequency = MapUtils.getLong(endpoint.getProperties(), HibernateMessageReceiver.POLLING_FREQUENCY); // cannot be null, it's required
		
		return new Object[] { sessionQuery, readQuery, singleMessage, sessionAck, ackUpdate, singleAck, pollingFrequency };
		
	}
	
	
	Session getSession() throws Exception
    {
        Transaction tx = TransactionCoordination.getInstance().getTransaction();
        if (tx != null) {
            if (tx.hasResource(sessionFactory)) {
                logger.debug("Retrieving session from current transaction");
                Object r = tx.getResource(sessionFactory);
                if (r instanceof MuleXaSession) 
                	return ((MuleXaSession) r).impl;
                else
                	return (Session) r;
                
            }
        }
        logger.debug("Retrieving new session from session factory");
        final Session session = sessionFactory.openSession();
        if (tx != null) {
            logger.debug("Binding session to current transaction");
            try {
            	Object r;
            	if (tx instanceof XaTransaction) 
            		r = new MuleXaSession(session);
            	else
            		r = session;
            	
            	tx.bindResource(sessionFactory, r);
            } catch (TransactionException e) {
                throw new RuntimeException("Could not bind connection to current transaction", e);
            }
        }
        return session;
    }
	
	void closeSession(Session session) {
		logger.debug("Closing session");
		session.close();
	}

	private class MuleXaSession implements MuleXaObject {
		private Session impl;
		
		MuleXaSession(Session s) { this.impl = s; }

		public void close() throws Exception {
			closeSession(impl);
		}

		public boolean delist() throws Exception {
			return false;
		}

		public Object getTargetObject() {
			return impl;
		}

		public boolean isReuseObject() {
			return false;
		}

		public void setReuseObject(boolean reuseObject) {
		}

	}

}
