package org.mule.transport.hibernate;

import org.apache.commons.jxpath.JXPathContext;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.mule.api.MuleException;
import org.mule.api.endpoint.ImmutableEndpoint;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.service.Service;
import org.mule.api.transaction.Transaction;
import org.mule.api.transaction.TransactionException;
import org.mule.api.transport.MessageReceiver;
import org.mule.transaction.TransactionCoordination;
import org.mule.transport.AbstractConnector;



public class HibernateConnector extends AbstractConnector {

	private static final String _SINGLE_MESSAGE = ".singleMessage";
	private static final String _ACK = ".ack";
	private static final String _SINGLE_ACK = ".singleAck";
	
	private static final String HIBERNATE_PROTOCOL = "hibernate";
	private SessionFactory sessionFactory;
	private HibernateSessionMerge sessionMerge;
	private HibernateSessionDelete sessionDelete;
	
	@Override
	protected void doInitialise() throws InitialisationException {
		// TODO Auto-generated method stub
		
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
	protected void doStart() throws MuleException {
	}

	@Override
	protected void doStop() throws MuleException {
	}

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
	
	private Boolean getBooleanProperty(ImmutableEndpoint endpoint, String property) {
		String s = (String) endpoint.getProperty(property);
		Boolean single = Boolean.FALSE;
		if (s != null)
			single = Boolean.valueOf(s);
		return single;
	}
	
	/*
	 * org.mule.util.MapUtils new version have getLongValue()
	 */
	private Long getLongProperty(ImmutableEndpoint endpoint, String property) {
		String s = (String) endpoint.getProperty(property);
		long l = 0L;
		if (s != null) {
			try {
				l = Long.parseLong(s); 
			} catch (NumberFormatException e) {
			}
		}
		return new Long(l);
	}
	
	private Object[] createReceiverParameters(ImmutableEndpoint endpoint) {
		String readName = endpoint.getEndpointURI().getHost();
		if (logger.isDebugEnabled())
			logger.debug("read query name = "+readName);
		
		String readQuery = (String) endpoint.getProperty(readName);
		Boolean singleMessage = getBooleanProperty(endpoint, readName+_SINGLE_MESSAGE);
		String ackUpdate = (String) endpoint.getProperty(readName+_ACK);
		Boolean singleAck = getBooleanProperty(endpoint, readName+_SINGLE_ACK);
		
		if (logger.isDebugEnabled())
			logger.debug("read query = '"+readQuery+"' ; ack update = '"+ackUpdate+"'");
		
		Long pollingFrequency = getLongProperty(endpoint, "pollingFrequency");
		
		return new Object[] { readQuery, singleMessage, ackUpdate, singleAck, pollingFrequency };
	}
	
	String createSenderParameter(ImmutableEndpoint endpoint) {
		String writeName = endpoint.getEndpointURI().getHost();
		logger.debug("write query name = "+writeName);
		
		String writeUpdate = (String) endpoint.getProperty(writeName);
		return writeUpdate;
	}
	
	void executeUpdate(Session session, String updateHql, Object payload) {
		Query q = session.createQuery(updateHql);
		
		String[] np = q.getNamedParameters();
		if (np != null && np.length > 0) {
			JXPathContext context = JXPathContext.newContext(payload);
			//for (String p : np)
			for (int i = 0 ; i < np.length; i++)
				q.setParameter(np[i], context.getValue(np[i]));
		}
		q.executeUpdate();
	}
	
	Session getSession() throws Exception
    {
        Transaction tx = TransactionCoordination.getInstance().getTransaction();
        if (tx != null) {
            if (tx.hasResource(sessionFactory)) {
                logger.debug("Retrieving session from current transaction");
                return (Session) tx.getResource(sessionFactory);
            }
        }
        logger.debug("Retrieving new session from session factory");
        Session session = sessionFactory.openSession();

        if (tx != null) {
            logger.debug("Binding session to current transaction");
            try {
                tx.bindResource(sessionFactory, session);
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



}
