package org.mule.providers.hibernate;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.jxpath.JXPathContext;
import org.hibernate.LockMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.mule.MuleManager;
import org.mule.config.i18n.CoreMessages;
import org.mule.impl.internal.notifications.NotificationException;
import org.mule.impl.internal.notifications.TransactionNotification;
import org.mule.impl.internal.notifications.TransactionNotificationListener;
import org.mule.providers.AbstractConnector;
import org.mule.providers.jdbc.NowPropertyExtractor;
import org.mule.transaction.TransactionCoordination;
import org.mule.transaction.XaTransaction;
import org.mule.umo.TransactionException;
import org.mule.umo.UMOComponent;
import org.mule.umo.UMOException;
import org.mule.umo.UMOTransaction;
import org.mule.umo.endpoint.UMOEndpoint;
import org.mule.umo.endpoint.UMOImmutableEndpoint;
import org.mule.umo.lifecycle.InitialisationException;
import org.mule.umo.manager.UMOServerNotification;
import org.mule.umo.provider.UMOMessageReceiver;
import org.mule.util.MapUtils;
import org.mule.util.properties.JXPathPropertyExtractor;
import org.mule.util.properties.PropertyExtractor;



public class HibernateConnector extends AbstractConnector implements TransactionNotificationListener {

	private static final String _SINGLE_MESSAGE = ".singleMessage";
	private static final String _ACK = ".ack";
	private static final String _SINGLE_ACK = ".singleAck";
	
	private static final String HIBERNATE_PROTOCOL = "hibernate";
	private SessionFactory sessionFactory;
	private HibernateSessionMerge sessionMerge;
	private HibernateSessionDelete sessionDelete;
	//@SuppressWarnings("unchecked")
	private List/*<Class>*/ queryValueExtractors;
	private List/*<PropertyExtractor>*/ propertyExtractors;
	private Map tx2s = new IdentityHashMap();
	
	// @SuppressWarnings("unchecked")
	private static final List/*<Class>*/ DEFAULT_QUERY_VALUE_EXTRACTORS = new ArrayList/*<Class>*/();
	static {
		DEFAULT_QUERY_VALUE_EXTRACTORS.add(NowPropertyExtractor.class);
		DEFAULT_QUERY_VALUE_EXTRACTORS.add(JXPathPropertyExtractor.class);
	}
	
	//@Override
	//@SuppressWarnings("unchecked")
	protected void doInitialise() throws InitialisationException {
		List/*<Class>*/ qve = queryValueExtractors;
		if (qve == null)
			qve = DEFAULT_QUERY_VALUE_EXTRACTORS;
			
		propertyExtractors = new ArrayList/*<PropertyExtractor>*/();
		try {
			// FIXME when switched to 1.5
			for (Iterator iter = qve.iterator(); iter.hasNext();)
				propertyExtractors.add((PropertyExtractor) ((Class) iter.next()).newInstance());
		} catch (Exception e) {
			throw new InitialisationException(CoreMessages.failedToCreate("Hibernate Connector"), e, this);	
		}
		if (MuleManager.getInstance().getTransactionManager() != null) {
			try {
				MuleManager.getInstance().registerListener(this);
			} catch (NotificationException e) {
				throw new InitialisationException(e, this);
			}
		}
		
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
	protected void doStart() throws UMOException {
	}

	//@Override
	protected void doStop() throws UMOException {
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

	//@SuppressWarnings("unchecked")
	public List/*<Class>*/ getQueryValueExtractors() {
		return queryValueExtractors;
	}

	//@SuppressWarnings("unchecked")
	public void setQueryValueExtractors(List/*<Class>*/ queryValueExtractors) {
		this.queryValueExtractors = queryValueExtractors;
	}
	
	//@Override
	protected UMOMessageReceiver createReceiver(UMOComponent component, UMOEndpoint endpoint) throws Exception {
		return getServiceDescriptor().createMessageReceiver(this, component, endpoint, createReceiverParameters(endpoint));
    }
	
	private Boolean getBooleanProperty(UMOImmutableEndpoint endpoint, String property) {
		String s = (String) endpoint.getProperty(property);
		Boolean single = Boolean.FALSE;
		if (s != null)
			single = Boolean.valueOf(s);
		return single;
	}
	
	/*
	 * org.mule.util.MapUtils new version have getLongValue()
	 */
	private Long getLongProperty(UMOImmutableEndpoint endpoint, String property) {
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
	
	private Object[] createReceiverParameters(UMOImmutableEndpoint endpoint) {
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
		Integer maxResults = new Integer(getLongProperty(endpoint, readName+".maxResults").intValue());
		
		String lockModes = (String) endpoint.getProperty(readName+".lockModes");
		Map lockModeMap = new LinkedHashMap();
		
		if (lockModes != null) {
		StringTokenizer st = new StringTokenizer(lockModes, ";");
		
		while (st.hasMoreTokens()) {
			String t = st.nextToken();
			int i = t.indexOf('=');
			if (i != -1) {
				String alias = t.substring(0, i);
				if (i < t.length()) {
					String lockMode = t.substring(i+1);
					lockModeMap.put(alias, LockMode.parse(lockMode));
				}
			}
			
		}
		}
		
		return new Object[] { readQuery, singleMessage, ackUpdate, singleAck, pollingFrequency, maxResults, lockModeMap };
	}
	
	String createSenderParameter(UMOImmutableEndpoint endpoint) {
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
        UMOTransaction tx = TransactionCoordination.getInstance().getTransaction();
        if (tx != null) {
            if (tx.hasResource(sessionFactory)) {
                logger.debug("Retrieving session from current transaction");
                return (Session) tx.getResource(sessionFactory); 
            }
        }
        logger.debug("Retrieving new session from session factory");
        final Session session = sessionFactory.openSession();
        if (tx != null) {
            logger.debug("Binding session to current transaction");
            try {
            	if (tx instanceof XaTransaction) {
            		synchronized (tx2s) {
						tx2s.put(tx, session);
					}
            	}
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

	
	
	public void onNotification(UMOServerNotification notification) {
		if (notification instanceof TransactionNotification) {
			TransactionNotification tn = (TransactionNotification) notification;
			if (! tn.getActionName().equals("begin")) {
				synchronized (tx2s) {
					Session s = (Session) tx2s.remove(tn.getSource());
					if (s != null) {
						closeSession(s);
					}
				}
			}
		}
	}
	
}
