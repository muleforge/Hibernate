package org.mule.transport.hibernate;

import org.hibernate.Session;

public interface HibernateSessionMerge {

	public Object merge(Session session, Object payload);
	
}
