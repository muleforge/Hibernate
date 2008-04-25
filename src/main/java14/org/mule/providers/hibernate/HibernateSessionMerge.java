package org.mule.providers.hibernate;

import org.hibernate.Session;

public interface HibernateSessionMerge {

	public Object merge(Session session, Object payload);
	
}
