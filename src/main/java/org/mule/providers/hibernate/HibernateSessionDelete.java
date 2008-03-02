package org.mule.providers.hibernate;

import org.hibernate.Session;

public interface HibernateSessionDelete {

	public void delete(Session session, Object payload);
}
