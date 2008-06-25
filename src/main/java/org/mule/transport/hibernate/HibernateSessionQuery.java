package org.mule.transport.hibernate;

import org.hibernate.Query;
import org.hibernate.Session;

public interface HibernateSessionQuery {

	Query createSelectQuery(Session session, String query);
	
	Query createUpdateQuery(Session session, String update, Object message);
	
}
