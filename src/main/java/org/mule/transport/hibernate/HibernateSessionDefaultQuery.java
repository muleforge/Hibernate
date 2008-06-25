package org.mule.transport.hibernate;

import org.apache.commons.jxpath.JXPathContext;
import org.hibernate.Query;
import org.hibernate.Session;

public class HibernateSessionDefaultQuery implements HibernateSessionQuery {

	private int maxResults;
	
	public void setMaxResults(int maxResults) {
		this.maxResults = maxResults;
	}
	
	public Query createSelectQuery(Session session, String query) {
		Query q = session.createQuery(query);
		if (maxResults > 0)
			q.setMaxResults(maxResults);
		return q;
	}

	public Query createUpdateQuery(Session session, String update, Object message) {
		Query q = session.createQuery(update);

		String[] np = q.getNamedParameters();
		if (np != null && np.length > 0) {
			JXPathContext context = JXPathContext.newContext(message);
			for (String p : np)
				q.setParameter(p, context.getValue(p));
		}
		return q;
	}


}
