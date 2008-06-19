package org.mule.transport.hibernate.extra;

import java.util.List;

import org.hibernate.LockMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.mule.transport.hibernate.HibernateSessionDefaultQuery;

public class HibernateSessionLockModeQuery extends HibernateSessionDefaultQuery {

	private LockMode lockMode;
	private List<String> aliases;
	
	public void setLockMode(String lockMode) {
		this.lockMode = LockMode.parse(lockMode);
	}
	
	public void setAliases(List<String> aliases) {
		this.aliases = aliases;
	}
	
	public Query createSelectQuery(Session session, String query) {
		Query q = super.createSelectQuery(session, query);
		
		if (lockMode != null && aliases != null)
			for (String alias : aliases)
				q.setLockMode(alias, lockMode);
		
		return q;
	}
}
