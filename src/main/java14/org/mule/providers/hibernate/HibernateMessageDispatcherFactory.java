package org.mule.providers.hibernate;

import org.mule.providers.AbstractMessageDispatcherFactory;
import org.mule.umo.UMOException;
import org.mule.umo.endpoint.UMOImmutableEndpoint;
import org.mule.umo.provider.UMOMessageDispatcher;

public class HibernateMessageDispatcherFactory extends
		AbstractMessageDispatcherFactory {

	//@Override
	public UMOMessageDispatcher create(UMOImmutableEndpoint endpoint)
			throws UMOException {
		return new HibernateMessageDispatcher(endpoint);
	}

}
