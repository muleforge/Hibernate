package org.mule.transport.hibernate;

import org.mule.api.MuleException;
import org.mule.api.endpoint.OutboundEndpoint;
import org.mule.api.transport.MessageDispatcher;
import org.mule.transport.AbstractMessageDispatcherFactory;

public class HibernateMessageDispatcherFactory extends
		AbstractMessageDispatcherFactory {

	@Override
	public MessageDispatcher create(OutboundEndpoint endpoint)
			throws MuleException {
		return new HibernateMessageDispatcher(endpoint);
	}

}
