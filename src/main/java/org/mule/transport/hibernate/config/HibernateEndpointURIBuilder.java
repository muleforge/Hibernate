package org.mule.transport.hibernate.config;

import java.net.URI;
import java.util.Properties;

import org.mule.api.endpoint.MalformedEndpointException;
import org.mule.endpoint.AbstractEndpointURIBuilder;

public class HibernateEndpointURIBuilder extends AbstractEndpointURIBuilder {

	@Override
	protected void setEndpoint(URI uri, Properties props)
			throws MalformedEndpointException {
		address = uri.getSchemeSpecificPart().substring(2); // remove '//'
	}

}
