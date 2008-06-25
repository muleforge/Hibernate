package org.mule.transport.hibernate.config;

import org.mule.config.spring.factories.InboundEndpointFactoryBean;
import org.mule.config.spring.factories.OutboundEndpointFactoryBean;
import org.mule.config.spring.handlers.AbstractMuleNamespaceHandler;
import org.mule.config.spring.parsers.specific.TransactionDefinitionParser;
import org.mule.config.spring.parsers.specific.endpoint.TransportEndpointDefinitionParser;
import org.mule.endpoint.URIBuilder;
import org.mule.transaction.XaTransactionFactory;
import org.mule.transport.hibernate.HibernateConnector;
import org.mule.transport.hibernate.HibernateMessageReceiver;
import org.mule.transport.hibernate.HibernateTransactionFactory;

public class HibernateNamespaceHandler extends AbstractMuleNamespaceHandler {
	
	private static final String RECEIVER_QUERY = "query";
	private static final String DISPATCHER_CHANGE = "change";

	public void init() {
		registerConnectorDefinitionParser(HibernateConnector.class);
		
		TransportEndpointDefinitionParser inboundParser =
			new TransportEndpointDefinitionParser(HibernateConnector.HIBERNATE_PROTOCOL, TransportEndpointDefinitionParser.PROTOCOL, InboundEndpointFactoryBean.class,
					new String[] { RECEIVER_QUERY },  //requiredAddressAttributes 
					new String[] { HibernateMessageReceiver.POLLING_FREQUENCY }   // requiredProperties
			);
		inboundParser.addAlias(RECEIVER_QUERY, URIBuilder.HOST); // anything can go in HOST, contrary to PATH
		registerBeanDefinitionParser("inbound-endpoint", inboundParser);
		
		
		TransportEndpointDefinitionParser outboundParser = new TransportEndpointDefinitionParser(HibernateConnector.HIBERNATE_PROTOCOL, TransportEndpointDefinitionParser.PROTOCOL, OutboundEndpointFactoryBean.class,
				new String[] { DISPATCHER_CHANGE },  // requiredAddressAttributes
				new String[] {} // requiredProperties
		);
		outboundParser.addAlias(DISPATCHER_CHANGE, URIBuilder.HOST); // anything can go in HOST, contrary to PATH
		registerBeanDefinitionParser("outbound-endpoint", outboundParser);
		
		registerBeanDefinitionParser("transaction", new TransactionDefinitionParser(HibernateTransactionFactory.class));
		registerBeanDefinitionParser("xa-transaction", new TransactionDefinitionParser(XaTransactionFactory.class));
	}

}
