# Hibernate Transport for Mule 3.1

This transport implements hibernate support for Mule 3.1

## Build Instructions:
To build use Maven execute the below command from the root folder, this will build the mule-transport-hibernate-3.x.x.jar and add it to the transports folder, you can then reference this jar from your mule project.

```
mvn package
```

You can also import the eclipse project to your workspace and reference the project in your mule project java  build path for debugging.

## Use Instructions:
To include this transport in your mule flow you need to create...  
1. Spring Bean JDBC Data Source  
2. Spring Bean Hibernate Session Factory  
3. Spring Bean Hibernate Transaction Manager  
4. JDBC Connector  
5. Hibernate Connector  
6. Hibernate Endpoint

## Spring Bean JDBC Data Source
```
<spring:bean id="jdbcDataSource" 
        name="Bean" 
        class="org.enhydra.jdbc.standard.StandardDataSource" 
        destroy-method="shutdown" 
        doc:name="JDBCDataSourceBean>
    <spring:property 
            name="url" 
            value="jdbc:sqlserver://localhost:1433;databaseName=TestDB;user=sa;password=cupcake"/>
    <spring:property 
            name="driverName" 
            value="com.microsoft.sqlserver.jdbc.SQLServerDriver"/>  
</spring:bean>
```

## Spring Bean Hibernate Session Factory
```
<spring:bean 
        id="hibernateSessionFactory" 
        name="hibernateSessionFactory"
        class="org.springframework.orm.hibernate3.annotation.AnnotationSessionFactoryBean"
        doc:name="Hibernate Session Factory">  
    <spring:property name="hibernateProperties">  
        <spring:props>  
            <spring:prop key="hibernate.show_sql">true</spring:prop>  
            <spring:prop key="hibernate.dialect">org.hibernate.dialect.SQLServerDialect</spring:prop>  
        </spring:props>  
    </spring:property>  
    <spring:property name="annotatedClasses">  
        <spring:list>  
            <spring:value>Person</spring:value>  
        </spring:list>  
    </spring:property>  
    <spring:property name="configurationClass">  
        <spring:value>org.hibernate.cfg.AnnotationConfiguration</spring:value>  
    </spring:property>  
    <spring:property name="dataSource" ref="jdbcDataSource"/>  
</spring:bean>
```

## Spring Bean Hibernate Transaction Manager
```
<spring:bean 
        id="transactionManager" 
        name="transactionManager" 
        class="org.springframework.orm.hibernate3.HibernateTransactionManager" 
        doc:name="Transaction Manager">  
    <spring:property name="sessionFactory" ref="hibernateSessionFactory"/>  
</spring:bean>
```

## JDBC Connector
```
<jdbc:connector 
        name="jdbcConnector" 
        dataSource-ref="jdbcDataSource" 
        validateConnections="false" 
        transactionPerMessage="true" 
        queryTimeout="10" 
        pollingFrequency="5000" 
        doc:name="Database (JDBC)"/>
```

## Hibernate Connector
``` 
<hibernate:connector name="hibernateConnector" sessionFactory-ref="hibernateSessionFactory"/>
```

## Hibernate Endpoint -- add to your flow
```
<hibernate:outbound-endpoint  change="merge" connector-ref="hibernateConnector"/>
```

## Namespace:
To use the hibernate xml namespace you must import it into your flow add...
```
xmlns:hibernate="http://tabletlive.com/schema/mule/hibernate"
```
to your root node and...  
```
http://tabletlive.com/schema/mule/hibernate http://tabletlive.com/schema/mule/hibernate/mule-hibernate.xsd
```
to the root nodes schema location attribute.

The above example uses a JDBC connection for Microsoft SQL Server however you can change your datasource to any JDBC driver you choose.  
Another thing to note is the property 
```
<spring:property name="annotatedClasses">
    <spring:list>
        <spring:value>Person</spring:value>
    </spring:list>
</spring:property>
```
on the session factory, this is the list of the classes you would like to persist with Hibernate, you can use a plain hibernate config instead of this, please see the Spring configuration guide for hibernate for further info [http://static.springsource.org/spring/docs/2.5.x/reference/orm.html](http://static.springsource.org/spring/docs/2.5.x/reference/orm.html).