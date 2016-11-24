package com.caijun.em;

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;

import com.caijun.em.monitor.dbnet.DBInfo;

public class SysInit implements ApplicationListener<ContextRefreshedEvent>{
	private Systore systore;

	public void setSystore(Systore systore) {
		this.systore = systore;
	}
	
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		ConfigurableApplicationContext context = (ConfigurableApplicationContext) event.getApplicationContext();
		ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
		
		
		
	}
	
	private BeanDefinitionBuilder initDataSource(DBInfo dbInfo) {
        BeanDefinitionBuilder dataSourceBuider = BeanDefinitionBuilder.genericBeanDefinition(com.mchange.v2.c3p0.ComboPooledDataSource.class);
        dataSourceBuider.setDestroyMethodName("closes");
        dataSourceBuider.addPropertyValue("driverClass", "oracle.jdbc.driver.OracleDriver");
        dataSourceBuider.addPropertyValue("jdbcUrl", dbInfo.getUrl());
        dataSourceBuider.addPropertyValue("user", dbInfo.getUsr());
        dataSourceBuider.addPropertyValue("password", dbInfo.getPassword());
        return dataSourceBuider;
    }

	

}
