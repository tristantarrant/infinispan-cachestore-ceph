package org.infinispan.persistence.ceph.configuration;

import static org.infinispan.persistence.ceph.configuration.CephStoreConfiguration.KEY;
import static org.infinispan.persistence.ceph.configuration.CephStoreConfiguration.KEYRING_PATH;
import static org.infinispan.persistence.ceph.configuration.CephStoreConfiguration.KEY_2_STRING_MAPPER;
import static org.infinispan.persistence.ceph.configuration.CephStoreConfiguration.MONITOR_HOST;
import static org.infinispan.persistence.ceph.configuration.CephStoreConfiguration.POOL_NAME;
import static org.infinispan.persistence.ceph.configuration.CephStoreConfiguration.POOL_NAME_PREFIX;
import static org.infinispan.persistence.ceph.configuration.CephStoreConfiguration.USER_NAME;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class CephStoreConfigurationBuilder
      extends AbstractStoreConfigurationBuilder<CephStoreConfiguration, CephStoreConfigurationBuilder> {

   public CephStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, CephStoreConfiguration.attributeDefinitionSet());
   }

   public CephStoreConfigurationBuilder userName(String userName) {
      attributes.attribute(USER_NAME).set(userName);
      return self();
   }

   public CephStoreConfigurationBuilder key(String key) {
      attributes.attribute(KEY).set(key);
      return self();
   }

   public CephStoreConfigurationBuilder keyringPath(String keyringPath) {
      attributes.attribute(KEYRING_PATH).set(keyringPath);
      return self();
   }

   public CephStoreConfigurationBuilder monitorHost(String monitorHost) {
      attributes.attribute(MONITOR_HOST).set(monitorHost);
      return self();
   }

   public CephStoreConfigurationBuilder poolName(String poolName) {
      attributes.attribute(POOL_NAME).set(poolName);
      return self();
   }
   
   public CephStoreConfigurationBuilder poolNamePrefix(String poolNamePrefix) {
      attributes.attribute(POOL_NAME_PREFIX).set(poolNamePrefix);
      return self();
   }

   public CephStoreConfigurationBuilder key2StringMapper(String key2StringMapper) {
      attributes.attribute(KEY_2_STRING_MAPPER).set(key2StringMapper);
      return self();
   }

   @Override
   public void validate() {
      super.validate();
      //no further validation is needed, if config values are not specified by user, default values are used 
   }

   @Override
   public CephStoreConfiguration create() {
      return new CephStoreConfiguration(attributes.protect(), async.create(), singletonStore.create());
   }

   @Override
   public CephStoreConfigurationBuilder self() {
      return this;
   }

}
