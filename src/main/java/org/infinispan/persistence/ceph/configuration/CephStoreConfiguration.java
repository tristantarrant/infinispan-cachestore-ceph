package org.infinispan.persistence.ceph.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.ceph.CephStore;
import org.infinispan.persistence.keymappers.MarshalledValueOrPrimitiveMapper;

@ConfigurationFor(CephStore.class)
@BuiltBy(CephStoreConfigurationBuilder.class)
public class CephStoreConfiguration extends AbstractStoreConfiguration {

   final static AttributeDefinition<String> USER_NAME = AttributeDefinition.builder("userName", "admin").immutable()
         .build();
   final static AttributeDefinition<String> KEY = AttributeDefinition.builder("key", "").immutable().build();
   final static AttributeDefinition<String> KEYRING_PATH = AttributeDefinition
         .builder("keyringPath", "/etc/ceph/ceph.client.admin.keyring").immutable().build();
   final static AttributeDefinition<String> MONITOR_HOST = AttributeDefinition.builder("monitorHost", "127.0.0.1:6789")
         .immutable().build();
   final static AttributeDefinition<String> POOL_NAME = AttributeDefinition.builder("poolName", "").immutable().build();
   final static AttributeDefinition<String> POOL_NAME_PREFIX = AttributeDefinition.builder("poolNamePrefix", "ispn")
         .immutable().build();
   final static AttributeDefinition<String> KEY_2_STRING_MAPPER = AttributeDefinition
         .builder("key2StringMapper", MarshalledValueOrPrimitiveMapper.class.getName()).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CephStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(),
            USER_NAME, KEY, KEYRING_PATH, MONITOR_HOST, POOL_NAME, POOL_NAME_PREFIX, KEY_2_STRING_MAPPER);
   }

   private final Attribute<String> userName;
   private final Attribute<String> key;
   private final Attribute<String> keyringPath;
   private final Attribute<String> monitorHost;
   private final Attribute<String> poolName;
   private final Attribute<String> poolNamePrefix;
   private final Attribute<String> key2StringMapper;

   public CephStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
         SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);

      this.userName = attributes.attribute(USER_NAME);
      this.key = attributes.attribute(KEY);
      this.keyringPath = attributes.attribute(KEYRING_PATH);
      this.monitorHost = attributes.attribute(MONITOR_HOST);
      this.poolName = attributes.attribute(POOL_NAME);
      this.poolNamePrefix = attributes.attribute(POOL_NAME_PREFIX);
      this.key2StringMapper = attributes.attribute(KEY_2_STRING_MAPPER);
   }

   public String userName() {
      return userName.get();
   }

   public String key() {
      return key.get();
   }

   public String keyringPath() {
      return keyringPath.get();
   }

   public String monitorHost() {
      return monitorHost.get();
   }

   public String poolName() {
      return poolName.get();
   }

   public String poolNamePrefix() {
      return poolNamePrefix.get();
   }

   public String key2StringMapper() {
      return key2StringMapper.get();
   }

}
