package org.infinispan.persistence.ceph.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfigurationChildBuilder;

public abstract class AbstractCephStoreConfigurationChildBuilder<S> extends AbstractStoreConfigurationChildBuilder<S> implements CephStoreConfigurationChildBuilder<S> {
   private final CephStoreConfigurationBuilder builder;

   protected AbstractCephStoreConfigurationChildBuilder(CephStoreConfigurationBuilder builder) {
      super(builder);
      this.builder = builder;
   }

   @Override
   public CephStoreConfigurationBuilder userName(String username) {
      return builder.userName(username);
   }
   
   @Override
   public CephStoreConfigurationBuilder key(String key) {
      return builder.key(key);
   }
   
   @Override
   public CephStoreConfigurationBuilder keyringPath(String keyringPath) {
      return builder.keyringPath(keyringPath);
   }
   
   @Override
   public CephStoreConfigurationBuilder monitorHost(String monitorHost) {
      return builder.monitorHost(monitorHost);
   }
   
   @Override
   public CephStoreConfigurationBuilder poolName(String poolName) {
      return builder.poolName(poolName);
   }
   
   
}
