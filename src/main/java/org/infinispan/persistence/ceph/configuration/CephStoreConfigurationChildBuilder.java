package org.infinispan.persistence.ceph.configuration;

import org.infinispan.configuration.cache.StoreConfigurationChildBuilder;

public interface CephStoreConfigurationChildBuilder<S> extends StoreConfigurationChildBuilder<S> {
   
   /**
    * User name for authentication against Ceph.
    */
   public CephStoreConfigurationBuilder userName(String userName);
   
   /**
    * Key for Ceph authentication. Alternatively, path to keyring file which contains key can be set up via {@link keyringPath}. 
    */
   public CephStoreConfigurationBuilder key(String key);
   
   /**
    * Path to Ceph key ring file. This file must contain valid key for authnetication.
    */
   public CephStoreConfigurationBuilder keyringPath(String keyringPath);
   
   /**
    * Ceph monitor. Expected format is $MINITOR_HOST:$PORT, e.g. 127.0.0.1:6789
    */
   public CephStoreConfigurationBuilder monitorHost(String monitorHost);
   
   /**
    * Name of the Ceph pool where cache data will be stored. Note that if pool name is set, no additional prefix is added, so if two
    * caches are configured with same pool name, cache store will overwrite keys with the same name!
    */
   public CephStoreConfigurationBuilder poolName(String poolName);
   
   /**
    * Prefix for constructing Ceph pool name. Pool name is constructed as $PREFIX_$CACHENAME, where $CACHENAME is cache name where all 
    * non-alphabetical and non-digit characters are replaced by underscode. 
    */
   public CephStoreConfigurationBuilder poolNamePrefix(String poolNamePrefix);
   
   

}
