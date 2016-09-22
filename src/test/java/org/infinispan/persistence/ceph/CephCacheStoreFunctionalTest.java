package org.infinispan.persistence.ceph;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.ceph.configuration.CephStoreConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.ceph.CephCacheStoreTest")
public class CephCacheStoreFunctionalTest extends BaseStoreFunctionalTest {
   
   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence,
         boolean preload) {
      persistence.addStore(CephStoreConfigurationBuilder.class)
            .preload(preload)
            .userName("admin")
            .key(System.getProperty("cephKey"))
            .monitorHost(System.getProperty("cephMonitor") == null ? "127.0.0.1:6789" : System.getProperty("cephMonitor"));
      return persistence;
   }

}
