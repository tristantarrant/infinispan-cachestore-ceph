package org.infinispan.persistence.ceph;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.ceph.configuration.CephStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.ceph.CephCacheStoreTest")
public class CephCacheStoreTest<K, V> extends BaseStoreTest {

   @Override
   protected AdvancedLoadWriteStore<Object, Object> createStore() throws PersistenceException {
      CephStore<Object, Object> cs = new CephStore<Object, Object>();
      ConfigurationBuilder cfgBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      cfgBuilder.persistence().addStore(CephStoreConfigurationBuilder.class)
            .userName("admin")
            .key(System.getProperty("cephKey"))
            .monitorHost(System.getProperty("cephMonitor") == null ? "127.0.0.1:6789" : System.getProperty("cephMonitor"));
      cs.init(createContext(cfgBuilder.build()));
      return cs;
   }
}
