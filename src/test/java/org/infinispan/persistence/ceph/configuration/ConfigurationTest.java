package org.infinispan.persistence.ceph.configuration;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.ceph.CephStore;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.ceph.configuration.ConfigurationTest")
public class ConfigurationTest<K, V> extends AbstractInfinispanTest {

   public void testCacheStoreConfiguration() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.persistence().addStore(CephStoreConfigurationBuilder.class)
         .userName("admin")
         .key("AQCY2sdXyDIcJxAAK1edRJ8xOJ2NkkiXzAuq5A==")
         .monitorHost("192.168.122.145:6789")
         .poolName("ispn-store");

      Configuration configuration = cfg.build();
      CephStoreConfiguration storeCfg = (CephStoreConfiguration) configuration.persistence().stores().get(0);
      assertEquals(storeCfg.userName(), "admin");
      assertEquals(storeCfg.key(), "AQCY2sdXyDIcJxAAK1edRJ8xOJ2NkkiXzAuq5A==");
      assertEquals(storeCfg.monitorHost(), "192.168.122.145:6789");
      assertEquals(storeCfg.poolName(), "ispn-store");
      
      cfg = new ConfigurationBuilder();
      cfg.persistence().addStore(CephStoreConfigurationBuilder.class).read(storeCfg);
      Configuration configuration2 = cfg.build();
      CephStoreConfiguration storeCfg2 = (CephStoreConfiguration) configuration2.persistence().stores().get(0);
      assertEquals(storeCfg2.userName(), "admin");
      assertEquals(storeCfg2.key(), "AQCY2sdXyDIcJxAAK1edRJ8xOJ2NkkiXzAuq5A==");
      assertEquals(storeCfg2.monitorHost(), "192.168.122.145:6789");
      assertEquals(storeCfg2.poolName(), "ispn-store");
   }
   
   public void testDefaultPoolName() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.persistence().addStore(CephStoreConfigurationBuilder.class)
         .userName("admin")
         .key("AQCY2sdXyDIcJxAAK1edRJ8xOJ2NkkiXzAuq5A==")
         .monitorHost("192.168.122.145:6789")
         .poolNamePrefix("ispn");
      
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(cfg)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache("testCache$%");
            @SuppressWarnings("unchecked")
            CephStore<K, V> store = (CephStore<K, V>) TestingUtil.getFirstLoader(cache);
            assertEquals(store.getPoolName(), "ispn_testCache__");
         }
      });
   }
}