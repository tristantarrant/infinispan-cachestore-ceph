package org.infinispan.persistence.ceph.configuration;

import java.util.HashMap;
import java.util.Map;

public enum Attribute {
   // must be first
   UNKNOWN(null),

   KEY("key"), 
   KEYRING_PATH("keyring-path"), 
   KEY_2_STRING_MAPPER("key-2-string-mapper"), 
   MONITOR_HOST("monitor-host"), 
   POOL_NAME("pool-name"), 
   POOL_NAME_PREFIX("pool-name-prefix"),
   USER_NAME("user-name");

   private final String name;

   private Attribute(final String name) {
      this.name = name;
   }

   public String getLocalName() {
      return name;
   }

   private static final Map<String, Attribute> attributes;

   static {
      final Map<String, Attribute> map = new HashMap<String, Attribute>(64);
      for (Attribute attribute : values()) {
         final String name = attribute.getLocalName();
         if (name != null) {
            map.put(name, attribute);
         }
      }
      attributes = map;
   }

   public static Attribute forName(final String localName) {
      final Attribute attribute = attributes.get(localName);
      return attribute == null ? UNKNOWN : attribute;
   }

   @Override
   public String toString() {
      return name;
   }
}
