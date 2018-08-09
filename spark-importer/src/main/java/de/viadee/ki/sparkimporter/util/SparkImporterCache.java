package de.viadee.ki.sparkimporter.util;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import javax.cache.Cache;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SparkImporterCache {

    public final static String CACHE_VARIABLE_NAMES_AND_TYPES = "cacheVariableNamesAndTypes";

    private Map<String, IgniteCache<String, String>> caches = new HashMap<>();
    private Ignite ignite;

    private static SparkImporterCache instance;

    private SparkImporterCache(){
        ignite = Ignition.start();
    }

    public static synchronized SparkImporterCache getInstance(){
        if(instance == null){
            instance = new SparkImporterCache();
        }
        return instance;
    }

    public void stopIgnite() {
        Ignition.stopAll(false);
    }


    public void addValueToCache(String cacheName, String valueKey, String value) {
        if(caches.containsKey(cacheName)) {
            caches.get(cacheName).put(valueKey, value);
        } else {
            IgniteCache<String, String> cache = ignite.getOrCreateCache(cacheName);
            cache.put(valueKey, value);
            caches.put(cacheName, cache);
        }

    }

    public String getValueFromCache(String cacheName, String valueKey) {
        if(caches.containsKey(cacheName)) {
            return caches.get(cacheName).get(valueKey);
        } else {
            return null;
        }
    }

    public Map<String, String> getAllCacheValues(String cacheName) {
        IgniteCache<String, String> cache = caches.get(cacheName);
        Map<String, String> result = new HashMap<>();
        Iterator<Cache.Entry<String, String>> it = cache.iterator();
        while(it.hasNext()) {
            Cache.Entry<String, String> entry = it.next();
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
