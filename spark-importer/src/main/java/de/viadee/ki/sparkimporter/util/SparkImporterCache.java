package de.viadee.ki.sparkimporter.util;

import de.viadee.ki.sparkimporter.exceptions.WrongCacheValueTypeException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import javax.cache.Cache;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SparkImporterCache {

    public final static String CACHE_VARIABLE_NAMES_AND_TYPES = "cacheVariableNamesAndTypes";

    private Map<String, IgniteCache<String, Object>> caches = new HashMap<>();
    private Map<String, Class> cacheValueTypes = new HashMap<>();
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


    public void addValueToCache(String cacheName, String valueKey, Object value) throws WrongCacheValueTypeException {
        if(caches.containsKey(cacheName)) {
            if(cacheValueTypes.get(cacheName) == null
                    || !cacheValueTypes.get(cacheName).equals(value.getClass())) {
                throw new WrongCacheValueTypeException("value type '"+value.getClass().getSimpleName()+"' of object to be added to cache does not match the ones existing in the cache '"+cacheValueTypes.get(cacheName).getSimpleName()+"'");
            } else {
                caches.get(cacheName).put(valueKey, value);
            }
        } else {
            IgniteCache<String, Object> cache = ignite.getOrCreateCache(cacheName);
            cache.put(valueKey, value);
            caches.put(cacheName, cache);

            cacheValueTypes.put(cacheName, value.getClass());
        }

    }

    public<T extends Object> T getValueFromCache(String cacheName, String valueKey, Class<T> valueType) throws WrongCacheValueTypeException {
        if(caches.containsKey(cacheName)) {
            if(cacheValueTypes.get(cacheName) == null
                    || !cacheValueTypes.get(cacheName).equals(valueType)) {
                throw new WrongCacheValueTypeException("provided value type '"+valueType.getSimpleName()+"' does not match the one from cache '"+cacheValueTypes.get(cacheName).getSimpleName()+"'");
            } else {
                return ((T)caches.get(cacheName).get(valueKey));
            }

        } else {
            return null;
        }
    }

    public<T extends Object> Map<String, T> getAllCacheValues(String cacheName, Class<T> valueType) throws WrongCacheValueTypeException {
        IgniteCache<String, Object> cache = caches.get(cacheName);
        Map<String, T> result = new HashMap<>();
        Iterator<Cache.Entry<String, Object>> it = cache.iterator();
        while(it.hasNext()) {
            Cache.Entry<String, Object> entry = it.next();
            if(cacheValueTypes.get(cacheName) == null
                || !cacheValueTypes.get(cacheName).equals(valueType)) {
                throw new WrongCacheValueTypeException("provided value type '"+valueType.getSimpleName()+"' does not match the one from cache '"+cacheValueTypes.get(cacheName).getSimpleName()+"'");
            } else {
                result.put(entry.getKey(), ((T) entry.getValue()));
            }
        }
        return result;
    }
}
