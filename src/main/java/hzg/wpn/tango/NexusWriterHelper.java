package hzg.wpn.tango;

import hzg.wpn.xenv.ResourceManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author khokhria
 * @since 2/25/16.
 */
public class NexusWriterHelper {
    private NexusWriterHelper(){}

    private static final Map<String,String> MAPPING = new HashMap<>();

    {
         try{
             Map<String,String> mapping = (Map<String, String>) (Map) ResourceManager.loadProperties("etc/DataFormatServer", "nxpath.mapping");
             MAPPING.putAll(mapping);
         } catch (IOException e) {
             throw new RuntimeException("Failed to load mapping file.", e);
         }

    }

    /**
     *
     *
     * @param key
     * @return associated nxPath or key
     */
    public static String toNxPath(String key){
         return MAPPING.getOrDefault(key, key);
    }
}
