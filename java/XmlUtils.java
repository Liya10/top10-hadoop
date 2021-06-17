
package ru.mai.dep806.bigdata.mr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple xml parsing utilities.
 */
public class XmlUtils {

    // Example row:
    //  <row Id="8" Reputation="947" CreationDate="2008-07-31T21:33:24.057" DisplayName="Eggs McLaren" LastAccessDate="2012-10-15T22:00:45.510" WebsiteUrl="" Location="" AboutMe="&lt;p&gt;This is a puppet test account." Views="5163" UpVotes="12" DownVotes="9" AccountId="6" />
    public static Map<String, String> parseXmlRow(String xml) {
        Map<String, String> map = new HashMap<>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");

            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];

                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }

        return map;
    }
}