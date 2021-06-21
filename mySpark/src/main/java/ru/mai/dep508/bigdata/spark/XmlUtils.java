package ru.mai.dep508.bigdata.spark;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.*;

import java.util.*;

/**
 * Simple xml parsing utilities.
 */
public class XmlUtils {


    public static Map<String, String> parseXmlToMap(String xml) {
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

    public static Row parseXmlToRow(String xml, String[] fields) {
        Map<String, String> map = parseXmlToMap(xml);
        Object[] values = new Object[fields.length];
        int i = 0;
        for (String field : fields) {
            values[i++] = map.get(field);
        }
        return new GenericRow(values);
    }

    public static StructType createDataFrameStruct(String[] fields) {
        StructField[] structFields = Arrays.stream(fields)
                .map(field -> DataTypes.createStructField(field, DataTypes.StringType, true))
                .toArray(StructField[]::new);
        return new StructType(structFields);
    }

    public static List<String> splitTags(String tagString) {
        if (StringUtils.isBlank(tagString)) {
            return Collections.emptyList();
        }
        String unescapedTags = StringEscapeUtils.unescapeHtml(tagString);
        String[] tags = unescapedTags.split("><");
        tags[0] = tags[0].substring(1);
        String lastTag = tags[tags.length - 1];
        tags[tags.length - 1] = lastTag.substring(0, lastTag.length() - 1);

        return Arrays.asList(tags);
    }
}
