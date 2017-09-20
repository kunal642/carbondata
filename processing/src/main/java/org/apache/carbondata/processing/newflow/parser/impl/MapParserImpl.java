package org.apache.carbondata.processing.newflow.parser.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.newflow.complexobjects.MapObject;
import org.apache.carbondata.processing.newflow.parser.ComplexParser;
import org.apache.carbondata.processing.newflow.parser.GenericParser;

import org.apache.commons.lang.ArrayUtils;

/**
 * It parses the string to @{@link MapObject} using delimiter.
 * It is thread safe as the state of class don't change while
 * calling @{@link GenericParser#parse(Object)} method
 */
public class MapParserImpl implements ComplexParser<MapObject> {

  private Pattern pattern;

  private Pattern pattern1;

  private List<GenericParser> children = new ArrayList<>(2);

  private String nullFormat;

  public MapParserImpl(String[] delimiter, String nullFormat) {
    pattern = Pattern.compile(CarbonUtil.delimiterConverter(delimiter[0]));
    pattern1 = Pattern.compile(CarbonUtil.delimiterConverter(delimiter[1]));
    this.nullFormat = nullFormat;
  }

  @Override
  public MapObject parse(Object data) {
    if (data != null) {
      String value = data.toString();
      if (!value.isEmpty() && !value.equals(nullFormat)) {
        String[] splits = pattern.split(value, -1);
        if (ArrayUtils.isNotEmpty(splits)) {
          Map<Object, Object> map = new HashMap<>(splits.length);
          for (String split: splits) {
            String[] keyValue = splitKeyValue(split);
            map.put(children.get(0).parse(keyValue[0]), children.get(1).parse(keyValue[1]));
          }
          return new MapObject(map);
        }
      }
    }
    return null;
  }

  private String[] splitKeyValue(String str) {
    return pattern1.split(str);
  }

  @Override
  public void addChildren(GenericParser parser) {
    children.add(parser);
  }

}
