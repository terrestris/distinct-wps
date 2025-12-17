/* Copyright 2020-present terrestris GmbH & Co. KG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.terrestris.geoserver.wps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.geoserver.config.GeoServer;
import org.geoserver.security.decorators.ReadOnlyDataStore;
import org.geoserver.wps.gs.GeoServerProcess;
import org.geoserver.wps.process.RawData;
import org.geoserver.wps.process.StringRawData;
import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.data.postgis.PostGISDialect;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.VirtualTable;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.util.logging.Logging;
import org.springframework.web.util.UriUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@DescribeProcess(
  title = "distinctValues",
  description = "Gets the distinct values of a column."
)
public class DistinctValues implements GeoServerProcess {

  private static final Logger LOGGER = Logging.getLogger(DistinctValues.class);

  private final ObjectMapper objectMapper;

  private final GeoServer geoServer;

  public DistinctValues(GeoServer geoServer) {
    this.objectMapper = new ObjectMapper();
    this.geoServer = geoServer;
  }

  @DescribeResult(
    name = "result",
    description = "The distinct values json.",
    primary = true
  )
  public RawData execute(
    @DescribeParameter(
      name = "layerName",
      description = "The qualified name of the layer to retrieve the values from. The layer must be based on a " +
        "JDBC/postgres datastore and be based on a single table with a name equal to the layer name."
    ) final String layerName,
    @DescribeParameter(
      name = "propertyName",
      description = "The property name to retrieve the values of."
    ) final String propertyName,
    @DescribeParameter(
      name = "filter",
      description = "An optional CQL filter to apply.",
      min = 0
    ) final String filter,
    @DescribeParameter(
      name = "viewParams",
      description = "Optional view params.",
      min = 0
    ) final String viewParams,
    @DescribeParameter(
      name = "addQuotes",
      description = "Optional flag to add single quotes to the values",
      min = 0
    ) final Boolean addQuotes
  ) throws JsonProcessingException, SQLException {
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      var factory = new JsonNodeFactory(false);
      var root = factory.arrayNode();

      var tableName = layerName.split(":")[1];
      JDBCDataStore dataStore;
      try {
        dataStore = getDataStore(layerName);
      } catch (DataStoreException e) {
        return error(e.getMessage());
      }
      var schema = dataStore.getDatabaseSchema();
      var virtualTables = dataStore.getVirtualTables();
      conn = dataStore.getDataSource().getConnection();
      String sql;
      var virtualTable = virtualTables.get(tableName);
      if (!virtualTables.isEmpty() && virtualTable != null) {
        sql = getModifiedSql(virtualTable, viewParams, propertyName, filter);
        LOGGER.fine(String.format("Modified SQL: %s", sql));
      } else {
        sql = String.format("select distinct(%s) from %s.%s ", propertyName, schema, tableName);
        if (filter != null) {
          var decoded = UriUtils.decode(filter, "UTF-8");
          // workaround to sanitize incoming CQL wrt. null values (GeoServer/Tools doesn't like `= null`)
          decoded = decoded.replaceAll("=\\s*null", "is null");
          var parsedFilter = ECQL.toFilter(decoded);
          var where = new PostGISDialect(null).createFilterToSQL().encodeToString(parsedFilter);
          sql += where;
        }
      }
      LOGGER.fine("Using SQL: " + sql);

      stmt = conn.prepareStatement(sql);
      rs = stmt.executeQuery();

      while (rs.next()) {
        var node = factory.objectNode();
        var value = rs.getString(1);
        if (addQuotes != null && addQuotes) {
          value = String.format("'%s'", value);
        }
        node.put("dsp", rs.getString(1));
        node.put("val", value);
        root.add(node);
      }
      return success(root);
    } catch (Exception e) {
      LOGGER.log(Level.FINE, "Error when getting distinct values: " + e.getMessage());
      LOGGER.log(Level.FINE, "Stack trace:", e);
      return error("Error: " + e.getMessage());
    } finally {
      if (conn != null) {
        conn.close();
      }
      if (stmt != null) {
        stmt.close();
      }
      if (rs != null) {
        rs.close();
      }
    }
  }

  private JDBCDataStore getDataStore(String layerName) throws IOException {
    // HBD: In case of custom attribute configurations the feature source will not produce the correct data store.
    // However, if NO custom attribute configurations are configured, the data store from the feature type itself
    // seems to contain no virtual tables, producing wrong results. So this function will try to use the one
    // returned from the feature source first, if that fails, the one from the feature type will be used.
    var featureType = geoServer.getCatalog().getFeatureTypeByName(layerName);
    if (featureType == null) {
      throw new DataStoreException("Feature type not found.");
    }
    var source = featureType.getFeatureSource(null, null);
    if (source == null) {
      throw new DataStoreException("Source not found.");
    }
    var store = source.getDataStore();
    if (store instanceof ReadOnlyDataStore) {
      var readOnly = (ReadOnlyDataStore) store;
      if (readOnly.isWrapperFor(JDBCDataStore.class)) {
        return readOnly.unwrap(JDBCDataStore.class);
      }
    }
    // try the feature type store
    var info = featureType.getStore();
    store = info.getDataStore(null);
    if (store instanceof JDBCDataStore) {
      return (JDBCDataStore) store;
    }
    if (store instanceof ReadOnlyDataStore) {
      ReadOnlyDataStore wrapper = (ReadOnlyDataStore) store;
      if (wrapper.isWrapperFor(JDBCDataStore.class)) {
        return wrapper.unwrap(JDBCDataStore.class);
      }
    }
    LOGGER.fine("Datastore type is " + store.getClass());
    throw new DataStoreException("Store is not a JDBC data store.");
  }

  private String getModifiedSql(VirtualTable virtualTable, String viewParams, String propertyName, String filter) throws JSQLParserException, CQLException, FilterToSQLException {
    var sql = virtualTable.getSql();
    if (viewParams != null) {
      var decoded = UriUtils.decode(viewParams, "UTF-8");
      var params = decoded.split(";");
      for (var param : params) {
        var parts = param.split(":");
        sql = sql.replace("%" + parts[0] + "%", parts[1]);
      }
    }
    // replace default values in case they were not included in the request
    for (var param : virtualTable.getParameterNames()) {
      sql = sql.replace("%" + param + "%", virtualTable.getParameter(param).getDefaultValue());
    }
    LOGGER.fine("Parsing SQL: " + sql);
    var select = (PlainSelect) CCJSqlParserUtil.parse(sql);
    var distinct = select.getDistinct();
    List<SelectItem<Expression>> selectItems = (List) select.getSelectItems();
    selectItems = selectItems.stream().filter(item -> {
      var expression = item.getExpression();
      var func = new Function();
      func.setName("DISTINCT");
      var list = new ExpressionList<>();
      list.withExpressions(Collections.singletonList(expression));
      func.setParameters(list);
      if (distinct == null) {
        item.withExpression(func);
      }
      if (item.getAlias() != null) {
        return item.getAlias().getName().equalsIgnoreCase(propertyName);
      } else {
        return item.toString().equalsIgnoreCase(propertyName);
      }
    }).collect(Collectors.toList());
    select.setSelectItems((List) selectItems);
    if (filter != null) {
      var decoded = UriUtils.decode(filter, "UTF-8");
      // workaround to sanitize incoming CQL wrt. null values (GeoServer/Tools doesn't like `= null`)
      decoded = decoded.replaceAll("=\\s*null", "is null");
      var parsedFilter = ECQL.toFilter(decoded);
      var where = new PostGISDialect(null).createFilterToSQL().encodeToString(parsedFilter);
      var expression = CCJSqlParserUtil.parseCondExpression(where.substring("WHERE".length()));
      var oldWhere = select.getWhere();
      var and = new AndExpression(oldWhere, expression);
      select.setWhere(and);
    }
    // since most of the select items are cleared, we need to clear the order items as well
    if (select.getOrderByElements() != null) {
      select.getOrderByElements().clear();
    }
    return select.toString();
  }

  public final StringRawData error(String msg) throws JsonProcessingException {
    var returnMap = new HashMap<>(2);
    returnMap.put("message", msg);
    returnMap.put("success", false);
    return new StringRawData(this.objectMapper.writeValueAsString(returnMap), "application/json");
  }

  public final StringRawData success(JsonNode dataset) throws JsonProcessingException {
    return new StringRawData(this.objectMapper.writeValueAsString(dataset), "application/json");
  }

}
