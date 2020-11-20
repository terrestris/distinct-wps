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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.security.decorators.ReadOnlyDataStore;
import org.geoserver.wps.gs.GeoServerProcess;
import org.geoserver.wps.process.RawData;
import org.geoserver.wps.process.StringRawData;
import org.geotools.data.DataAccess;
import org.geotools.data.FeatureSource;
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
import org.opengis.feature.Feature;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.Filter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
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
        ) final String viewParams
    ) throws JsonProcessingException, SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            JsonNodeFactory factory = new JsonNodeFactory(false);
            ArrayNode root = factory.arrayNode();

            String tableName = layerName.split(":")[1];
            FeatureTypeInfo featureType = geoServer.getCatalog().getFeatureTypeByName(layerName);
            if (featureType == null) {
                return error("Feature type not found.");
            }
            FeatureSource<? extends FeatureType, ? extends Feature> source = featureType.getFeatureSource(null, null);
            if (source == null) {
                return error("Source not found.");
            }
            DataAccess<? extends FeatureType, ? extends Feature> store = source.getDataStore();
            if (!(store instanceof JDBCDataStore) && !(store instanceof ReadOnlyDataStore)) {
                return error("Store is not a JDBC data store.");
            }
            if (store instanceof ReadOnlyDataStore) {
                store = ((ReadOnlyDataStore) store).unwrap(JDBCDataStore.class);
            }
            if (!(store instanceof JDBCDataStore)) {
                return error("Store is not a JDBC data store.");
            }
            JDBCDataStore dataStore = (JDBCDataStore) store;
            Map<String, VirtualTable> virtualTables = dataStore.getVirtualTables();
            conn = dataStore.getDataSource().getConnection();
            String sql;
            if (virtualTables.size() > 0) {
                sql = getModifiedSql(virtualTables.get(tableName), viewParams, propertyName, filter);
            } else {
                sql = String.format("select distinct(%s) from %s ", propertyName, tableName);
                if (filter != null) {
                    Filter parsedFilter = ECQL.toFilter(filter);
                    String where = new PostGISDialect(null).createFilterToSQL().encodeToString(parsedFilter);
                    sql += where;
                }
            }
            LOGGER.fine("Using SQL: " + sql);

            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();

            while (rs.next()) {
                ObjectNode node = factory.objectNode();
                node.put("dsp", rs.getString(1));
                node.put("val", rs.getString(1));
                root.add(node);
            }
            return success(root);
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Error when getting distinct values: " + e.getMessage());
            LOGGER.log(Level.FINEST, "Stack trace:", e);
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

    private String getModifiedSql(VirtualTable virtualTable, String viewParams, String propertyName, String filter) throws JSQLParserException, CQLException, FilterToSQLException {
        String sql = virtualTable.getSql();
        if (viewParams != null) {
            String[] params = viewParams.split(";");
            for (String param : params) {
                String[] parts = param.split(":");
                sql = sql.replace("%" + parts[0] + "%", parts[1]);
            }
        }
        Statement parse = CCJSqlParserUtil.parse(sql);
        Select stmt = (Select) parse;
        PlainSelect select = (PlainSelect) stmt.getSelectBody();
        List<SelectItem> selectItems = select.getSelectItems();
        selectItems = selectItems.stream().filter(item -> {
            SelectExpressionItem expressionItem = (SelectExpressionItem) item;
            Expression expression = expressionItem.getExpression();
            Function func = new Function();
            func.setName("DISTINCT");
            ExpressionList list = new ExpressionList();
            list.setExpressions(Collections.singletonList(expression));
            func.setParameters(list);
            expressionItem.setExpression(func);
            if (expressionItem.getAlias() != null) {
                return expressionItem.getAlias().getName().equals(propertyName);
            } else {
                return expressionItem.toString().equalsIgnoreCase(propertyName);
            }
        }).collect(Collectors.toList());
        select.setSelectItems(selectItems);
        if (filter != null) {
            Filter parsedFilter = ECQL.toFilter(filter);
            String where = new PostGISDialect(null).createFilterToSQL().encodeToString(parsedFilter);
            Expression expression = CCJSqlParserUtil.parseCondExpression(where.substring("WHERE".length()));
            Expression oldWhere = select.getWhere();
            AndExpression and = new AndExpression(oldWhere, expression);
            select.setWhere(and);
        }
        return select.toString();
    }

    public final StringRawData error(String msg) throws JsonProcessingException {
        Map<String, Object> returnMap = new HashMap<>(2);
        returnMap.put("message", msg);
        returnMap.put("success", false);

        return new StringRawData(this.objectMapper.writeValueAsString(returnMap), "application/json");
    }

    public final StringRawData success(JsonNode dataset) throws JsonProcessingException {
        return new StringRawData(this.objectMapper.writeValueAsString(dataset), "application/json");
    }

}
