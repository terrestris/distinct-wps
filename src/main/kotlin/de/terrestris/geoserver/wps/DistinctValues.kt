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
package de.terrestris.geoserver.wps

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import net.sf.jsqlparser.JSQLParserException
import net.sf.jsqlparser.expression.Function
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.expression.operators.relational.ExpressionList
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.select.SelectExpressionItem
import net.sf.jsqlparser.statement.select.SelectItem
import org.geoserver.config.GeoServer
import org.geoserver.security.decorators.ReadOnlyDataStore
import org.geoserver.wps.gs.GeoServerProcess
import org.geoserver.wps.process.RawData
import org.geoserver.wps.process.StringRawData
import org.geotools.data.jdbc.FilterToSQLException
import org.geotools.data.postgis.PostGISDialect
import org.geotools.filter.text.cql2.CQLException
import org.geotools.filter.text.ecql.ECQL
import org.geotools.jdbc.JDBCDataStore
import org.geotools.jdbc.VirtualTable
import org.geotools.process.factory.DescribeParameter
import org.geotools.process.factory.DescribeProcess
import org.geotools.process.factory.DescribeResult
import org.geotools.util.logging.Logging
import org.springframework.web.util.UriUtils
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.util.*
import java.util.logging.Level
import java.util.stream.Collectors

@DescribeProcess(title = "distinctValues", description = "Gets the distinct values of a column.")
class DistinctValues(private val geoServer: GeoServer) : GeoServerProcess {

    private val objectMapper: ObjectMapper = ObjectMapper()

    @DescribeResult(name = "result", description = "The distinct values json.", primary = true)
    @Throws(
        JsonProcessingException::class, SQLException::class
    )
    fun execute(
        @DescribeParameter(
            name = "layerName",
            description = "The qualified name of the layer to retrieve the values from. The layer must be based on a " +
                    "JDBC/postgres datastore and be based on a single table with a name equal to the layer name."
        ) layerName: String,
        @DescribeParameter(
            name = "propertyName",
            description = "The property name to retrieve the values of."
        ) propertyName: String,
        @DescribeParameter(name = "filter", description = "An optional CQL filter to apply.", min = 0) filter: String?,
        @DescribeParameter(name = "viewParams", description = "Optional view params.", min = 0) viewParams: String?
    ): RawData {
        var conn: Connection? = null
        var stmt: PreparedStatement? = null
        var rs: ResultSet? = null
        return try {
            val factory = JsonNodeFactory(false)
            val root = factory.arrayNode()
            val tableName = layerName.split(":".toRegex())[1]
            val featureType =
                geoServer.catalog.getFeatureTypeByName(layerName) ?: return error("Feature type not found.")
            val source = featureType.getFeatureSource(null, null) ?: return error("Source not found.")
            var store = source.dataStore
            if (store !is JDBCDataStore && store !is ReadOnlyDataStore) {
                return error("Store is not a JDBC data store.")
            }
            if (store is ReadOnlyDataStore) {
                store = store.unwrap(JDBCDataStore::class.java)
            }
            if (store !is JDBCDataStore) {
                return error("Store is not a JDBC data store.")
            }
            val dataStore = store
            val schema = dataStore.databaseSchema
            val virtualTables = dataStore.virtualTables
            conn = dataStore.dataSource.connection
            var sql: String
            val virtualTable = virtualTables[tableName]
            if (virtualTables.isNotEmpty() && virtualTable != null) {
                sql = getModifiedSql(virtualTable, viewParams, propertyName, filter)
                LOGGER.fine("Modified SQL: $sql")
            } else {
                sql = "select distinct($propertyName) from $schema.$tableName "
                if (filter != null) {
                    val decoded = UriUtils.decode(filter, "UTF-8")
                    val parsedFilter = ECQL.toFilter(decoded)
                    val where = PostGISDialect(null).createFilterToSQL().encodeToString(parsedFilter)
                    sql += where
                }
                LOGGER.fine("Custom SQL: $sql")
            }
            stmt = conn.prepareStatement(sql)
            rs = stmt.executeQuery()
            while (rs.next()) {
                val node = factory.objectNode()
                node.put("dsp", rs.getString(1))
                node.put("val", rs.getString(1))
                root.add(node)
            }
            success(root)
        } catch (e: Exception) {
            LOGGER.fine("Error when getting distinct values: " + e.message)
            LOGGER.log(Level.FINEST, "Stack trace:", e)
            error("Error: " + e.message)
        } finally {
            conn?.close()
            stmt?.close()
            rs?.close()
        }
    }

    @Throws(JSQLParserException::class, CQLException::class, FilterToSQLException::class)
    private fun getModifiedSql(
        virtualTable: VirtualTable,
        viewParams: String?,
        propertyName: String,
        filter: String?
    ): String {
        var sql = virtualTable.sql
        if (viewParams != null) {
            val decoded = UriUtils.decode(viewParams, "UTF-8")
            val params = decoded.split(";".toRegex())
            for (param in params) {
                val parts = param.split(":".toRegex())
                sql = sql.replace("%" + parts[0] + "%", parts[1])
            }
        }
        // replace default values in case they were not included in the request
        virtualTable.parameterNames.forEach {
            param -> sql = sql.replace("%$param%", virtualTable.getParameter(param).defaultValue)
        }
        val parse = CCJSqlParserUtil.parse(sql)
        val stmt = parse as Select
        val select = stmt.selectBody as PlainSelect
        var selectItems = select.selectItems
        selectItems = selectItems.stream().filter { item: SelectItem ->
            val expressionItem = item as SelectExpressionItem
            val expression = expressionItem.expression
            val func = Function()
            func.name = "DISTINCT"
            val list = ExpressionList()
            list.expressions = listOf(expression)
            func.parameters = list
            expressionItem.expression = func
            if (expressionItem.alias != null) {
                return@filter expressionItem.alias.name.equals(propertyName, ignoreCase = true)
            } else {
                return@filter expressionItem.toString().equals(propertyName, ignoreCase = true)
            }
        }.collect(Collectors.toList())
        select.selectItems = selectItems
        if (filter != null) {
            val decoded = UriUtils.decode(filter, "UTF-8")
            val parsedFilter = ECQL.toFilter(decoded)
            val where = PostGISDialect(null).createFilterToSQL().encodeToString(parsedFilter)
            val expression = CCJSqlParserUtil.parseCondExpression(where.substring("WHERE".length))
            val oldWhere = select.where
            val and = AndExpression(oldWhere, expression)
            select.where = and
        }
        return select.toString()
    }

    @Throws(JsonProcessingException::class)
    fun error(msg: String): StringRawData {
        val returnMap: MutableMap<String, Any> = HashMap(2)
        returnMap["message"] = msg
        returnMap["success"] = false
        return StringRawData(objectMapper.writeValueAsString(returnMap), "application/json")
    }

    @Throws(JsonProcessingException::class)
    fun success(dataset: JsonNode?): StringRawData {
        return StringRawData(objectMapper.writeValueAsString(dataset), "application/json")
    }

    companion object {
        private val LOGGER = Logging.getLogger(DistinctValues::class.java)
    }

}
