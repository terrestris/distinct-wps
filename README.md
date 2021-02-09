## Get distinct property values ##

A WPS that can be used to retrieve distinct property values directly
from the DB. This works for PostGIS based layers and for layers based
directly on a table. The table name and layer name must be identical,
or the layer must be based on a custom SQL statement.

You can specify filters with the `filters` parameter. If the layer is
based on a SQL you can specify viewParams with the `viewParams` parameter.

Please note that parsing/manipulating the SQL might fail. If so, have
a look at the Geoserver log files to see what might be the problem
(for example, `start` is not a reserved word in Postgres, but parsing
it will fail if not quoted with double quotes).

## Inputs ##

* use `layerName` to specify the qualified feature type name
* use `propertyName` to specify the column to retrieve the values for
* optionally use `filter` to specify a CQL filter in order to add a filter
* optionally use `viewParams` to specify filter values for SQL based feature types
* optionally use `addQuotes` to enforce the enclosure of the `val` values in single quotes (if false or missing no quotes are added)

The result will be a JSON array with objects like this:

```json
[{
  "val": "firstValue",
  "dsp": "firstValue"
}, {
  "val": "Second value",
  "dsp": "Second value"
}]
```

With `addQuotes` set to true you will get:

```json
[{
  "val": "'firstValue'",
  "dsp": "firstValue"
}, {
  "val": "'Second value'",
  "dsp": "Second value"
}]
```
