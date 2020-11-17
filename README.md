## Get distinct property values ##

A WPS that can be used to retrieve distinct property values directly
from the DB. Only works for PostGIS based layers and for layers based
directly on a table. The table name and layer name must be identical.

## Inputs ##

* use `layerName` to specify the qualified feature type name
* use `propertyName` to specify the column to retrieve the values for
* optionally use `filter` to specify a CQL filter in order to add a filter

The result will be a JSON array with objects like this:

```json
[{
  "val": "firstValue",
  "dsp": "firstValue"
}, {
  "val": "Second value",
  "dsp": "Second value"
}]
