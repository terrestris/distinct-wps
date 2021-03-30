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

## Download ##

Download the latest version from [here](https://nexus.terrestris.de/#browse/browse:public:de%2Fterrestris%2Fgeoserver%2Fwps%2Fdistinct-wps).

## Inputs ##

* use `layerName` to specify the qualified feature type name
* use `propertyName` to specify the column to retrieve the values for
* optionally use `filter` to specify a CQL filter in order to add a filter (you need to URI-encode special chars)
* optionally use `viewParams` to specify filter values for SQL based feature types
* optionally use `addQuotes` to enforce the enclosure of the `val` values in single quotes (if false or missing no quotes are added)
* optionally use `limit` to limit your results
* optionally use `order` to order your results `ASC`ending or `DESC`ending
* optionally use `type` to control the output format. Supported types currently are `ext` (the default) or a plain `list`

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

When using `type` set to `list` results will become a simple array.

## cURL example:
`curl -X POST -F 'file=@req.xml' 'http://localhost:8080/geoserver/ows'`

Contents of `req.xml`:
```
<?xml version="1.0" encoding="UTF-8"?><wps:Execute version="1.0.0" service="WPS" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.opengis.net/wps/1.0.0" xmlns:wfs="http://www.opengis.net/wfs" xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:gml="http://www.opengis.net/gml" xmlns:ogc="http://www.opengis.net/ogc" xmlns:wcs="http://www.opengis.net/wcs/1.1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsAll.xsd">
  <ows:Identifier>gs:DistinctValues</ows:Identifier>
  <wps:DataInputs>
    <wps:Input>
      <ows:Identifier>layerName</ows:Identifier>
      <wps:Data>
        <wps:LiteralData>namespace:mylayer</wps:LiteralData>
      </wps:Data>
    </wps:Input>
    <wps:Input>
      <ows:Identifier>propertyName</ows:Identifier>
      <wps:Data>
        <wps:LiteralData>population</wps:LiteralData>
      </wps:Data>
    </wps:Input>
    <wps:Input>
      <ows:Identifier>filter</ows:Identifier>
      <wps:Data>
        <wps:LiteralData>geometrie IS NOT NULL</wps:LiteralData>
      </wps:Data>
    </wps:Input>
    <wps:Input>
      <ows:Identifier>limit</ows:Identifier>
      <wps:Data>
        <wps:LiteralData>5</wps:LiteralData>
      </wps:Data>
    </wps:Input>
    <wps:Input>
      <ows:Identifier>order</ows:Identifier>
      <wps:Data>
        <wps:LiteralData>DESC</wps:LiteralData>
      </wps:Data>
    </wps:Input>
    <wps:Input>
      <ows:Identifier>type</ows:Identifier>
      <wps:Data>
        <wps:LiteralData>list</wps:LiteralData>
      </wps:Data>
    </wps:Input>
  </wps:DataInputs>
  <wps:ResponseForm>
    <wps:RawDataOutput mimeType="application/octet-stream">
      <ows:Identifier>result</ows:Identifier>
    </wps:RawDataOutput>
  </wps:ResponseForm>
</wps:Execute>
```
