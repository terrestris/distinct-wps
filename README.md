## Get distinct property values ##

A WPS for GeoServer that can be used to retrieve distinct property values directly
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

## Installation in GeoServer Cloud

GeoServer Cloud is distributed as Docker images with a pre-configured set of extensions.
In order to install an additional extension, you need to mount the required resources, such
as jar files, in the running containers and instruct the java launcher to use them.

In docker-compose it'd be as easy as bind-mounting the WPS `myWPS.jar`
on the wps container's `/opt/app/bin/BOOT-INF/lib/myWPS.jar`, but trying
to do the same in Kubernetes would override all jars in that folder.

The recommended way is to mount the jar files somewhere else
(for example, in `/opt/app/plugins/`) and set the `JAVA_OPTS` environment variable to
include this location, like in the following example:

```
  wps:
    image: geoservercloud/geoserver-cloud-wps:1.1.0
    environment:
      JAVA_OPTS: "-cp /opt/app/bin:/opt/app/bin/BOOT-INF/lib/*:/opt/app/plugins/myWPS.jar"
    volumes:
      - ./myWPS.jar:/opt/app/plugins/myWPS.jar
```

## Inputs ##

* use `layerName` to specify the qualified feature type name
* use `propertyName` to specify the column to retrieve the values for
* optionally use `filter` to specify a CQL filter in order to add a filter (you need to URI-encode special chars)
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

When using `type` set to `list` results will become a simple array.

## cURL example:
`curl -X POST -H "Content-Type: application/xml" --data-binary @req.xml http://localhost:9090/geoserver/wps`

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
        <wps:LiteralData>scalerank</wps:LiteralData>
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
