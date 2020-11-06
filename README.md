### Arrow extension for WarpScript

WarpScript functions for conversions from and to Apache Arrow [columnar format](https://arrow.apache.org/docs/format/Columnar.html).


#### From WarpScript to Arrow

<pre>
->ARROW    // Encode input into Arrow columnar format (BYTES).
</pre>

The supported input types are described in the conversion table below.
The output is a BYTES representation of an Arrow table, containing a map of metadata and columns.
The arrow metadata will contain the fields *WarpScriptConversionMode*, *WarpScriptVersion* and *WarpScriptTimeUnitsPerSecond*.

| Input description | Output description | Metadata description | WarpScriptConversionMode |
|-------------------|--------------------|----------------------|----------------------|
| List of GTS encoders or GTS | one column for classname, one per label key, one for timestamp, one for latitude, one for longitude, one for elevation, one per value type | no additional output metadata | ENCODERS |
| A GTS | one column for timestamp, one for latitude, one for longitude, one for elevation, one for value | output metadata includes full GTS metadata | GTS |
| A pair list containing a map of metadata and a map of lists of equal size | one column per list | the input map of metadata is the output metadata | PAIR |
| A WarpFrame (if using WarpFrame extension) | one column for timestamp, one column per GTS | each column is named with GTS classname and has GTS label metadata | WARPFRAME |

Empty columns are not encoded.

#### From Arrow to WarpScript

<pre>
ARROW->    // Decode an Arrow stream (BYTES).
</pre>

The function will try to infer the type of the result using the value of the metadata *WarpScriptConversionMode*, based on the conversion table above.
If the input has no *WarpScriptConversionMode*, it will use the default WarpScriptConversionMode PAIR.

### NOTE

Streaming Arrow format from memory or memory mapped files is not available yet. Contact us if you need this feature.
SHM extension can be used as a workaround.

We made a blog article to present this extension: https://blog.senx.io/conversions-to-apache-arrow-format/


#### Build notes

Depends on rev >= 2.2.0

