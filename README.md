### NOTICE

This extension is still in beta and some behaviour may change.

### Arrow extension for WarpScript

WarpScript functions for conversions from and to Apache Arrow [streaming format](https://arrow.apache.org/docs/ipc.html).

Arrow format is tabular and is composed of a set of fields (i.e. named columns). It can also be associated with a map of metadata.

<pre>
->ARROW    // Encode input into Arrow streaming format (BYTES).
</pre>

Input:
 * a LIST containing either GTS or GTSENCODER instances.

The output has at least a `classname` field, and a `timestamp` field. Then depending if they would not be empty, it can also have one field per GTS label or attribute key found in the input data, a `latitude` field, a `longitude` field, an `elevation` field, and one field per value type: `LONG`, `DOUBLE`, `BOOLEAN`, `STRING`, `BYTES`.

Note that data points from different input GTS or GTSENCODER that are of the same type are in the same column. They can be identified by the `classname` and label related fields.


Other supported input:
 * a GTS,
 * a GTSENCODER,
 * a LIST of two items: custom metadata (a MAP), and field vectors (a MAP of LIST of same size).

In these cases, there are no label nor attribute related fields. Instead, labels and attributes if not empty are wrapped as a JSON string and passed as associated metadata (use singleton lists to fall into the previous case if needed).

<pre>
ARROW->    // Decode an Arrow stream (BYTES).
</pre>

Suported output is decided by the value of the metadata *WarpScriptType*:
 * *GTS*: a GTS,
 * *GTSENCODER*: a GTSENCODER,
 * *LIST* (default): a LIST of two items: custom metadata (a MAP), and field vectors (a MAP of LIST of same size).

#### Build notes

Depends on rev >= 2.2.0


### TROUBLESHOOT

After installing from [warpfleet](https://warpfleet.senx.io/), make sure the content of the file [warp10.conf](https://raw.githubusercontent.com/senx/warp10-ext-arrow/master/warp10.conf) is copied to your warp10 configuration folder.
