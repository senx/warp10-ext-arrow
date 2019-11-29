### NOTICE

We made a blog article to present this extension: https://blog.senx.io/conversions-to-apache-arrow-format/

This extension is still in beta and subject to modifications.

### Arrow extension for WarpScript

WarpScript functions for conversions from and to Apache Arrow [streaming format](https://arrow.apache.org/docs/ipc.html).

Arrow format is tabular and is composed of a set of fields (i.e. named columns). It can also be associated with a map of metadata.

<pre>
->ARROW    // Encode input into Arrow streaming format (BYTES).
</pre>

Input:
 * a LIST containing either GTS or GTSENCODER instances.

The output has at least a `classname` field, and a field per existing GTS label key and attribute. Then, depending if they would not be empty it can have a `timestamp` field, a `latitude` field, a `longitude` field, an `elevation` field, and one field per value type: `LONG`, `DOUBLE`, `BOOLEAN`, `STRING`, `BYTES`.

The classnames and labels are repeated on each row with data from the same input GTS (but they are dictionary-encoded so it is memory efficient).

Data points from different input GTS or GTSENCODER that are of the same type are in the same column. They can be identified by the `classname` and label related fields.

Other supported input:
 * a GTS,
 * a GTSENCODER,
 * a LIST of two items: custom metadata (a MAP), and field vectors (a MAP of LIST of same size).

If the input is a single GTS or a single GTSENCODER, there is no field for the classname, the labels and the attributes, but they can be found in the Arrow metadata.

<pre>
ARROW->    // Decode an Arrow stream (BYTES).
</pre>

Suported output is decided by the value of the metadata *WarpScriptType*:
 * *GTS*: a GTS,
 * *GTSENCODER*: a GTSENCODER,
 * *LIST* (default): a LIST of two items: custom metadata (a MAP), and field vectors (a MAP of LIST of same size).

#### Build notes

Depends on rev >= 2.2.0

