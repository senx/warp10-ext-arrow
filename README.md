### Arrow extension for WarpScript

WarpScript functions for conversions from and to Apache Arrow [streaming format](https://arrow.apache.org/docs/ipc.html).

<pre>
->ARROW    // Encode input into Arrow streaming format (BYTES).
</pre>

Supported input:
 * a GTS,
 * a GTSENCODER,
 * a LIST of two items: custom metadata (a MAP), and field vectors (a MAP of LIST of same size),
 * a LIST containing either GTS or GTSENCODER.

In the last case, the arrow schema has an additional column for classnames, and one additional column per label/attribute key. In the other cases, metadata are wrapped into a JSON string of the custom metadata.

<pre>
ARROW->    // Decode an Arrow stream (BYTES).
</pre>

Suported output is decided by the value of the metadata *WarpScriptType*:
 * *GTS*: a GTS,
 * *GTSENCODER*: a GTSENCODER,
 * *LIST* or *NULL*: a LIST of two items: custom metadata (a MAP), and field vectors (a MAP of LIST of same size).

#### Build notes

Depends on rev >= 2.2