### Arrow extension for WarpScript

WarpScript functions for conversions from and to Apache Arrow [streaming format](https://arrow.apache.org/docs/ipc.html).

<pre>
->ARROW    // Encode a GTS, a GTSENCODER, a STRING, or BYTES into Arrow streaming format (BYTES).
ARROW->    // Decode an Arrow stream (BYTES). Depending on its input, this function pushes a GTS, a GTSENCODER, or the Arrow metadata (a MAP) followed by the Arrow field vectors (a MAP of LIST).
</pre>

#### Build notes

Depends on warpscript version that is on github's repository head of Warp 10.