### Arrow extension for WarpScript

WarpScript functions for conversions from and to Apache Arrow [streaming format](https://arrow.apache.org/docs/ipc.html).

<pre>
->ARROW    // Encode input into Arrow streaming format (BYTES). Input can be a GTS, a GTSENCODER, or a LIST of two items: custom metadata (a MAP), and field vectors (a MAP of LIST of same size).
ARROW->    // Decode an Arrow stream (BYTES) into one of the three input types of ->ARROW.
</pre>

#### Build notes

Depends on warpscript version that is on github's repository head of Warp 10.