### Arrow extension for WarpScript

Provides WarpScript functions for conversion to Apache Arrow [streaming format](https://arrow.apache.org/docs/ipc.html).

Can currently convert GTS and GTSENCODER. May support more WarpScript types if needed.

<pre>
TOARROW    // Encode an object into a byte array in Arrow streaming format.
ARROWTO    // Decode a byte array in Arrow streaming format.
</pre>

#### Build notes

Depends on warpscript version that is on github's repository head of Warp 10. 