# MTXMX - JACK matrix mixer controllable via MQTT

Basic usage:
```
mtxmx --root ROOT_TOPIC --url mqtt://localhost?client_id=mtxmx
```

## Variables used in this manual

* `ROOT_TOPIC` - root topic used in MQTT
* `ENDPOINT_ID` - `inxxx` or `outxxx` where `xxx` is 3-digit zero-padded input or output id, in the range (inclusive) 1..input-endpoints-max or 1..output-endpoints-max (`--*-endpoints-max` are command line arguments)

## MQTT topics:

### to MTXMX

To set the value, append `/set` to the topic, MTXMX will reply retained message without `/set` if the command and argument was valid. Also, when MTXMX receives retained messages for the first time, it will process them even if they don't end with `/set`.

* `ROOT_TOPIC/config/ENDPOINT_ID/gain` - dB gain of the given input or output
* `ROOT_TOPIC/config/ENDPOINT_ID/connect_to` - 2-dimensional array containing names of JACK ports to connect to. The root array contains subarrays. Each subarray corresponds to a single channel and its elements are port names to connect this channel to. Setting it is necessary - MTXMX gets number of channels per endpoint from this value. Specify empty subarrays if you don't want MTXMX to connect ports.
* `ROOT_TOPIC/OUT_ENDPOINT_ID/IN_ENDPOINT_ID/level` - dB gain of this send (matrix point)
* `ROOT_TOPIC/OUT_ENDPOINT_ID/IN_ENDPOINT_ID/state` - `on` or `true` to enable, `off` or `false` to disable this send
* `ROOT_TOPIC/ENDPOINT_ID/state` - enable or mute the whole input or output - `on` or `true` to enable, `off` or `false` to mute


### from MTXMX

* `ROOT_TOPIC/status/sample_rate`
* `ROOT_TOPIC/status/xruns` - JACK xruns occurred since MTXMX start
* `ROOT_TOPIC/status/ENDPOINT_ID/connected_to` - format as in `ENDPOINT_ID/connect_to`, but reporting the ports that MTXMX is actually connected to
* `ROOT_TOPIC/OUT_ENDPOINT_ID/IN_ENDPOINT_ID/meter` - dB signal level (meter)
