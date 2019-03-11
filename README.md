# SensorgnomeServer #

(C) 2019 John Brzustowski

License: GPL2 or greater.

A server for the [sensorgnome project](https://sensorgnome.org) in literate go.
This file uses the [Literate Markdown Tangle](https://github.com/driusan/lmt) technique:
code blocks marked with triple back-ticks are extracted into source files using `lmt`
and these are compiled into an executable.

## Intro ##
This server manages a growing set of networked sensorgnome receivers.

## Functions ##
- [register sensorgnomes](# register)
- manage messages from SGs (i.e. store and/or forward)
- provide SG status to clients
- manage sync of SGs to motus.org (i.e. download and process raw data)
- manage remote access to SGs (i.e. let users interact directly with an SG)
- allow sensorgnomes from trusted IP addresses to self-register
- allow sensorgnomes from untrusted IP addresses to self-register if they
  provide credentials; e.g. from motus.org and/or sensorgnome.org

### Message Channels ###

Messages arrive on these channels:

- signed datagrams sent to a public UDP port (59022)
   - the signature proves the message originated from the specified sensorgnome
   - the SG uses its server-issued public/private key pair to sign

- unsigned datagrams sent to a local UDP port (59023); the datagrams are sent from
  an SG local port mapped through ssh to the server's port 59023

- streams sent to a local TCP port (59024); streams come from an SG via ssh.
  On the SG, we'd be doing:
```
    ssh -f -N -L 59024:localhost:59024 sg_remote@sensorgnome.org
```
  to map local port 59024 to server port 59024, and then output on the SG would
  use `cat /etc/hostname - | nc -u localhost 59024` instead of `ssh ...`; this
  sends the serial number as the first line, then copies all output from the
  sensorgnome's `uploader.js` to the server socket

- the factory ssh keys used by SGs to login before registering connect to a
  local unix domain port dedicated to registration
  Protocol:
    SG>  SERNO[,name,password] (12-character serno followed by optional name, password)
    SRV> FAILED (if serial number not valid)
    SRV> FAILED (if serial number already registered and name,password not valid credentials)
    SRV> PORT\nPUBKEY\nPRIVKEY (otherwise); these might be new credentials or existing ones

Note that some BBKs are running a special release that allows them to send signed datagrams,
but unfortunately we hardcoded the host address to 131.162.131.200 in uploader.js
