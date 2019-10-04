# SensorgnomeServer #

(C) 2019 John Brzustowski

License: GPL2 or greater.

A server for the [sensorgnome project](https://sensorgnome.org).

## Intro ##
This server manages a growing set of networked sensorgnome receivers.

## Functions ##
- [register sensorgnomes](# register)
- manage messages from SGs:
  - [x] store
  - [ ] forward
- [x] provide SG status to clients
- [x] manage sync of SGs to motus.org (i.e. download and process raw data)
- [x] manage remote access to SGs (i.e. let users interact directly with an SG)
- [x] allow sensorgnomes from trusted IP addresses to self-register
- [x] allow sensorgnomes from untrusted IP addresses to self-register if they
  provide credentials; e.g. from motus.org and/or sensorgnome.org
- [ ] allow remotely changing on-board tag database
- [ ] allow remotely changing on-board deployment.txt configuration file

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

### Status Page ###
- this server regenerates a simple markdown page whenever an event triggers it
- the hugo server detects a change to the markdown file and regenerates static html
- page is public and currently served from [new.sensorgnome.org](https://new.sensorgnome.org)

### Status Server ###
- this server listens on port 50025 for TCP connections, and replies to these commands:
  - **who**:  list of `serno,port` for connected receivers
  - **ports**: list of `port` of connected receivers
  - **serno**: list of `serno` of connected receivers
  - **status**: json-formated status of all *active* receivers, connected or not.  *active*
  means connected at least once since the server was launched

### Registration Server ###
- login via ssh to port 59022 with the factory keys forces the command "nc localhost 59026",
which communicates with this server's registration listener on port 59026

## Individual version changes ##

### BBBK 2015-08-27 ###
- e.g. SG-5113BBBK2853 (BSC HQ)
- uploader.js:
  - command is now `nc localhost 59024`
  - `pushStartupInfo()` now sends serno as first line on stream
- maintain_ssh_tunnel: map localhost:59024 to sensorgnome.org:59024
- new file `/etc/network/if-up.d/init_ssh_tunnel` runs `maintain_ssh_tunnel` when
  a non-local, non-usb interface comes up:

### BBBK 2017-03-06 ###

- same changes as above

### Receivers completed ###
- SG-1614BBBK1666
- SG-1614BBBK1807

### Clean up of tunnel vs. streaming ###

- ultimate new setup on server side:
  - TCP port 59022: sshd_sg, as before
  - UDP port 59022: accept signed datagrams from the network, as before but now in same program
  - UDP port 59023: accept unsigned datagrams from localhost (*TBD*).
  - TCP port 59024: accept streams over an ssh (and thus authenticated) connection;
                    the stream begins with the SG's serial number

This way:
- datagrams authenticated by embedded signature, when needed (i.e. sent from arbitrary hosts
  which are not using ssh)
- datagrams authenticated by virtue of arriving over authenticated channel (ssh) and not signed
  (first
- no race conditions between mapping ports and running a program over
  ssh (see https://github.com/jbrzusto/openssh-portable/issues/1)

But this requires retooling SG-side code to include the serno on all messages sent over
the authenticated channel.

### `/home/bone/proj/bonedongle/master/uploader.js` ###
The the child process is now
```
nc -u localhost 59024
```
rather than
```
usr/bin/ssh -T -o ControlMaster=auto", -o ControlPath=/tmp/sgremote", -o ServerAliveInterval=5", -o ServerAliveCountMax=3",-i /home/bone/.ssh/id_dsa", -p 59022", sg_remote@sensorgnome.org", /home/sg_remote/code/sg_remote
```
We also modify the first line of startup-info sent by the uploader to simply be the SG's serial number (e.g. SG-1234BBBK5678)
to identify the stream:

```js
Uploader.prototype.pushStartupInfo = function() {
    var ts = (new Date()).getTime()/1000;
    this.child.stdin.write( "SG-" + Machine.machineID + "\n" + "M," + ts + ",machineID," + Machine.machineID + "\n" +
                            "M," + ts + ",bootCount," + Machine.bootCount + "\n");
};
```
### `/etc/hosts` ###
Change to hardwire new sensorgnome.org address:
```sh
sed -i -e '/sensorgnome.org/d' /etc/hosts
echo 108.63.14.166 sensorgnome.org >> /etc/hosts
sed -i -e '/StrictHostKeyChecking/s/^.*$/StrictHostKeyChecking no/' /etc/ssh/ssh_config
```

### `/home/bone/proj/bonedongle/scripts/maintain_ssh_tunnel`

Drop use of autossh (since we're running from a cronjob anyway) and map streaming port:
```sh
# maintain a reverse tunnel portmap to sensorgnome.org
# run every 5 minutes from /etc/cron.d
#
# map server:TUNNEL_PORT -> localhost:22  (ssh reverse tunnel)
# map localhost:59024 -> server:59024     (message streaming)

TUNNEL_PORT_FILE=/home/bone/.ssh/tunnel_port
UNIQUE_KEY_FILE=/home/bone/.ssh/id_dsa
REMOTE_USER=sg_remote
REMOTE_HOST=sensorgnome.org
REMOTE_SSH_PORT=59022
REMOTE_STREAM_PORT=59024
LOCAL_STREAM_PORT=59024

if [[ -f $TUNNEL_PORT_FILE ]]; then
    read TUNNEL_PORT < $TUNNEL_PORT_FILE
    ssh -f -N -T \
        -L$LOCAL_STREAM_PORT:localhost:$REMOTE_STREAM_PORT \
        -R$TUNNEL_PORT:localhost:22 \
        -o ControlMaster=auto \
        -o ControlPath=/tmp/sgremote \
        -o ServerAliveInterval=5 \
        -o ServerAliveCountMax=3 \
        -i $UNIQUE_KEY_FILE \
        -p $REMOTE_SSH_PORT \
        $REMOTE_USER@$REMOTE_HOST
fi
```

### `/etc/network/if-up.d/init_ssh_tunnel` ###

This file attempts to set up a tunnel as soon a non-local network interface comes up:
```bash
#!/bin/bash
#
# initiate an ssh tunnel without waiting for cron
#
if [[ "$IFACE" != "usb0" && "$IFACE" != "lo" ]]; then
        /home/bone/proj/bonedongle/scripts/maintain_ssh_tunnel
fi
exit 0
```

Also note that some BBKs are running a special release that allows them to send signed datagrams,
but unfortunately we hardcoded the host address to 131.162.131.200 in uploader.js

