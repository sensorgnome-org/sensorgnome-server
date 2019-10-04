# How to renew SSL certificates for sensorgnome.org and *.sensorgnome.org

The sensorgnome.org sever uses free certificates issued by letsencrypt.org
These have to be renewed every 90 days.
This is easy and automatic for the main domain `sensorgnome.org` and is
handled by a cronjob:

```bash
# cat /etc/cron.d/letsencrypt_renewal
0 0,12 * * * root python -c 'import random; import time; time.sleep(random.random() * 3600)' && /usr/bin/certbot renew
```

Unfortunately, the SSL for the wildcard domain `*.sensorgnome.org` has to be renewed
manually every 3 months, while **logged in as `root`** like so:

```bash
# certbot certonly --manual -d '*.sensorgnome.org'
Saving debug log to /var/log/letsencrypt/letsencrypt.log
Plugins selected: Authenticator manual, Installer nginx
Obtaining a new certificate
Performing the following challenges:
dns-01 challenge for sensorgnome.org

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
NOTE: The IP of this machine will be publicly logged as having requested this
certificate. If you're running certbot in manual mode on a machine that is not
your server, please ensure you're okay with that.

Are you OK with your IP being logged?
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
(Y)es/(N)o: Y

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Please deploy a DNS TXT record under the name

_acme-challenge.sensorgnome.org with the following value:

@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  <-- actually some random string

Before continuing, verify the record is deployed.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Press Enter to Continue
```
Then go to the DNS registrar for sensorgnome.org, currently namespro.ca,
and edit the record for `_acme-challenge.sensorgnome.org`, replacing it with
the value given above as `@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@`.

You then **have to wait** for the DNS record to propagate to nameservers, which
takes around 1 hour(!).  You can check it like so:
```bash
$ nslookup
> set type=txt
> _acme-challenge.sensorgnome.org
Server:         slns1.namespro.ca
Address:        67.228.254.4#53

_acme-challenge.sensorgnome.org text = "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
```
If the same random string appears in the nslookup reply as the certbot script
generated, then you can go back to the certbot window and hit Enter.

You'll then see:

```bash
Waiting for verification...
Cleaning up challenges

IMPORTANT NOTES:
 - Congratulations! Your certificate and chain have been saved at:
   /etc/letsencrypt/live/sensorgnome.org/fullchain.pem
   Your key file has been saved at:
   /etc/letsencrypt/live/sensorgnome.org/privkey.pem
   Your cert will expire on 2020-01-01. To obtain a new or tweaked
   version of this certificate in the future, simply run certbot
   again. To non-interactively renew *all* of your certificates, run
   "certbot renew"
 - If you like Certbot, please consider supporting our work by:

   Donating to ISRG / Let's Encrypt:   https://letsencrypt.org/donate
   Donating to EFF:                    https://eff.org/donate-le

```

And note that the message about non-interactively renewing all certificates
does not actually apply to wildcard domains like `*.sensorgnome.org`.

Finally, restart the web server to deploy the renewed certificates:

```bash
# systemctl restart nginx
```

It might of course be easier to purchase commercial SSL certs.
