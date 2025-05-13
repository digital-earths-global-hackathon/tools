# Simple proxy setup

This setup uses caddy with souin and badger

Steps (if you don't have docker):
Install go (e.g. `mamba install go`)

Install xcaddy (`go install github.com/caddyserver/xcaddy/cmd/xcaddy@latest`)

Make sure to have the xcaddy in your path (might be in `~/go/bin/xcaddy`)

```
xcaddy build --with github.com/darkweak/souin/plugins/caddy --with github.com/darkweak/storages/badger/caddy
ln -s PATH_TO/Caddyfile .

Adjust the dockerfile to have the "official" name of your server in the first line.

```
./caddy start
```

on config changes
```
caddy reload
```