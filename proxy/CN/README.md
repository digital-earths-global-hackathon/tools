# Simple proxy setup

This setup uses caddy with souin and badger

Steps (if you don't have docker):

Install go (e.g. `mamba install go`)

Install xcaddy (`go install github.com/caddyserver/xcaddy/cmd/xcaddy@latest`)

Make sure to have the xcaddy in your path (might be in `~/go/bin`)

```
 xcaddy build --with github.com/darkweak/souin/plugins/caddy --with github.com/darkweak/storages/badger/caddy --with github.com/darkweak/storages/nuts/caddy 
 ln -s PATH_TO/Caddyfile .
```

Adjust the `caddy.yaml` file, this should mainly concern the file/dir names at the top, and the names, the proxy should respond to (at the bottom)

Generate a new caddy file

```
python gen_caddyfile.py caddy.yaml > Caddyfile
```

and place it into the directory where you want to start caddy.

```
caddy start
```

on config changes

```
caddy reload
```

Adjust your node's catalog by adding all datasets from the online catalog and replacing the URIs for the files with `http://YOUR_SERVER_NAME:2081` (or whichever port you choose).

Enjoy!