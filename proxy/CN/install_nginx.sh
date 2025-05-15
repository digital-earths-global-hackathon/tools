#!/bin/bash

set -evxu

NGINX_VERSION="1.28.0"
INSTALL_DIR="$HOME/nginx"
if [ -d /data2/share/florain ] ; then 
    CACHE_DIR="/data2/share/florain/nginx"
else
    CACHE_DIR="$HOME/nginx_cache"
fi
CONF_DIR="$HOME/nginx_conf"
CONF_FILE="$CONF_DIR/nginx.conf"
COMMON_CONF="$CONF_DIR/proxy_cache_common.conf"

# Prepare directories
mkdir -p "$INSTALL_DIR" "$CACHE_DIR" "$CONF_DIR"

# Download and compile Nginx if not already installed
if [ ! -x "$INSTALL_DIR/sbin/nginx" ]; then
    echo "🔧 Installing Nginx $NGINX_VERSION locally..."

    if [ ! -f nginx-$NGINX_VERSION.tar.gz ]; then
        wget http://nginx.org/download/nginx-$NGINX_VERSION.tar.gz
    fi
    if [ ! -f pcre2-10.45.tar.bz2 ]; then
        wget https://github.com/PCRE2Project/pcre2/releases/download/pcre2-10.45/pcre2-10.45.tar.bz2
        tar xf pcre2-10.45.tar.bz2
    fi

    if [ ! -f openssl-3.5.0.tar.gz ]; then
        wget https://github.com/openssl/openssl/releases/download/openssl-3.5.0/openssl-3.5.0.tar.gz
        tar xf openssl-3.5.0.tar.gz
    fi

    tar xf nginx-$NGINX_VERSION.tar.gz
    cd nginx-$NGINX_VERSION

    ./configure \
        --prefix="$INSTALL_DIR" \
        --with-http_ssl_module \
        --with-http_v2_module \
        --with-http_slice_module \
        --with-http_stub_status_module \
        --with-pcre=$(pwd)/../pcre2-10.45 \
        --with-openssl=$(pwd)/../openssl-3.5.0

    make -j
    make install
    cd ..
    # rm -rf nginx-$NGINX_VERSION*
    cp ${INSTALL_DIR}/conf/mime.types ${CONF_DIR}/mime.types
else
    echo "✅ Nginx already installed at $INSTALL_DIR"
fi



# Write shared proxy/cache config
cat > "$COMMON_CONF" <<EOF
proxy_cache my_cache;
proxy_cache_valid 200 302 30m;
proxy_cache_valid 404 3m;
proxy_cache_use_stale error timeout updating;
proxy_cache_revalidate on;
proxy_cache_min_uses 1;
proxy_ignore_headers Cache-Control Expires;
slice 10m;
proxy_set_header Range \$http_range;
proxy_set_header If-Range \$http_if_range;
proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;
proxy_next_upstream_tries 3;
add_header X-Cache-Status \$upstream_cache_status;
EOF

# Write main nginx.conf
cat > "$CONF_FILE" <<EOF
worker_processes auto;

events {
    worker_connections 2048;
}

http {
    include       mime.types;
    default_type  application/octet-stream;

    sendfile on;

#    aio threads;

    proxy_cache_path $CACHE_DIR levels=1:2 keys_zone=my_cache:500m inactive=24d max_size=2000g use_temp_path=off;

    log_format cache_status '\$remote_addr - \$host "\$request" '
                            'Cache:\$upstream_cache_status '
                            'Status:\$status Size:\$body_bytes_sent';

    access_log $INSTALL_DIR/logs/access.log cache_status;

    server {
        listen 8080;

        location /jasmin/ {
            proxy_pass https://hackathon-o.s3-ext.jc.rl.ac.uk:443/;
            proxy_http_version 1.1;
            include $CONF_DIR/proxy_cache_common.conf;
        }
        location /catalog {
            proxy_pass https://digital-earths-global-hackathon.github.io:443;
            include $CONF_DIR/proxy_cache_common.conf;
        }
        location /dkrz-swift/ {
            proxy_pass https://swift.dkrz.de:443/;
            include $CONF_DIR/proxy_cache_common.conf;
        }
        location /eerie.cloud/ {
            proxy_pass https://eerie.cloud.dkrz.de:443/;
            include $CONF_DIR/proxy_cache_common.conf;
        }
        location /nowake/ {
            proxy_pass https://nowake.nicam.jp:443/;
            include $CONF_DIR/proxy_cache_common.conf;
        }
        location /dkrz-minio/ {
            proxy_pass https://s3.eu-dkrz-1.dkrz.cloud:443/;
            include $CONF_DIR/proxy_cache_common.conf;
        }
        location /nginx_status {
            stub_status;
            access_log off;
            allow 127.0.0.1;
            deny all;
        }
    }
}
EOF

# Start Nginx
echo "🚀 Starting Nginx with shared config and 2TB cache..."
"$INSTALL_DIR/sbin/nginx" -c "$CONF_FILE" -p "$INSTALL_DIR"

echo "✅ Nginx running on http://localhost:8080"
echo "📦 Cache: 2TB persistent, 10MB slices, retries enabled"
echo "🧩 Shared config via: $COMMON_CONF"
echo "📊 Monitor status:"
echo "  curl http://localhost:8080/nginx_status"
echo "🛑 Stop Nginx with:"
echo "  $INSTALL_DIR/sbin/nginx -s stop -c $CONF_FILE -p $INSTALL_DIR"
