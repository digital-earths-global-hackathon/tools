#!/usr/bin/env python

import yaml
import sys
import os.path

def read_config(filename):
    with open(filename) as f:
        return yaml.safe_load(f)


def gen_header(config):
    log_str = get_logging(config)
    template = f"{{\n    order cache before reverse_proxy \n{log_str}\n}}"
    return template


def get_logging(config):
    log_string = """
    log {{
        output file {filename}
        level {level}
        }}\n""".format(**config["logging"])
    return log_string


def gen_server(server: dict, config: dict):
    cache_basedir = config["cache_basedir"]
    server_string = "\nhttp://{server_name}:{server_port} {{\n".format(**server)
    for remote in config["remotes"]:
        cachedir = f"{cache_basedir}/nuts/{remote['remote_name']}_{server['server_name']}"

        server_string += config["template_cache_handle"].format(**server, **remote, cachedir = cachedir)
        os.makedirs(cachedir, exist_ok=True)
    server_string += "\n}\n"
    return server_string


def gen_servers(config):
    servers = "\n".join(gen_server(server, config) for server in config["servers"])
    return servers


if __name__ == "__main__":
    config = read_config(sys.argv[1])
    print(gen_header(config))
    print(gen_servers (config))
