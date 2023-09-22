# Copyright(C) Facebook, Inc. and its affiliates.
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0
from json import load, JSONDecodeError


class SettingsError(Exception):
    pass


class Settings:
    def __init__(self, key_name, key_path, passphrase, base_port, user, LAN_hosts, repo_name, repo_url,
                 branch, instance_type, aws_regions):
        inputs_str = [
            key_name, key_path, passphrase, user, repo_name, repo_url, branch, instance_type
        ]
        regions = aws_regions if isinstance(aws_regions, list) else [aws_regions]
        hosts = LAN_hosts if isinstance(LAN_hosts, list) else [LAN_hosts]

        inputs_str += regions
        inputs_str += hosts

        ok = all(isinstance(x, str) for x in inputs_str)
        ok &= isinstance(base_port, int)
        ok &= len(regions) > 0
        ok &= len(hosts) > 0
        if not ok:
            raise SettingsError('Invalid settings types')

        self.key_name = key_name
        self.key_path = key_path
        self.passphrase = passphrase

        self.user = user
        self.hosts = hosts

        self.base_port = base_port

        self.repo_name = repo_name
        self.repo_url = repo_url
        self.branch = branch

        self.instance_type = instance_type
        self.aws_regions = regions

    @classmethod
    def load(cls, filename):
        try:
            with open(filename, 'r') as f:
                data = load(f)

            return cls(
                data['key']['name'],
                data['key']['path'],
                data['key']['passphrase'],
                data['port'],
                data['user'],
                data['LAN']['hosts'],
                data['repo']['name'],
                data['repo']['url'],
                data['repo']['branch'],
                data['instances']['type'],
                data['instances']['regions'],
            )
        except (OSError, JSONDecodeError) as e:
            raise SettingsError(str(e))

        except KeyError as e:
            raise SettingsError(f'Malformed settings: missing key {e}')
