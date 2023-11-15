# Copyright(C) Facebook, Inc. and its affiliates.
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0
from collections import OrderedDict
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko.rsakey import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
from math import ceil
from copy import deepcopy
import subprocess

from benchmark.config import Committee, NodeParameters, WorkerCache, BenchParameters, ConfigError
from benchmark.utils import BenchError, ExecutionModel, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from benchmark.local_instance import InstanceManager


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningful error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class LANBench:
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path, self.manager.settings.passphrase
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def install(self):
        Print.info('Installing rust and cloning the repo...')
        cmd = [
            'sudo apt-get update',
            'sudo apt-get -y upgrade',
            'sudo apt-get -y autoremove',

            # The following dependencies prevent the error: [error: linker `cc` not found].
            'sudo apt-get -y install build-essential',
            'sudo apt-get -y install cmake',

            # Install rust (non-interactive).
            'sudo apt-get -y install curl',
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            'source $HOME/.cargo/env',
            'rustup default stable',

            # This is missing from the Rocksdb installer (needed for Rocksdb).
            'sudo apt-get install -y clang',
            'sudo apt-get install pkg-config',
            'sudo apt-get install libssl-dev',

            # Install protobuf.
            'sudo apt-get install -y protobuf-compiler',

            # Install tmux
            'sudo apt-get install -y tmux',

            # Clone the repo.
            'sudo apt-get install -y git',
            f'(git clone {self.settings.repo_url} || (cd {self.settings.repo_name} ; git pull))'
        ]
        hosts = self.settings(flat=True)
        try:
            g = Group(*hosts, user=self.settings.user, connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
            Print.heading(f'Initialized testbed of {len(hosts)} nodes')
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to install repo on testbed', e)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts()
        delete_logs = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)']
        try:
            g = Group(*hosts, user=self.settings.user, connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _select_hosts(self, bench_parameters):
        # Collocate the primary and its workers on the same machine.
        if bench_parameters.collocate:
            nodes = max(bench_parameters.nodes)

            # Ensure there are enough hosts.
            hosts = self.manager.hosts()
            if len(hosts) < nodes:
                return []

            return hosts[:nodes]

        # Spawn the primary and each worker on a different machine. Each
        # authority runs in a single data center.
        else:
            nodes = max(bench_parameters.nodes) * (bench_parameters.workers+1)

            # Ensure there are enough hosts.
            hosts = self.manager.hosts()
            if len(hosts) < nodes:
                return []

            return hosts[:nodes]

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user=self.settings.user, connect_kwargs=self.connect)
        c.run(CommandMaker.clean_db(), hide=True)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _update(self, hosts, bench_parameters, include_execution=True):
        if bench_parameters.collocate:
            ips = list(set(hosts))
        else:
            ips = list(set([x for y in hosts for x in y]))

        Print.info(
            f'Updating {len(ips)} machines (branch "{self.settings.branch}")...'
        )
        compile_cmd = ' '.join(CommandMaker.compile())
        cmd = [
            f'(cd {self.settings.repo_name} && git fetch -f)',
            f'(cd {self.settings.repo_name} && git checkout {self.settings.branch})',
            f'(cd {self.settings.repo_name} && git reset --hard origin/{self.settings.branch})',
            # f'(cd {self.settings.repo_name} && git pull -f)',
            'source $HOME/.cargo/env',
            f'(cd {self.settings.repo_name}/crates/sslab-benchmark && {compile_cmd})',  
        ]
        if include_execution:
            compile_cmd = ' '.join(CommandMaker.compile(execution_model=bench_parameters.execution_model, LAN=True))
            cmd += [f'(cd {self.settings.repo_name}/crates/sslab-core && {compile_cmd})']
        else:
            cmd += [f'(cd {self.settings.repo_name}/narwhal/node && {compile_cmd})']

        cmd += [CommandMaker.alias_binaries(
            f'./{self.settings.repo_name}/target/release/', include_execution
        )]
        
        g = Group(*ips, user=self.settings.user, connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)

    def _config(self, hosts, node_parameters, bench_parameters, include_execution=True):
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
        sleep(0.5)  # Removing the store may take time.

        # Create alias for the client and nodes binary.
        cmd = CommandMaker.alias_binaries(PathMaker.binary_path(), include_execution)
        subprocess.run([cmd], shell=True)

        # Generate configuration files.
        primary_names = []
        primary_key_files = [PathMaker.primary_key_file(
            i) for i in range(len(hosts))]
        for filename in primary_key_files:
            cmd = CommandMaker.generate_key(filename).split()
            subprocess.run(cmd, check=True)
            cmd_pk = CommandMaker.get_pub_key(filename).split()
            pk = subprocess.check_output(cmd_pk, encoding='utf-8').strip()
            primary_names += [pk]

        primary_network_names = []
        primary_network_key_files = [PathMaker.primary_network_key_file(
            i) for i in range(len(hosts))]
        for filename in primary_network_key_files:
            cmd = CommandMaker.generate_network_key(filename).split()
            subprocess.run(cmd, check=True)
            cmd_pk = CommandMaker.get_pub_key(filename).split()
            pk = subprocess.check_output(cmd_pk, encoding='utf-8').strip()
            primary_network_names += [pk]

        if bench_parameters.collocate:
            addresses = OrderedDict(
                (x, (y, [z] * (bench_parameters.workers + 1))) for x, y, z in zip(primary_names, primary_network_names, hosts)
            )
        else:
            addresses = OrderedDict(
                (x, (y, z)) for x, y, z in zip(primary_names, primary_network_names, hosts)
            )
        committee = Committee(addresses, self.settings.base_port)
        committee.print(PathMaker.committee_file())

        worker_names = []
        worker_key_files = [PathMaker.worker_key_file(
            i) for i in range(bench_parameters.workers*len(hosts))]
        for filename in worker_key_files:
            cmd = CommandMaker.generate_network_key(filename).split()
            subprocess.run(cmd, check=True)
            cmd_pk = CommandMaker.get_pub_key(filename).split()
            pk = subprocess.check_output(cmd_pk, encoding='utf-8').strip()
            worker_names += [pk]

        if bench_parameters.collocate:
            workers = OrderedDict(
                (x, OrderedDict(
                    (worker_names[i*bench_parameters.workers + y],
                     [h] * (bench_parameters.workers))
                    for y in range(bench_parameters.workers))
                 ) for i, (x, h) in enumerate(zip(primary_names, hosts))
            )
        else:
            workers = OrderedDict(
                (x, OrderedDict(
                    (worker_names[i*bench_parameters.workers + y], h) for y in range(workers))
                 ) for i, (x, h) in enumerate(zip(primary_names, hosts))
            )

        # 2 ports used per authority so add 2 * num authorities to base port
        worker_cache = WorkerCache(
            workers, self.settings.base_port + (2 * len(primary_names)))
        worker_cache.print(PathMaker.workers_file())
        node_parameters.print(PathMaker.parameters_file())

        # Cleanup all nodes and upload configuration files.
        primary_names = primary_names[:len(
            primary_names)-bench_parameters.faults]
        progress = progress_bar(
            primary_names, prefix='Uploading config files:')
        for i, name in enumerate(progress):
            for ip in list(committee.ips(name) | worker_cache.ips(name)):
                c = Connection(ip, user=self.settings.user, connect_kwargs=self.connect)
                c.run(f'{CommandMaker.cleanup()} || true', hide=True)
                c.put(PathMaker.committee_file(), '.')
                c.put(PathMaker.workers_file(), '.')
                c.put(PathMaker.primary_key_file(i), '.')
                c.put(PathMaker.primary_network_key_file(i), '.')
                for j in range(bench_parameters.workers):
                    c.put(PathMaker.worker_key_file(
                        i*bench_parameters.workers + j), '.')
                c.put(PathMaker.parameters_file(), '.')

        return (committee, worker_cache)

    def _run_single(self, rate, committee, worker_cache, bench_parameters, concurrency_level=10, debug=False):
        faults = bench_parameters.faults

        # Kill any potentially unfinished run and delete logs.
        hosts = list(committee.ips() | worker_cache.ips())
        self.kill(hosts=hosts, delete_logs=True)

        # Run the clients (they will wait for the nodes to be ready).
        # Filter all faulty nodes from the client addresses (or they will wait
        # for the faulty nodes to be online).
        Print.info('Booting clients...')
        workers_addresses = worker_cache.workers_addresses(faults)
        rate_share = ceil(rate / worker_cache.workers())
        for i, addresses in enumerate(workers_addresses):
            for (id, address) in addresses:
                host = address.split(':')[1].strip("/")
                cmd = CommandMaker.run_client(
                    address,
                    rate_share,
                    [x for y in workers_addresses for _, x in y]
                )
                log_file = PathMaker.client_log_file(i, id)
                self._background_run(host, cmd, log_file)

        # Run the primaries (except the faulty ones).
        Print.info('Booting primaries...')
        for i, address in enumerate(committee.primary_addresses(faults)):
            host = address.split(':')[1].strip("/")
            cmd = CommandMaker.run_primary(
                PathMaker.primary_key_file(i),
                PathMaker.primary_network_key_file(i),
                PathMaker.worker_key_file(i),
                PathMaker.committee_file(),
                PathMaker.workers_file(),
                PathMaker.db_path(i),
                PathMaker.parameters_file(),
                concurrency_level=concurrency_level,
                debug=debug
            )
            log_file = PathMaker.primary_log_file(i)
            self._background_run(host, cmd, log_file)

        # Run the workers (except the faulty ones).
        Print.info('Booting workers...')
        for i, addresses in enumerate(workers_addresses):
            for (id, address) in addresses:
                host = address.split(':')[1].strip("/")
                cmd = CommandMaker.run_worker(
                    PathMaker.primary_key_file(i),
                    PathMaker.primary_network_key_file(i),
                    PathMaker.worker_key_file(i*bench_parameters.workers + id),
                    PathMaker.committee_file(),
                    PathMaker.workers_file(),
                    PathMaker.db_path(i, id),
                    PathMaker.parameters_file(),
                    id,  # The worker's id.
                    debug=debug
                )
                log_file = PathMaker.worker_log_file(i, id)
                self._background_run(host, cmd, log_file)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(20), prefix=f'Running benchmark ({duration} sec):'):
            sleep(ceil(duration / 20))
        self.kill(hosts=hosts, delete_logs=False)

    def _logs(self, committee, worker_cache, faults):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        workers_addresses = worker_cache.workers_addresses(faults)
        progress = progress_bar(
            workers_addresses, prefix='Downloading workers logs:')
        for i, addresses in enumerate(progress):
            for id, address in addresses:
                host = address.split(':')[1].strip("/")
                c = Connection(host, user=self.settings.user,
                               connect_kwargs=self.connect)
                c.get(
                    PathMaker.client_log_file(i, id),
                    local=PathMaker.client_log_file(i, id)
                )
                c.get(
                    PathMaker.worker_log_file(i, id),
                    local=PathMaker.worker_log_file(i, id)
                )

        primary_addresses = committee.primary_addresses(faults)
        progress = progress_bar(
            primary_addresses, prefix='Downloading primaries logs:')
        for i, address in enumerate(progress):
            host = address.split(':')[1].strip("/")
            c = Connection(host, user=self.settings.user, connect_kwargs=self.connect)
            c.get(
                PathMaker.primary_log_file(i),
                local=PathMaker.primary_log_file(i)
            )

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults)

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False, include_execution=True):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Select which hosts to use.
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        # Update nodes.
        try:
            self._update(selected_hosts, bench_parameters, include_execution)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        # Upload all configuration files.
        try:
            committee, worker_cache = self._config(
                selected_hosts, node_parameters, bench_parameters
            )
        except (subprocess.SubprocessError, GroupException) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to configure nodes', e)

        if bench_parameters.execution_model != ExecutionModel.NEZHA:
            bench_parameters.concurrency_level = [1]
            
        # Run benchmarks.
        for n in bench_parameters.nodes:
            committee_copy = deepcopy(committee)
            committee_copy.remove_nodes(committee.size() - n)

            worker_cache_copy = deepcopy(worker_cache)
            worker_cache_copy.remove_nodes(worker_cache.size() - n)

            for r in bench_parameters.rate:
                for concurrency_level in bench_parameters.concurrency_level:
                    Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s, concurrency level: {concurrency_level})')

                    # Run the benchmark.
                    for i in range(bench_parameters.runs):
                        Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                        try:
                            self._run_single(
                                r, committee_copy, worker_cache_copy, bench_parameters, concurrency_level, debug
                            )

                            faults = bench_parameters.faults
                            logger = self._logs(
                                committee_copy, worker_cache_copy, faults)
                            logger.print(PathMaker.result_file(
                                faults,
                                n,
                                bench_parameters.workers,
                                bench_parameters.collocate,
                                r,
                                270,
                            ))
                        except (subprocess.SubprocessError, GroupException, ParseError) as e:
                            self.kill(hosts=selected_hosts)
                            if isinstance(e, GroupException):
                                e = FabricError(e)
                            Print.error(BenchError('Benchmark failed', e))
                            continue
