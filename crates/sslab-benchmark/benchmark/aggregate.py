# Copyright(C) Facebook, Inc. and its affiliates.
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0
from re import search
from collections import defaultdict
from statistics import mean, stdev
from glob import glob
from copy import deepcopy
from os.path import join
import os

from benchmark.utils import PathMaker


class Setup:
    def __init__(self, faults, nodes, workers, collocate, rate, tx_size, execution_model, concurrency_level, skewness):
        self.nodes = nodes
        self.workers = workers
        self.collocate = collocate
        self.rate = rate
        self.tx_size = tx_size
        self.faults = faults
        self.execution_model = execution_model
        self.concurrency_level = concurrency_level
        self.skewness = skewness
        self.max_latency = 'any'

    def __str__(self):
        return (
            f' Faults: {self.faults}\n'
            f' Committee size: {self.nodes}\n'
            f' Workers per node: {self.workers}\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {self.rate} tx/s\n'
            f' Skewness: {self.skewness}\n'
            f' Max latency: {self.max_latency} ms\n'
            f' Execution Model: {self.execution_model}\n'
            f' Concurrency level: {self.concurrency_level}\n'
        )

    def __eq__(self, other):
        return isinstance(other, Setup) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    @classmethod
    def from_str(cls, raw):
        try:
            faults = int(search(r'Faults: (\d+)', raw).group(1))
            nodes = int(search(r'Committee size: (\d+)', raw).group(1))
            workers = int(search(r'Worker\(s\) per node: (\d+)', raw).group(1))
            collocate = 'True' == search(
                r'Collocate primary and workers: (True|False)', raw
            ).group(1)
            rate = int(search(r'Input rate: (\d+)', raw).group(1))
            tx_size = int(search(r'Average Transaction size: (\d+)', raw).group(1))
            execution_model = search(r'Execution mode: (\w+)', raw).group(1)
            concurrency_level = int(search(r'Concurrency level: (\d+)', raw).group(1))
            skewness = float(search(r'skewness: (\d+\.\d+)', raw).group(1))
        except AttributeError as e:
            print(raw)
            raise e 
        
        return cls(faults, nodes, workers, collocate, rate, tx_size, execution_model, concurrency_level, skewness)


class Result:
    def __init__(self, mean_tps, mean_latency, mean_send_rate, mean_batch_size, abort_rate, mean_effective_tps,
                 std_tps=0, std_latency=0, std_abort_rate=0, std_effective_tps=0):
        self.mean_tps = mean_tps
        self.mean_latency = mean_latency
        self.mean_send_rate = mean_send_rate
        self.mean_batch_size = mean_batch_size
        self.mean_abort_rate = abort_rate
        self.mean_effective_tps = mean_effective_tps
        self.std_tps = std_tps
        self.std_latency = std_latency
        self.std_abort_rate = std_abort_rate
        self.std_effective_tps = std_effective_tps

    def __str__(self):
        return (
            f' TPS: {self.mean_tps} +/- {self.std_tps} tx/s\n'
            f' Latency: {self.mean_latency} +/- {self.std_latency} ms\n'
            f' Actual Sending Rate: {self.mean_send_rate} tx/s\n'
            f' Average Batch size: {self.mean_batch_size} KB\n'
            f' Abort Rate: {self.mean_abort_rate} +/- {self.std_abort_rate} %\n'
            f' Effective TPS:: {self.mean_effective_tps} +/- {self.std_effective_tps} tx/s\n'
        )

    @classmethod
    def from_str(cls, raw):
        tps = int(search(r'Execution TPS: (\d+)', raw).group(1))
        latency = int(search(r'Execution latency: (\d+)', raw).group(1))
        send_rate = int(search(r'Actual Sending Rate: (\d+)', raw).group(1))
        batch_size = int(search(r'Average Batch size: (\d+)', raw).group(1))
        abort_rate = float(search(r'Abort Rate: (\d+\.\d+)', raw).group(1))
        effective_tps = int(search(r'Effective TPS: (\d+)', raw).group(1))
        return cls(tps, latency, send_rate, batch_size, abort_rate, effective_tps)

    @classmethod
    def aggregate(cls, results):
        if len(results) == 1:
            return results[0]

        mean_tps = round(mean([x.mean_tps for x in results]))
        mean_latency = round(mean([x.mean_latency for x in results]))
        mean_send_rate = round(mean([x.mean_send_rate for x in results]))
        mean_batch_size = round(mean([x.mean_batch_size for x in results]))
        mean_abort_rate = round(mean([x.mean_abort_rate for x in results]), 2)
        mean_effective_tps = round(mean([x.mean_effective_tps for x in results]))
        std_tps = round(stdev([x.mean_tps for x in results]))
        std_latency = round(stdev([x.mean_latency for x in results]))
        std_abort_rate = round(stdev([x.mean_abort_rate for x in results]), 2)
        std_effective_tps = round(stdev([x.mean_effective_tps for x in results]))
        return cls(mean_tps, mean_latency, mean_send_rate, mean_batch_size, mean_abort_rate, mean_effective_tps,
                   std_tps, std_latency, std_abort_rate, std_effective_tps)


class LogAggregator:
    def __init__(self, max_latencies):
        assert isinstance(max_latencies, list)
        assert all(isinstance(x, int) for x in max_latencies)

        self.max_latencies = max_latencies

        data = ''
        for filename in glob(join(PathMaker.results_path(), '*.txt')):
            with open(filename, 'r') as f:
                data += f.read()

        records = defaultdict(list)
        for chunk in data.replace(',', '').split('SUMMARY')[1:]:
            if chunk:
                records[Setup.from_str(chunk)] += [Result.from_str(chunk)]

        self.records = {k: Result.aggregate(v) for k, v in records.items()}

    def print(self):
        if not os.path.exists(PathMaker.plots_path()):
            os.makedirs(PathMaker.plots_path())

        results = [
            self._print_skewness(),
            self._print_concurrency(),
            self._print_execution(),
        ]
        for name, records in results:
            for setup, values in records.items():
                data = '\n'.join(
                    f' Variable value: X={x}\n{y}' for x, y in values
                )
                string = (
                    '\n'
                    '-----------------------------------------\n'
                    ' RESULTS:\n'
                    '-----------------------------------------\n'
                    f'{setup}'
                    '\n'
                    f'{data}'
                    '-----------------------------------------\n'
                )

                filename = PathMaker.agg_file(
                    name,
                    setup.faults,
                    setup.nodes,
                    setup.workers,
                    setup.collocate,
                    setup.rate,
                    setup.execution_model,
                    setup.concurrency_level,
                    setup.skewness,
                )
                with open(filename, 'w') as f:
                    f.write(string)

    def _print_latency(self):
        records = deepcopy(self.records)
        organized = defaultdict(list)
        for setup, result in records.items():
            rate = setup.rate
            setup.rate = 'any'
            organized[setup] += [(result.mean_tps, result, rate)]

        for setup, results in list(organized.items()):
            results.sort(key=lambda x: x[2])
            organized[setup] = [(x, y) for x, y, _ in results]

        return 'latency', organized
    
    def _print_skewness(self):
        records = deepcopy(self.records)
        organized = defaultdict(list)
        for setup, result in records.items():
            skewness = setup.skewness
            setup.skewness = 'any'
            organized[setup] += [(skewness, result)]

        for setup, results in list(organized.items()):
            results.sort(key=lambda x: x[0])
            organized[setup] = [(x, y) for x, y in results]

        return 'skewness', organized

    def _print_concurrency(self):
        records = deepcopy(self.records)
        organized = defaultdict(list)
        for setup, result in records.items():
            clevel = setup.concurrency_level
            setup.concurrency_level = 'any'
            organized[setup] += [(clevel, result)]
            
        for setup, results in list(organized.items()):
            results.sort(key=lambda x: x[0])
            organized[setup] = [(x, y) for x, y in results]
            
        return 'concurrency', organized
    
    def _print_execution(self):
        records = deepcopy(self.records)
        organized = defaultdict(list)
        for setup, result in records.items():
            rate = setup.rate
            setup.rate = 'any'
            organized[setup] += [(result.mean_tps, result, rate)]

        for setup, results in list(organized.items()):
            results.sort(key=lambda x: x[2])
            organized[setup] = [(x, y) for x, y, _ in results]

        return 'execution', organized

    def _print_tps(self, scalability):
        records = deepcopy(self.records)
        organized = defaultdict(list)
        for max_latency in self.max_latencies:
            for setup, result in records.items():
                setup = deepcopy(setup)
                if result.mean_latency <= max_latency:
                    setup.rate = 'any'
                    setup.max_latency = max_latency
                    if scalability:
                        variable = setup.workers
                        setup.workers = 'x'
                    else:
                        variable = setup.nodes
                        setup.nodes = 'x'

                    new_point = all(variable != x[0] for x in organized[setup])
                    highest_tps = False
                    for v, r in organized[setup]:
                        if result.mean_tps > r.mean_tps and variable == v:
                            organized[setup].remove((v, r))
                            highest_tps = True
                    if new_point or highest_tps:
                        organized[setup] += [(variable, result)]

        [v.sort(key=lambda x: x[0]) for v in organized.values()]
        return 'tps', organized
