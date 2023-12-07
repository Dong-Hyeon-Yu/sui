# Copyright(C) Facebook, Inc. and its affiliates.
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0
from re import findall, search, split
import matplotlib.pyplot as plt
import matplotlib.ticker as tick
from glob import glob
from itertools import cycle

from benchmark.utils import ExecutionModel, PathMaker
from benchmark.config import PlotParameters
from benchmark.aggregate import LogAggregator


@tick.FuncFormatter
def default_major_formatter(x, pos):
    if pos is None:
        return
    # if x >= 1_000:
    #     return f'{x/1000:.0f}k'
    # else:
    return f'{x:.0f}'


@tick.FuncFormatter
def sec_major_formatter(x, pos):
    if pos is None:
        return
    return f'{float(x)/1000:.2f}'

@tick.FuncFormatter
def milisec_major_formatter(x, pos):
    if pos is None:
        return
    return f'{float(x)}'


@tick.FuncFormatter
def mb_major_formatter(x, pos):
    if pos is None:
        return
    return f'{x:,.0f}'


class PlotError(Exception):
    pass


class Ploter:
    def __init__(self, filenames):
        if not filenames:
            raise PlotError('No data to plot')

        self.results = []
        try:
            for filename in filenames:
                with open(filename, 'r') as f:
                    self.results += [f.read().replace(',', '')]
        except OSError as e:
            raise PlotError(f'Failed to load log files: {e}')

    def _natural_keys(self, text):
        def try_cast(text): return int(text) if text.isdigit() else text
        return [try_cast(c) for c in split(r'(\d+)', text)]

    def _tps(self, data):
        values = findall(r' TPS: (\d+) \+/- (\d+)', data)
        values = [(int(x), int(y)) for x, y in values]
        return list(zip(*values))

    def _latency(self, data, scale=1):
        values = findall(r' Latency: (\d+) \+/- (\d+)', data)
        values = [(float(x)/scale, float(y)/scale) for x, y in values]
        return list(zip(*values))
    
    def _abort_rate(self, data):
        values = findall(r'Abort Rate: (\d+\.\d+) \+/- (\d+\.\d+)', data)
        values = [(float(x), float(y)) for x, y in values]
        return list(zip(*values))
    
    def _effective_tps(self, data):
        values = findall(r' Effective TPS:: (\d+) \+/- (\d+)', data)
        values = [(int(x), int(y)) for x, y in values]
        return list(zip(*values))

    def _variable(self, data, isFloat=False):
        if isFloat:
            return [float(x) for x in findall(r'Variable value: X=(\d+\.\d+)', data)]
        else:
            return [int(x) for x in findall(r'Variable value: X=(\d+)', data)]

    def _tps2bps(self, x):
        data = self.results[0]
        size = int(search(r'Transaction size: (\d+)', data).group(1))
        return x * size / 10**6

    def _bps2tps(self, x):
        data = self.results[0]
        size = int(search(r'Transaction size: (\d+)', data).group(1))
        return x * 10**6 / size

    def _plot(self, x_label, y_label, y_axis, z_axis, type):
        plt.figure()
        markers = cycle(['o', 'v', 's', 'p', 'D', 'P'])
        self.results.sort(key=self._natural_keys, reverse=(type == 'tps'))
        for result in self.results:
            y_values, y_err = y_axis(result)
            x_values = self._variable(result, isFloat=True) if 'skewness' in type else self._variable(result, isFloat=False)
            if len(y_values) != len(y_err) or len(y_err) != len(x_values):
                print(x_values)
                print(y_values)
                raise PlotError('Unequal number of x, y, and y_err values')

            plt.errorbar(
                x_values, y_values, yerr=y_err, label=z_axis(result),
                linestyle='dotted', marker=next(markers), capsize=3
            )

        plt.legend(loc='lower center', bbox_to_anchor=(0.5, 1), ncol=3)
        plt.xlim(xmin=0)
        plt.ylim(bottom=0)
        plt.xlabel(x_label, fontweight='bold')
        plt.ylabel(y_label[0], fontweight='bold')
        plt.xticks(weight='bold')
        plt.yticks(weight='bold')
        plt.grid()
        ax = plt.gca()
        ax.xaxis.set_major_formatter(default_major_formatter)
        ax.yaxis.set_major_formatter(default_major_formatter)
        if 'latency' in type:
            ax.yaxis.set_major_formatter(milisec_major_formatter)
        if 'skewness' in type:
            ax.xaxis.set_major_formatter(tick.StrMethodFormatter('{x:.1f}'))
        if 'abort_rate' in type:
            ax.yaxis.set_major_formatter(tick.StrMethodFormatter('{x:.1f}'))
        if len(y_label) > 1:
            secaxy = ax.secondary_yaxis(
                'right', functions=(self._tps2bps, self._bps2tps)
            )
            secaxy.set_ylabel(y_label[1])
            secaxy.yaxis.set_major_formatter(mb_major_formatter)

        for x in ['pdf', 'png']:
            plt.savefig(PathMaker.plot_file(type, x), bbox_inches='tight')
            
        plt.cla()

    @staticmethod
    def nodes(data):
        x = search(r'Committee size: (\d+)', data).group(1)
        f = search(r'Faults: (\d+)', data).group(1)
        faults = f'({f} faulty)' if f != '0' else ''
        return f'{x} nodes {faults}'

    @staticmethod
    def workers(data):
        x = search(r'Workers per node: (\d+)', data).group(1)
        f = search(r'Faults: (\d+)', data).group(1)
        faults = f'({f} faulty)' if f != '0' else ''
        return f'{x} workers {faults}'

    @staticmethod
    def max_latency(data):
        x = search(r'Max latency: (\d+)', data).group(1)
        f = search(r'Faults: (\d+)', data).group(1)
        faults = f'({f} faulty)' if f != '0' else ''
        return f'Max latency: {float(x) / 1000:,.1f} s {faults}'
        
    @staticmethod
    def send_rates(data):
        x = search(r'Actual Sending Rate: (\d+)', data).group(1)
        b = search(r'Average Batch size: (\d+)', data).group(1)
        f = search(r'Faults: (\d+)', data).group(1)
        faults = f'({f} faulty)' if f != '0' else ''
        return f'Send rate: {int(x)} tx/s ({b} KB) {faults}'
        
    @staticmethod
    def execution_model(data):
        x = search(r'Execution Model: (\w+)', data).group(1)
        c = search(r'Concurrency level: (\d+)', data).group(1)
        return f'{x} ({c})'

    @classmethod
    def plot_latency(cls, files, scalability):
        assert isinstance(files, list)
        assert all(isinstance(x, str) for x in files)
        z_axis = cls.workers if scalability else cls.nodes
        x_label = 'Throughput (tx/s)'
        y_label = ['Latency (s)']
        ploter = cls(files)
        ploter._plot(x_label, y_label, ploter._latency, z_axis, 'latency')

    @classmethod
    def plot_tps(cls, files, scalability):
        assert isinstance(files, list)
        assert all(isinstance(x, str) for x in files)
        z_axis = cls.max_latency
        x_label = 'Workers per node' if scalability else 'Committee size'
        y_label = ['Throughput (tx/s)', 'Throughput (MB/s)']
        ploter = cls(files)
        ploter._plot(x_label, y_label, ploter._tps, z_axis, 'tps')
        
    @classmethod
    def plot_concurrency(cls, files, skewness=0.0, tps=False, latency=False, abort_rate=False, effective_tps=False):
        assert tps + latency + abort_rate + effective_tps == 1
        assert isinstance(files, list)
        assert all(isinstance(x, str) for x in files)
        z_axis = cls.send_rates
        x_label = 'Concurrency level'
        
        ploter = cls(files)
        if tps:
            y_label = ['Throughput (tx/s)']
            output_filename = f"concurrency-tps-{skewness}"
            plot_ftn = ploter._tps
        elif latency:
            y_label = ['Latency (ms)']
            output_filename = f"concurrency-latency-{skewness}"
            plot_ftn = ploter._latency
        elif abort_rate:
            y_label = ['Abort rate (%)']
            output_filename = f"concurrency-abort_rate-{skewness}"
            plot_ftn = ploter._abort_rate
        elif effective_tps:
            y_label = ['Effective TPS (tx/s)']
            output_filename = f"concurrency-effective_tps-{skewness}"
            plot_ftn = ploter._effective_tps
        
        ploter._plot(x_label, y_label, plot_ftn, z_axis, output_filename)
        
    @classmethod
    def plot_skewness(cls, files, clevel=0, tps=False, latency=False, abort_rate=False):
        assert tps + latency + abort_rate == 1
        assert isinstance(files, list)
        assert all(isinstance(x, str) for x in files)
        z_axis = cls.send_rates
        x_label = 'Skewness'
        y_label = ['Throughput (tx/s)' if tps else 'Latency (ms)' if latency else 'Abort rate (%)']
        ploter = cls(files)
        output_filename = f"skewness-{'tps' if tps else 'latency' if latency else 'abort_rate'}-{clevel}"
        ploter._plot(x_label, y_label, ploter._tps if tps else ploter._latency if latency else ploter._abort_rate, z_axis, output_filename)
        
    @classmethod
    def plot_execution(cls, files, skewness):
        assert isinstance(files, list)
        assert all(isinstance(x, str) for x in files)
        z_axis = cls.execution_model
        x_label = 'Throughput (tx/s)'
        y_label = ['Latency (ms)']
        ploter = cls(files)
        ploter._plot(x_label, y_label, ploter._latency, z_axis, f'execution-scalability-{skewness}')


    @classmethod
    def plot(cls, params_dict):
        try:
            params = PlotParameters(params_dict)
        except PlotError as e:
            raise PlotError('Invalid nodes or bench parameters', e)

        # Aggregate the logs.
        LogAggregator(params.max_latency).print()

        # Make the latency, tps, and robustness graphs.
        for skewness in params.skewness:
            execution_files = []
            for f in params.faults:
                
                for execution_model in params.execution_model:
                    clevels = params.concurrency_level if execution_model == ExecutionModel.NEZHA else [1]
                    for clevel in clevels:
                            execution_files += glob(
                                PathMaker.agg_file(
                                    'execution',
                                    f,
                                    params.nodes[0],
                                    params.workers[0],
                                    params.collocate,
                                    'any',
                                    execution_model,
                                    clevel,
                                    skewness
                                )
                            )
            cls.plot_execution(execution_files, skewness)
            
            for f in params.faults:
                concurrency_files = []
                for rate in params.rate:
                    concurrency_files += glob(
                        PathMaker.agg_file(
                            'concurrency',
                            f,
                            params.nodes[0],
                            params.workers[0],
                            params.collocate,
                            rate,
                            ExecutionModel.NEZHA,
                            'any',
                            skewness
                        )
                    )
            cls.plot_concurrency(concurrency_files, skewness, tps=True)
            cls.plot_concurrency(concurrency_files, skewness, latency=True)
            cls.plot_concurrency(concurrency_files, skewness, abort_rate=True)
            cls.plot_concurrency(concurrency_files, skewness, effective_tps=True)
            
        
        for f in params.faults:
            for clevel in params.concurrency_level:
                skewness_files = []
                for rate in params.rate:                
                    skewness_files += glob(
                        PathMaker.agg_file(
                            'skewness',
                            f,
                            params.nodes[0],
                            params.workers[0],
                            params.collocate,
                            rate,
                            ExecutionModel.NEZHA,
                            clevel,
                            'any',
                        )
                    )
                if skewness_files:
                    cls.plot_skewness(skewness_files, clevel, tps=True)
                    cls.plot_skewness(skewness_files, clevel, latency=True)
                    cls.plot_skewness(skewness_files, clevel, abort_rate=True)
