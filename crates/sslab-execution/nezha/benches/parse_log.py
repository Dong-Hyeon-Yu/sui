from re import findall
from glob import glob
import sys

MICRO = "µs"
MILLI = "ms"

def _parse_scheduling(log): 
    tmp = findall(r'ACG construct: (\d+\.\d+)', log)
    construct = [float(t) for t in tmp]
    
    tmp = findall(r'Hierachical sort: (\d+\.\d+)', log)
    sort = [float(t) for t in tmp]
    
    tmp = findall(r'Reorder: (\d+\.\d+)', log)
    reorder = [float(t) for t in tmp]
    
    tmp = findall(r'Extract schedule: (\d+\.\d+)', log)
    extraction = [float(t) for t in tmp]
        
    return construct, sort, reorder, extraction

def _parse_throughput(log):
    tmp = findall(r'thrpt:  \[\d+\.\d+ Kelem/s (\d+\.\d+) Kelem/s \d+\.\d+ Kelem/s\]', log)
    
    return [float(tps) for tps in tmp]
        

def _parse_latency(log):
    tmp = findall(r'Total: \d+\.\d+, Simulation: (\d+\.\d+), Scheduling: \d+\.\d+, Validation: \(execute: \d+\.\d+, validate: \d+\.\d+\), Commit: \d+\.\d+, Other: \d+\.\d+', log)
    simulation = [float(s) for s in tmp]
    
    tmp = findall(r'Total: \d+\.\d+, Simulation: \d+\.\d+, Scheduling: (\d+\.\d+), Validation: \(execute: \d+\.\d+, validate: \d+\.\d+\), Commit: \d+\.\d+, Other: \d+\.\d+', log)
    scheduling = [float(s) for s in tmp]
    
    tmp = findall(r'Total: \d+\.\d+, Simulation: \d+\.\d+, Scheduling: \d+\.\d+, Validation: \(execute: (\d+\.\d+), validate: \d+\.\d+\), Commit: \d+\.\d+, Other: \d+\.\d+', log)
    v_exec = [float(e) for e in tmp]
    
    tmp = findall(r'Total: \d+\.\d+, Simulation: \d+\.\d+, Scheduling: \d+\.\d+, Validation: \(execute: \d+\.\d+, validate: (\d+\.\d+)\), Commit: \d+\.\d+, Other: \d+\.\d+', log)
    v_val = [float(v) for v in tmp]
    
    tmp = findall(r'Total: \d+\.\d+, Simulation: \d+\.\d+, Scheduling: \d+\.\d+, Validation: \(execute: \d+\.\d+, validate: \d+\.\d+\), Commit: (\d+\.\d+), Other: \d+\.\d+', log)
    commit = [float(s) for s in tmp]
    
    tmp = findall(r'Total: \d+\.\d+, Simulation: \d+\.\d+, Scheduling: \d+\.\d+, Validation: \(execute: \d+\.\d+, validate: \d+\.\d+\), Commit: \d+\.\d+, Other: (\d+\.\d+)', log)
    other = [float(s) for s in tmp]
    
    tmp = findall(r'Ktps: (\d+\.\d+)', log)
    ktps = [float(t) for t in tmp]
    
    return ktps, simulation, scheduling, v_exec, v_val, commit, other

def result(log):
    result = ""
    
    if total := _parse_throughput(log):
        result = "[Throughput (ktps)]\n"
        for ktps in total:
            result += f"{ktps} \n"
        
        
    if latency:= _parse_latency(log):
        ktps, simulation, scheduling, v_exec, v_val, commit, other = latency
        result += "\n[Latency (Ktps; simulation (ms); scheduling (ms); validation (ms); commit (ms); other (ms))]\n"
        for k, si, sc, ve, vv, c, o in zip(ktps, simulation, scheduling, v_exec, v_val, commit, other, strict=True):
            result += f"{k} {si} {sc} {ve} {vv} {c} {o}\n"
    
    construct, sort, reorder, _ = _parse_scheduling(log)
    if construct:
        result += "\n[ACG construction]\n"
        for duration in construct:
            result += f"{duration} \n"
        
        result += "\n[Hierarchical sorting]\n"
        for duration in sort:
            result += f"{duration} \n"
            
        result += "\n[Reordering]\n"
        for duration in reorder:
            result += f"{duration} \n"
            
        
    return result

def process(target_file):
    assert isinstance(target_file, str)
    
    for filename in sorted(glob(target_file)):
        log = ""
        with open(filename, 'r') as f:
            log = f.read()
    
        with open(filename.split()[0]+".out", 'a') as f:
            f.write(result(log))
            
if __name__ == "__main__":
    target_file = sys.argv[1]
    process(target_file=target_file)