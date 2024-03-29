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

def _parse_effective_throughput(log):
    tmp = findall(r'committed: (\d+\.\d+)', log)
    committed = [float(t) for t in tmp]
    
    return committed

def _parse_total(log):
    tmp = findall(r'time:   \[\d+\.\d+ [µ|m]s (\d+\.\d+) ([µ|m]s) \d+\.\d+ [µ|m]s\]', log)
    
    total = []
    for duration, unit in tmp:
        duration = float(duration)
        if unit == MICRO:
            duration /= 1000
        total.append(duration)
        
    return total
        
def _parse_nezha(log):
    tmp = findall(r'Simulation: (\d+\.\d+)', log)
    simulation = [float(t) for t in tmp]
    
    tmp = findall(r'Scheduling: (\d+\.\d+)', log)
    scheduling = [float(t) for t in tmp]
    
    tmp = findall(r'Validation: (\d+\.\d+)', log)
    validation = [float(t) for t in tmp]
    
    tmp = findall(r'Commit: (\d+\.\d+)', log)
    commit = [float(t) for t in tmp]
    
    return simulation, scheduling, validation, commit

def result(log):
    
    total = _parse_total(log)
    result = "[total]\n"
    for duration in total:
        result += f"{duration} \n"
        
    
    if overall_latency := _parse_nezha(log):
        result += "\n[Overall Latency]\n"
        for simulation, schedule, validation, commit in zip(*overall_latency, strict=True):     
            result += f"{simulation:.6f} {schedule:.6f} {validation:.6f} {commit:.6f}\n"
    
    construct, sort, reorder, extraction = _parse_scheduling(log)
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
            
        result += "\n[Schedule extraction]\n"
        for duration in extraction:
            result += f"{duration} \n"
            
    committed = _parse_effective_throughput(log)
    if committed:
        result += "\n[Committed txn]\n"
        for c in committed:
            result += f"{c:.3f} \n"
        
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
    