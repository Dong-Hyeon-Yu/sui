from re import findall
from glob import glob
import sys

MICRO = "µs"
MILLI = "ms"

def _parse_criterion(log):
    tmp = findall(r'time:   \[\d+\.\d+ [µ|m]s (\d+\.\d+) ([µ|m]s) \d+\.\d+ [µ|m]s\]', log)
    
    total = []
    for duration, unit in tmp:
        duration = float(duration)
        if unit == MICRO:
            duration /= 1000
        total.append(duration)
        
    tmp = findall(r'ACG construct: (\d+\.\d+)', log)
    construct = [float(t) for t in tmp]
    
    tmp = findall(r'Hierachical sort: (\d+\.\d+)', log)
    sort = [float(t) for t in tmp]
    
    tmp = findall(r'Reorder: (\d+\.\d+)', log)
    reorder = [float(t) for t in tmp]
    
    tmp = findall(r'Extract schedule: (\d+\.\d+)', log)
    extraction = [float(t) for t in tmp]
        
    return total, construct, sort, reorder, extraction

def result(log):
    
    total, construct, sort, reorder, extraction = _parse_criterion(log)
    
    result = "[total]\n"
    for duration in total:
        result += f"{duration} \n"
        
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
    