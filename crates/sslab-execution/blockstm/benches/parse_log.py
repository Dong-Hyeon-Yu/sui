from re import findall
from glob import glob
import sys

MICRO = "µs"
MILLI = "ms"

def _parse_total(log):
    tmp = findall(r'time:   \[\d+\.\d+ [µ|m]s (\d+\.\d+) ([µ|m]s) \d+\.\d+ [µ|m]s\]', log)
    
    total = []
    for duration, unit in tmp:
        duration = float(duration)
        if unit == MICRO:
            duration /= 1000
        total.append(duration)
        
    return total

def _parse_latency(log):
    tmp = findall(r'total: (\d+\.?\d*)', log)
    total = [float(t) for t in tmp]
    
    tmp = findall(r'commit: (\d+\.?\d*)', log)
    commit = [float(c) for c in tmp]
        
    return total, commit

def result(log):
    
    total = _parse_total(log)
    
    result = "[total]\n"
    for duration in total:
        result += f"{duration} \n"
        
    if latency:= _parse_latency(log):
        total, commit = latency
        result += "\n[latency]\n"
        for t, c in zip(total, commit, strict=True):
            result += f"{t} {c}\n"
        
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
    