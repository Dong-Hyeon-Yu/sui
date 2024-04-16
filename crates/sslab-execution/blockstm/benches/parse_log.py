from re import findall
from glob import glob
import sys


def _parse_throughput(log):
    tmp = findall(r'thrpt:  \[\d+\.\d+ Kelem/s (\d+\.\d+) Kelem/s \d+\.\d+ Kelem/s\]', log)
    
    return [float(tps) for tps in tmp]

def _parse_latency(log):
    tmp = findall(r'execution: (\d+\.?\d*)', log)
    execution = [float(e) for e in tmp]
    
    tmp = findall(r'commit: (\d+\.?\d*)', log)
    commit = [float(c) for c in tmp]
    
    tmp = findall(r'Ktps: (\d+\.?\d*)', log)
    ktps = [float(t) for t in tmp]
        
    return ktps, execution, commit

def result(log):
    
    total = _parse_throughput(log)
    result = "[Throughput (ktps)]\n"
    for ktps in total:
        result += f"{ktps} \n"
        
    if latency:= _parse_latency(log):
        ktps, execution, commit = latency
        result += "\n[latency (Ktps; execution (ms); commit (ms))]\n"
        for k, e, c in zip(ktps, execution, commit, strict=True):
            result += f"{k} {e} {c}\n"
        
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
    