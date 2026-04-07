import json
from collections import defaultdict

log_file = r"D:\Tolle Projekte\Integrated-S3\logs\Logs-2026-03-22 02_08_37.json"

with open(log_file, 'r', encoding='utf-8') as f:
    logs = json.load(f)

# Collect errors
errors_by_type = defaultdict(list)

for entry in logs:
    if 'fields' in entry and 'exception_message' in entry['fields']:
        msg = entry['fields'].get('exception_message', 'Unknown')
        stack = entry['fields'].get('exception_stacktrace', '')
        timestamp = entry.get('date', 'Unknown')
        request_path = entry['fields'].get('RequestPath', 'Unknown')
        
        errors_by_type[msg].append({
            'timestamp': timestamp,
            'path': request_path,
            'stacktrace': stack[:1000] if len(stack) > 1000 else stack
        })

print(f"TOTAL ERRORS FOUND: {sum(len(v) for v in errors_by_type.values())}\n")
print("="*70)
print("ERROR SUMMARY BY TYPE:\n")

for msg, instances in sorted(errors_by_type.items(), key=lambda x: -len(x[1])):
    print(f"[{len(instances)} occurrences] {msg}")
    print(f"  First: {instances[0]['timestamp']}")
    print(f"  Last:  {instances[-1]['timestamp']}")
    if instances[0]['path'] != 'Unknown':
        print(f"  Sample path: {instances[0]['path'][:120]}")
    print(f"  Stack: {instances[0]['stacktrace'][:200]}")
    print()
