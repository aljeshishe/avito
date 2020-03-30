import json
import sys

from main import on_content

with open(sys.argv[1], encoding='utf-8') as fp:
    result = json.dumps(on_content(fp.read()), indent=2)
    print(result)