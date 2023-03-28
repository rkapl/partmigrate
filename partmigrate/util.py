import sys

def error(text):
    print(text, file=sys.stderr)
    sys.exit(1)