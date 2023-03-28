import io
import subprocess
import json
import os

from partmigrate.target import Target
from partmigrate.log import Log
from partmigrate.util import error

def call(args):
    p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
    cmd = ' '.join(args)
    if p.returncode != 0:
        print(p.stderr)
        error(f'Command `{cmd}` exited with error code {p.returncode}')
    return p

class Lvm(Target):
    log: Log
    lvm_name: str
    json: dict
    dev_name: str
    dev: any
    def __init__(self, log: Log, lvm_name) -> None:
        self.log = log
        self.lvm_name = lvm_name
        self.json = json.loads(call(['lvs', '--reportformat=json', '-o', 'path', lvm_name]).stdout)
        self.json = self.json['report'][0]['lv'][0]
        self.dev_name = self.json['lv_path']
        self.dev = open(self.dev_name, 'rb+')
        self.log.info(f'Opened LVM device {self.dev_name}')

    def read(self, offset: int, buffer):
        return os.preadv(self.dev.fileno(), [buffer], offset)

    def write(self, offset: int, buffer):
        return os.pwritev(self.dev.fileno(), [buffer], offset)

    def resize(self, new_size: int):
        def run_and_log(args):
            text = ' '.join(args)
            self.log.info(f'Running {text}')
            call(args)

        actual_size = self.size()
        if actual_size == new_size:
            return
        elif actual_size > new_size:
            run_and_log(['lvreduce', '-q', '-f', '-L', str(new_size)+'b', self.lvm_name])
        else:
            run_and_log(['lvextend', '-q', '-f', '-L', str(new_size)+'b', self.lvm_name])
            

    def size(self) -> int:
        self.dev.seek(0, io.SEEK_END)
        return self.dev.tell()

    def id(self) -> str:
        return self.dev_name

    def supports_destination(self):
        return True
        
    def supports_source(self):
        return True

    def close(self):
        self.dev.close()