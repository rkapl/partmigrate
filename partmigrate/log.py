import io
import dataclasses
import json

from partmigrate import util

@dataclasses.dataclass(eq=True)
class JobInfo():
    source_id: str
    dest_id: str
    source_size: int
    chunk: int

    def check_item(self, name, a, b):
        if a != b:
            util.error(f'Job mismatch, parameter {name} does not match, {a} != {b}')

    def check_match(self, other: 'JobInfo'):
        self.check_item('source', self.source_id, other.source_id)
        self.check_item('dest', self.dest_id, other.dest_id)
        self.check_item('chunk', self.chunk, other.chunk)

class Log():
    def __init__(self) -> None:
        pass

    def open(self, log_path):
        self.replaying = True
        self.dry_run = False
        self.job_info = None

        self.f = open(log_path, 'a+t')
        self.f.seek(0, io.SEEK_SET)
        
        line_type, line = self.get_non_info()

        if line_type is None:
            self.job_info = None
            self.replaying = False
            self.info('Partmigrate starting')
        else:
            if line_type != 'JOB':
                util.error(f'Did not expect {line_type} in the log file, expecting either INFO or JOB')

            job_info = json.loads(line)
            self.job_info = JobInfo(**{f.name:job_info[f.name] for f in dataclasses.fields(JobInfo)})
            self.replaying = True
            

    def get_non_info(self):
        assert self.replaying
        line = 'INFO Start'
        while line.startswith('INFO ') and len(line) > 0:
            line  = self.f.readline().strip()

        if len(line) == 0:
            return None, None
        else:
            return tuple(line.split(' ', maxsplit=1))
        
    def start_dry_run(self):
        self.replaying = False
        self.dry_run = True
        self.job_info = None

    def job(self, job_info: JobInfo):
        js = json.dumps(dataclasses.asdict(job_info))
        self.job_info = job_info
        if self.dry_run:
            print(js)
        else:
            self.f.write(f'JOB {js}\n')

    def info(self, text):
        if self.dry_run:
            print(text)
        else:
            if not self.replaying:
                self.f.write(f'INFO {text}\n')

    def op(self, op_id, op_cb):
        if self.dry_run:
            print(f'OP {op_id}')
        else:
            if self.replaying:
                line_type, line = self.get_non_info()
                if line is None:
                    # Ran out of replay, run the action
                    self.replaying = False
                    op_cb()
                    self.f.write(f'OP {op_id}\n')
                else:
                    if line_type != 'OP' or line != op_id:
                        util.error(f'Mismatched operation in log OP {op_id} != {line_type} {line}\n')
                    # Replay ok
            else:
                # Run the action
                op_cb()
                self.f.write(f'OP {op_id}\n')

    def close(self):
        if not self.dry_run:
            self.f.close()