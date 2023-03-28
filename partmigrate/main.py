import argparse
import progressbar
import dataclasses
import sys
import os.path

if __name__ == '__main__':
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from partmigrate.log import Log, JobInfo
from partmigrate.lvm import Lvm
from partmigrate.target import Target
from partmigrate.util import error

@dataclasses.dataclass
class Ctx:
    log: Log = None
    source: Target = None
    dest: Target = None
    chunk_size: int = None
    copy_chunk_size: int = None
    copy_chunks: int = None

def move_chunk(ctx: Ctx, stage: str, src: Target, src_id: int, dst: Target, dst_id: int, allow_partial=True):
    def full_write(dst, offset, buf):
        l = dst.write(offset, buf)
        if l != len(buf):
            error(f'Failed to write {len(buf)} bytes, only {l} written, dev={dst.id()}, offsset={offset}')
        
    def do():
        buf = bytearray(ctx.copy_chunk_size)
        for c in range(ctx.copy_chunks):
            offset = src_id * ctx.chunk_size + c * ctx.copy_chunk_size
            l = src.read(offset, buf)
            if l < ctx.copy_chunk_size:
                if not allow_partial:
                    error(f'Failed to read {len(buf)} bytes, only {l} read, dev={src.id()}, offfset={offset}')
                else:
                    full_write(dst, dst_id * ctx.chunk_size + c * ctx.copy_chunk_size, buf)
                    return

            full_write(dst, dst_id * ctx.chunk_size + c * ctx.copy_chunk_size, buf)

    ctx.log.op(f'{stage} move_chunk {src.id()} {src_id} {dst.id()} {dst_id}', do)

def resize(ctx: Ctx, stage: str, src: Target, chunks: int):
    def do():
        src.resize(chunks * ctx.chunk_size)

    ctx.log.op(f'{stage} resize {src.id()} {chunks}', do)

def run(ctx: Ctx):
    assert ctx.source.supports_source()
    assert ctx.dest.supports_destination()

    log = ctx.log

    # Initialize job info and arguments
    ctx.chunk_size = args.chunk * 1024 * 1024
    ctx.copy_chunk_size = args.copy_chunk * 1024 * 1024

    if ctx.chunk_size % ctx.copy_chunk_size != 0:
        error('Chunk size is not divisible by copy chunk size')
    ctx.copy_chunks = ctx.chunk_size // ctx.copy_chunk_size
    

    job_info = JobInfo(
        source_id=ctx.source.id(),
        dest_id=ctx.dest.id(),
        source_size=ctx.source.size(),
        chunk=ctx.chunk_size,
    )

    if log.job_info is not None:
        job_info.check_match(log.job_info)
    else:
        log.job(job_info)
    job_info = log.job_info

    # The algorithm itself
    # First we copy all data to destination, in reverse order and allocate an extra chunk
    # So the destination will look like this [3 2 1 0 E] (where E is empty spare chunk)
    # This will allow us to shrink & grow both source and destination respectively

    # We can then re-arrange the destionation into correct order, while using the spare chunk to make sure the data is always present (not just in memory)
    chunks = job_info.source_size // ctx.chunk_size
    if job_info.source_size % ctx.chunk_size > 0:
        chunks += 1

    def mk_chunk_iterator(prefix: str, count: int):
        ci = range(count)
        if log.dry_run:
            return ci
        return progressbar.progressbar(ci, prefix=prefix)

    def r(c):
        return chunks - c - 1

    stage = 'move'
    for c in mk_chunk_iterator('Moving ', chunks):
        last = c == (chunks - 1)
        first = c == 0
        resize(ctx, stage, ctx.dest, c + 1)
        move_chunk(ctx, stage, ctx.source, r(c), ctx.dest, c, allow_partial=first)

        if not last:
            resize(ctx, stage, ctx.source, chunks - c - 1)

    stage = 'reorder'
    resize(ctx, stage, ctx.dest, chunks + 1)
    half_chunks = chunks // 2
    spare_chunk = chunks
    for c in mk_chunk_iterator('Re-ordering ', half_chunks):
        move_chunk(ctx, 'reorder-1', ctx.dest, c, ctx.dest, spare_chunk)
        move_chunk(ctx, 'reorder-2', ctx.dest, r(c), ctx.dest, c)
        move_chunk(ctx, 'reorder-3', ctx.dest, spare_chunk, ctx.dest, r(c))
        
    resize(ctx, stage, ctx.dest, chunks)

parser = argparse.ArgumentParser()
parser.add_argument('--chunk', type=int, default=1024, help='Resize step size (MiB)')
parser.add_argument('--copy-chunk', type=int, default=64, help='Copy step size (MiB)')
parser.add_argument('--log', default='partmigrate.log', help='Log file for resuming of the operations')
parser.add_argument('--dry-run', '-d', action='store_true', help='Do not perform any operations, write log to stdout')
parser.add_argument('source')
parser.add_argument('destination')

args = parser.parse_args()

ctx = Ctx()
ctx.log = Log()

if args.dry_run:
    ctx.log.start_dry_run()
else:
    ctx.log.open(args.log)

ctx.source: Target = Lvm(ctx.log, args.source)
ctx.dest: Target = Lvm(ctx.log, args.destination)

try:
    run(ctx)
finally:
    ctx.log.close()
    ctx.source.close()
    ctx.dest.close()