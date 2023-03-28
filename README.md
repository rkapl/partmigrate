# LVM Partition Type migration

Allows offline migration between otherwise unsupported LVM disk types without consuming too much extra
space. E.g. restriping 3-stripes -> 4 stripes.

## Example Usage

Let's assume that we want to migrate existing partition `vg/a` into a new partition `vg/b`
(with different settings).

First create a small partition (e.g. 1 GiB) with the desired parameters (e.g. four stripes).
Partmigrate will eventually expand it to the correct size.

```bash
lvcreate -L 1G -i4 vg-a -n b
```

Make sure your file-systems are unmounted. Then run partmigrate to move the data from `a` to `b`. 
Make sure you have at least two chunks free to grow the `b` partition (by default chunk is 1 GiB).

```bash
python3 partmigrate/main.py vg-sys/a vg-sys/b
```

Finally, verify that you can mount vg-sys/b and `lvremove` `vg-sys/a`.

## Resuming

Partmigrate produces `partmigrate.log`. If partmigrate finds this file, it will resume from the last
finished operation.

## Principle of Operation

The migration is done by shrinking the old partition while simultaneously growing the new one
in small increments (chunks). There are two stages.

In the first stage, in each step:

    1) The new partition is extended by one chunk
    2) The last chunks of the old partition is copied to the just added chunk of the new partition
    3) The old partition is reduced by one chunk

Now the second partition contains the necessary data, but in reverse order. The second stage
fixes this by swapping the chunks.

