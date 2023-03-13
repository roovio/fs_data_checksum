# fs_data_checksum

Checksum database for file system directory subtree. Can be used to detect bit rot if run occasionally on a NAS directory.

It has just two modes of operation:

| Mode   | Description |
| ------ | ----------- |
| add    | subtree is scanned for files and their checksums are stored in a DB at the subtree root. Update will not overwrite an already stored checksum  |
| verify | subtree is scanned for files and actual checksum of each file is compared with checksum stored in DB. The outcome of the comparison is then reported  |
