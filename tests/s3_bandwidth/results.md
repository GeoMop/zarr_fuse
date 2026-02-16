# Theoretical and practical bandwidth for CESNET

## CESNET Ceph CL4 info

CL4 (Brno)

Ceph-based object storage

57 storage servers

24 HDDs per server

Total raw capacity: 26.607 PB

Access layer: 5 front-end servers + 9 application servers

Theoretical max bandwidth per client 1-2 Gbps


## Brno - Charon network bandwidth

Skirit (Brno) - Charon (Liberec) Bandwidth
(BOOKWORM)jan_brezina@skirit:~/lbc/workspace/zarr_test$ bash fs_bandwidth.sh .

write to charon, 16 x 64MB
1073741824 bytes (1,1 GB, 1,0 GiB) copied, 18,6463 s, 57,6 MB/s

read from charon 16x64MB
1073741824 bytes (1,1 GB, 1,0 GiB) copied, 15,4808 s, 69,4 MB/s

## S3 access from Charon, Liberec

Charon - CESNET (Liberec, 1GBit

Endpoint: https://s3.cl4.du.cesnet.cz
Bucket:   test-zarr-storage
Key:      bwtest/38c648e951bf461bad38f473ba22758e.bin
Size:     1073741824 bytes (1 GiB)

UPLOAD:   26.474 s  -> 38.7 MiB/s (0.32 Gbit/s)
DOWNLOAD: 7.460 s  -> 137.3 MiB/s (1.15 Gbit/s)

UPLOAD:   25.947 s  -> 39.5 MiB/s (0.33 Gbit/s)
DOWNLOAD: 7.133 s  -> 143.6 MiB/s (1.20 Gbit/s)


## S3 access from Skirit, Brno

Endpoint: https://s3.cl4.du.cesnet.cz
Bucket:   test-zarr-storage
Key:      bwtest/d20d78d0be4a4520b1e4e9a94cde1cff.bin
Size:     1073741824 bytes (1 GiB)

UPLOAD:   23.865 s  -> 42.9 MiB/s (0.36 Gbit/s)
DOWNLOAD: 5.664 s  -> 180.8 MiB/s (1.52 Gbit/s)

UPLOAD:   24.012 s  -> 42.6 MiB/s (0.36 Gbit/s)
DOWNLOAD: 5.478 s  -> 186.9 MiB/s (1.57 Gbit/s)

## Conclusions

- CESNET [documents](https://docs.du.cesnet.cz/en/docs/object-storage-s3/s5cmd?utm_source=chatgpt.com) peak bandwidth for 1-2Gps netwok connections using `s5cmd` util
- The network itself is capable of such bandwidth (Charon - Brno)
- For read we are close to the limit, which in fact is very close to the limit of mechanical HDDs.
- For write there is probably room for improvement up to 4-5 times.
- Storage has 5 frontends so it probably can handle multiple parallel writes. 

## TODO:
- Measure serial zarr and zarr-fuse R/W bandwidth.
- Measure real parallel read, parallel write bandwidth.
