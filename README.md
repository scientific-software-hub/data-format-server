# README #

[![Download](https://img.shields.io/github/release/hzg-wpi/data-format-server.svg?style=flat)](https://github.com/hzg-wpi/data-format-server/releases/latest)


This project is a part of [X-Environment](https://github.com/waltz-controls/xenv)  (Integrated Control System for High-throughput Tomography experiments). This Java Tango server uses [libpniio-jni](https://github.com/hzg-wpi/libpniio-jni) to write data into a NeXus file.

## Requirements ##

Due to libpniio-jni limitation this server can be run only on Debian wheezy. All dependencies of the cpp part of jni must be installed.


Have fun!

`docker run --rm --net host -e TANGO_HOST=localhost:10000 -v /tmp:/mnt -v /opt/xenv/hq/etc/DataFormatServer:/app/etc:ro -d hzhereon/data-format-server:5.1`
