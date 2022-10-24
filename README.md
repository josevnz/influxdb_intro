# Influxdb Datasets

A small project/ tutorial I put together while learning how to use Influxdb for data processing.

Please read the [tutorial/README](tutorial/README.md) file for details.

# Developer mode

```shell
python -m venv ~/virtualenv/influxdb_intro
. ~/virtualenv/influxdb_intro/bin/activate
pip install --upgrade pip
pip install --upgrade setuptools
pip install --upgrade build
pip install --upgrade wheel
python setup.py develop
```

And if you want to build a wheel to install somewhere else

```shell
. ~/virtualenv/influxdb_intro/bin/activate
python -m build --wheel .
```
