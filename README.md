# Distributed Sort P2P
An example of a very simple non-fault tolerant, structured 
hierarchical logical ring P2P network using Python.

## Usage
```
usage: peer.py [-h] [-r ROL] LOCAL_IP LOCAL_PORT PEER_IP PEER_PORT

positional arguments:
  LOCAL_IP
  LOCAL_PORT
  PEER_IP
  PEER_PORT

options:
  -h, --help         show this help message and exit
  -r ROL, --rol ROL
```
## Installation
Create a python virtual environment and activate it
```
$ python3 -m venv venv && source ./venv/bin/activate
```
install the requirements
```
$ pip install -r requirements.txt
```
