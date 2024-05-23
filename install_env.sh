#!/bin/bash

sudo apt update & wait
sudo apt upgrade -y & wait
sudo apt install python3-pip & wait
sudo pip3 install --upgrade pip3 & wait
sudo pip3 install python-dotenv & wait
sudo pip3 install --no-cache-dir --user -r requirements.txt & wait
sudo pip3 install aiohttp & wait
sudo pip3 install uvloop & wait
sudo pip3 install pytelegrambotapi & wait
sudo pip3 install orjson & wait
sudo pip3 install bravado & wait
sudo pip3 install asyncpg & wait
sudo pip3 install aiopika & wait
sudo pip3 install aio_pika & wait
sudo pip3 install aiodns & wait
sudo pip3 install dydx_v3_python & wait
sudo pip3 install --upgrade requests & wait
