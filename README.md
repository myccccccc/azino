[![ci](https://github.com/myccccccc/azino/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/myccccccc/azino/actions/workflows/build.yml)

---
# What is Azino?

# How to set up your dev environment?
```bash
git clone --recurse-submodules https://github.com/myccccccc/azino.git
cd azino
docker build -t azino . --network=host
docker run -itd  --name test --mount type=bind,source=../azino,target=/root/azino -p 22222:22 -P azino /bin/bash
ssh root@localhost -p 22222
```
