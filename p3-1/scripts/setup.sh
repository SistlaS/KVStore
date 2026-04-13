#!/bin/bash
set -ex

# rustc & cargo, etc.
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. "$HOME/.cargo/env" 

# uv manager
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

# python 3.12 & fetch deps
cd madkv
uv python install 3.12
uv sync

# add just gpg
wget -qO - 'https://proget.makedeb.org/debian-feeds/prebuilt-mpr.pub' | gpg --dearmor | sudo tee /usr/share/keyrings/prebuilt-mpr-archive-keyring.gpg 1> /dev/null
echo "deb [arch=all,$(dpkg --print-architecture) signed-by=/usr/share/keyrings/prebuilt-mpr-archive-keyring.gpg] https://proget.makedeb.org prebuilt-mpr $(lsb_release -cs)" | sudo tee /etc/apt/sources.list.d/prebuilt-mpr.list

# apt install packages
sudo apt update
sudo apt install -y tree default-jre liblog4j2-java # just
cargo install just --version 1.13.0 --force
export PATH="$HOME/.cargo/bin:$PATH"