#bin/bash

# Install snapd
sudo apt update
sudo apt install snapd
sudo snap install --classic certbot
sudo ln -s /snap/bin/certbot /usr/bin/certbot


sudo nano /etc/nginx/sites-available/ehomeho.com
