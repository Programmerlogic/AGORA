#!/usr/bin/env bash
# ============================================================
# AGORA — One-time EC2 instance setup (Ubuntu 24.04 / 22.04)
# Run this ONCE after SSH-ing into a fresh t2.micro instance.
# Usage:  chmod +x ec2-setup.sh && sudo ./ec2-setup.sh
# ============================================================
set -euo pipefail

echo "===== AGORA EC2 Setup ====="

# ---- 1. System updates ----
echo "[1/6] Updating system packages..."
apt-get update -y && apt-get upgrade -y

# ---- 2. Install Docker ----
echo "[2/6] Installing Docker..."
apt-get install -y ca-certificates curl gnupg
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
  > /etc/apt/sources.list.d/docker.list

apt-get update -y
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Let the ubuntu user run docker without sudo
usermod -aG docker ubuntu

# ---- 3. Create swap file (critical for t2.micro 1GB RAM) ----
echo "[3/6] Creating 2GB swap file..."
if [ ! -f /swapfile ]; then
    fallocate -l 2G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo "/swapfile none swap sw 0 0" >> /etc/fstab
    echo "Swap created and enabled."
else
    echo "Swap file already exists, skipping."
fi

# ---- 4. Create app directory ----
echo "[4/6] Creating app directory..."
mkdir -p /home/ubuntu/agora/data
chown -R ubuntu:ubuntu /home/ubuntu/agora

# ---- 5. Install systemd service ----
echo "[5/6] Installing systemd service..."
cp /home/ubuntu/agora/deploy/agora.service /etc/systemd/system/agora.service 2>/dev/null || \
cat > /etc/systemd/system/agora.service << 'EOF'
[Unit]
Description=AGORA Fraud Monitoring Dashboard
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/ubuntu/agora
ExecStart=/usr/bin/docker compose up -d --build
ExecStop=/usr/bin/docker compose down
User=ubuntu
Group=docker

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable agora.service

# ---- 6. Summary ----
echo "[6/6] Setup complete!"
echo ""
echo "  Next steps:"
echo "  1. Upload your app files to /home/ubuntu/agora/"
echo "  2. Upload agora_transactions.db to /home/ubuntu/agora/data/"
echo "  3. Create /home/ubuntu/agora/.env with: GROQ_API_KEY=your_key"
echo "  4. Run: cd /home/ubuntu/agora && docker compose up -d --build"
echo "  5. Access: http://<your-elastic-ip>:8501"
echo ""
echo "  Or simply run deploy.sh from your local machine."
