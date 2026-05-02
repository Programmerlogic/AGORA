# AGORA — AWS Free Tier Deployment Guide

Complete step-by-step guide to deploy AGORA on AWS for **$0/month** using Free Tier resources.

---

## Prerequisites

- An [AWS account](https://aws.amazon.com/free/) (credit card required for verification, but nothing is charged within Free Tier)
- [Git Bash](https://git-scm.com/downloads) or WSL on Windows (for running the deploy script)

---

## Step 1: Launch EC2 Instance

1. Go to [EC2 Console](https://console.aws.amazon.com/ec2/)
2. Click **Launch Instance**
3. Configure:

| Setting | Value |
|---|---|
| **Name** | `agora-dashboard` |
| **AMI** | Ubuntu Server 24.04 LTS (Free tier eligible) |
| **Instance type** | `t2.micro` (Free tier eligible) |
| **Key pair** | Create new → `agora-key` → Download `.pem` file |
| **Storage** | 30 GB gp3 (max free tier) |

4. Under **Network settings**, click **Edit** and create a Security Group:

| Type | Port | Source | Description |
|---|---|---|---|
| SSH | 22 | My IP | SSH access |
| Custom TCP | 8501 | 0.0.0.0/0 | Streamlit dashboard |

5. Click **Launch Instance**

---

## Step 2: Allocate Elastic IP (Free)

An Elastic IP gives you a static public IP that won't change when you stop/start the instance.

1. Go to **EC2 → Elastic IPs** → **Allocate Elastic IP address**
2. Click **Allocate**
3. Select the new IP → **Actions → Associate Elastic IP address**
4. Choose your `agora-dashboard` instance → **Associate**

> ⚠️ **Important**: An Elastic IP is free ONLY while associated with a running instance. If you stop the instance, the IP costs ~$0.005/hr. Either release it when not using, or keep the instance running.

---

## Step 3: SSH into EC2 and Run Setup

```bash
# Move the key file to a safe location and set permissions
# On Windows (Git Bash):
chmod 400 ~/Downloads/agora-key.pem

# SSH into the instance
ssh -i ~/Downloads/agora-key.pem ubuntu@<ELASTIC_IP>
```

Once connected:

```bash
# Upload and run the setup script (or paste the commands manually)
# Option A: Copy-paste ec2-setup.sh contents and run
sudo bash << 'EOF'
<paste contents of ec2-setup.sh here>
EOF

# Option B: If you've already uploaded the file
sudo bash /home/ubuntu/agora/deploy/ec2-setup.sh
```

After setup completes, **log out and log back in** so Docker group membership takes effect:

```bash
exit
ssh -i ~/Downloads/agora-key.pem ubuntu@<ELASTIC_IP>
```

Verify Docker works:

```bash
docker --version
free -h  # Should show ~2GB swap
```

---

## Step 4: Deploy from Your Local Machine

From your project directory (Git Bash on Windows):

```bash
cd /d/PROJECTS/AGORA

# Make deploy script executable
chmod +x deploy/deploy.sh

# Run deployment
bash deploy/deploy.sh <ELASTIC_IP> ~/Downloads/agora-key.pem
```

The script will:
1. Upload all application files (~2MB)
2. Upload `agora_transactions.db` (~840MB) — first time only
3. Build the Docker image on EC2
4. Start the container
5. Run a health check

**First-time database upload will take 5-15 minutes** depending on your upload speed. Subsequent deploys skip this step.

---

## Step 5: Access the Dashboard

Open your browser and go to:

```
http://<ELASTIC_IP>:8501
```

You should see the AGORA fraud monitoring dashboard.

---

## Common Operations

### View logs

```bash
ssh -i ~/Downloads/agora-key.pem ubuntu@<ELASTIC_IP>
docker logs -f agora
```

### Restart the app

```bash
ssh -i ~/Downloads/agora-key.pem ubuntu@<ELASTIC_IP>
cd /home/ubuntu/agora
docker compose -f deploy/docker-compose.yml restart
```

### Re-deploy after code changes

```bash
# From your local machine
bash deploy/deploy.sh <ELASTIC_IP> ~/Downloads/agora-key.pem
```

### Stop the app (saves money if not using)

```bash
# Stop the container
ssh -i ~/Downloads/agora-key.pem ubuntu@<ELASTIC_IP> "cd /home/ubuntu/agora && docker compose -f deploy/docker-compose.yml down"

# Or stop the entire EC2 instance (EBS storage still free)
# Do this from AWS Console: EC2 → Instances → Stop instance
# ⚠️ Release the Elastic IP if stopping long-term to avoid charges
```

### Check system resources

```bash
ssh -i ~/Downloads/agora-key.pem ubuntu@<ELASTIC_IP>
free -h          # Memory + swap usage
df -h /          # Disk usage
docker stats     # Container CPU/memory in real-time
```

---

## Free Tier Limits to Watch

| Resource | Limit | How to Check |
|---|---|---|
| EC2 hours | 750 hrs/month (1 instance 24/7 = ~730 hrs ✅) | AWS Billing Dashboard |
| EBS storage | 30 GB | `df -h /` on EC2 |
| Data transfer | 100 GB out/month | AWS Billing Dashboard |
| Elastic IP | Free while attached to running instance | EC2 → Elastic IPs |

> 💡 Set up a [Billing Alarm](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/monitor_estimated_charges_with_cloudwatch.html) at $1 threshold to get notified if you accidentally exceed Free Tier.

---

## Troubleshooting

### App won't start / OOM killed

```bash
# Check Docker logs
docker logs agora

# Check if swap is active
free -h

# If swap is missing, recreate it
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

### Database errors

```bash
# Verify DB file exists and has correct permissions
ls -la /home/ubuntu/agora/data/agora_transactions.db

# Check DB integrity
sqlite3 /home/ubuntu/agora/data/agora_transactions.db "PRAGMA integrity_check;"
```

### Container keeps restarting

```bash
# Check logs for the error
docker logs --tail 50 agora

# Common fix: rebuild
cd /home/ubuntu/agora
docker compose -f deploy/docker-compose.yml down
docker compose -f deploy/docker-compose.yml up -d --build
```

### Can't connect to port 8501

1. Check Security Group allows port 8501 from `0.0.0.0/0`
2. Check container is running: `docker ps`
3. Check Streamlit is listening: `docker exec agora ss -tlnp`
