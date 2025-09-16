#!/bin/bash
#
# Script to install kubectl command line tool
#
# Usage:
#    install_kubectl.sh [debian|redhat|suse]
#
# Useful Links:
#   Kubectl: https://kubernetes.io/docs/tasks/tools/#kubectl
#

set -euo pipefail

# ---------------------
# ----- Functions -----
# ---------------------
usage() {
  echo "Usage: $0 <distro>"
  echo "  distro - debian, redhat or suse"
  exit 1
}

add_kubernetes_repo() {
  {
    echo "[kubernetes]"
    echo "name=Kubernetes"
    echo "baseurl=https://pkgs.k8s.io/core:/stable:/v1.34/rpm/"
    echo "enabled=1"
    echo "gpgcheck=1"
    echo "gpgkey=https://pkgs.k8s.io/core:/stable:/v1.34/rpm/repodata/repomd.xml.key"
  } > /etc/yum.repos.d/kubernetes.repo
}

install_debian_kubectl() {
  apt-get update
  apt-get install -y apt-transport-https ca-certificates curl gnupg

  if [ ! -d /etc/apt/keyrings ]; then
    mkdir -p /etc/apt/keyrings
    chmod 0755 /etc/apt/keyrings
  fi

  curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.34/deb/Release.key | gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
  chmod 644 /etc/apt/keyrings/kubernetes-apt-keyring.gpg

  echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.34/deb/ /' | tee /etc/apt/sources.list.d/kubernetes.list
  chmod 644 /etc/apt/sources.list.d/kubernetes.list

  apt-get update
  apt-get install -y kubectl
}

install_red_hat_kubectl() {
  add_kubernetes_repo
  dnf install -y kubectl
}

install_suse_kubectl() {
  add_kubernetes_repo

  zypper update
  zypper install -y kubectl
}

install_kubectl() {
  case "$distro" in
    debian)
      install_debian_kubectl
      ;;
    redhat)
      install_red_hat_kubectl
      ;;
    suse)
      install_suse_kubectl
      ;;
    *)
      echo "Unsupported OS: $distro"
      exit 1
      ;;
  esac
}

# ----------------
# ----- Main -----
# ----------------
distro=$1

if [ -z "$distro" ]; then
  echo "Script requires a distro argument (debian, redhat, suse)"
  exit 1
fi

if ! command -v kubectl &> /dev/null; then
  echo "Installing Kubectl on $distro"
  install_kubectl
else
  echo "Kubectl is already installed."
fi
