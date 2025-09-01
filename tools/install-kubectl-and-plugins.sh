#!/bin/bash
#
# Script to install kubectl and krew plugin manager.
# It also installs some useful kubectl plugins like ctx and ns.
# Usage for plugins:
#   kubectl ctx <context-name> or just kubectl ctx, if you use fzf
#   kubectl ns <namespace-name> or just kubectl ns, if you use fzf
#
# Useful Links:
#   Kubectl: https://kubernetes.io/docs/tasks/tools/#kubectl
#   Krew: https://krew.sigs.k8s.io/docs/user-guide/setup/install/
#   Ctx and ns: https://github.com/ahmetb/kubectx
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

append_once() {
  local line="$1" file="$2"
  grep -qxF "$line" "$file" 2>/dev/null || echo "$line" >> "$file"
}

install_krew() {
  cd "$(mktemp -d)"
  OS="$(uname | tr '[:upper:]' '[:lower:]')"
  ARCH="$(uname -m | sed -e 's/x86_64/amd64/' -e 's/\(arm\)\(64\)\?.*/\1\2/' -e 's/aarch64$/arm64/')"
  KREW="krew-${OS}_${ARCH}"
  curl -fsSLO "https://github.com/kubernetes-sigs/krew/releases/latest/download/${KREW}.tar.gz"
  tar zxvf "${KREW}.tar.gz"
  ./"${KREW}" install krew

  shell="$(basename "$SHELL")"
  profile="$HOME/.bashrc"
  [ "$shell" = "zsh" ] && profile="$HOME/.zshrc"
  append_once "export PATH=\"${KREW_ROOT:-$HOME/.krew}/bin:$PATH\"" "$profile"

  echo "Please restart your shell or run 'source $profile' to use krew, after the installation."
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


if ! kubectl krew &> /dev/null; then
  echo "Installing kubectl plugins manager (krew)"
  install_krew
  export PATH="${KREW_ROOT:-$HOME/.krew}/bin:$PATH"
else
  echo "kubectl plugins manager (krew) is already installed."
fi

echo "Installing kubectl plugins (ctx, ns)"
kubectl krew update || true
for p in ctx ns; do
  if kubectl krew list | grep -qx "$p"; then
    kubectl krew upgrade "$p" || true
  else
    kubectl krew install "$p"
  fi
done
