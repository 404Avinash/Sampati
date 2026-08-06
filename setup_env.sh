#!/usr/bin/env bash
set -e

echo "============================================================"
echo "🚀 Setting up Ultimate AI/ML Python Development Environment"
echo "============================================================"

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CDIR="$PROJECT_DIR"

echo "📍 Directory: $CDIR"

# 1. Check Python version
if command -v python3 &>/dev/null; then
    PY_VER=$(python3 --version)
    echo "✅ Found $PY_VER"
else
    echo "❌ Python3 is not installed!"
    exit 1
fi

# 2. Check for GPU / CUDA
if command -v nvidia-smi &>/dev/null; then
    echo "⚡ NVIDIA GPU Detected:"
    nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader
else
    echo "ℹ️  No NVIDIA GPU detected or nvidia-smi not in PATH (CPU mode will be used)"
fi

# 3. Create virtual environment if not present
VENV_DIR="$CDIR/.venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "📦 Creating virtual environment (.venv)..."
    python3 -m venv "$VENV_DIR"
else
    echo "📦 Virtual environment (.venv) already exists."
fi

# 4. Activate virtual environment
source "$VENV_DIR/bin/activate"

# 5. Upgrade pip and build tools
echo "⬆️  Upgrading pip, setuptools, and wheel..."
pip install --upgrade pip setuptools wheel

# 6. Install PyTorch with CUDA 12.1 / PyTorch wheel repo if available, else PyPI
echo "🔥 Installing PyTorch & PyTorch Vision/Audio..."
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121 || pip install torch torchvision torchaudio

# 7. Install all requirements
echo "📦 Installing AI/ML & LLM dependency stack..."
pip install -r "$CDIR/requirements.txt"

echo "============================================================"
echo "🎉 ENVIRONMENT READY TO ROCK & ROLL!"
echo "To activate your environment run:"
echo "   source .venv/bin/activate"
echo "============================================================"
