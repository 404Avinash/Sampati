"""
Environment Capabilities Verification Script
"""
import sys

def check_environment():
    print("==================================================")
    print("🔍 VERIFYING AI / ML ENVIRONMENT & CAPABILITIES")
    print("==================================================")
    print(f"🐍 Python Version: {sys.version.split()[0]}")
    
    # Check PyTorch & CUDA
    try:
        import torch
        print(f"🔥 PyTorch Version: {torch.__version__}")
        cuda_available = torch.cuda.is_available()
        print(f"⚡ CUDA GPU Available: {cuda_available}")
        if cuda_available:
            print(f"🎮 GPU Device Name: {torch.cuda.get_device_name(0)}")
            print(f"🧠 GPU Memory: {torch.cuda.get_device_properties(0).total_memory / 1e9:.2f} GB")
    except ImportError:
        print("❌ PyTorch: Not installed")

    # Check Data Science
    for pkg in ["numpy", "pandas", "scipy", "sklearn"]:
        try:
            m = __import__(pkg)
            print(f"📊 {pkg.capitalize()}: {getattr(m, '__version__', 'Installed')}")
        except ImportError:
            print(f"❌ {pkg.capitalize()}: Not installed")

    # Check HuggingFace & LLM Frameworks
    for pkg in ["transformers", "datasets", "langchain", "openai"]:
        try:
            m = __import__(pkg)
            print(f"🤗 {pkg.capitalize()}: Installed")
        except ImportError:
            print(f"❌ {pkg.capitalize()}: Not installed")

    # Check Vision & Audio
    for pkg in ["cv2", "PIL", "librosa"]:
        try:
            m = __import__(pkg)
            print(f"👁️/🔊 {pkg}: Installed")
        except ImportError:
            print(f"❌ {pkg}: Not installed")

    # Check Web & Backend
    for pkg in ["fastapi", "uvicorn", "pydantic"]:
        try:
            m = __import__(pkg)
            print(f"🌐 {pkg.capitalize()}: Installed")
        except ImportError:
            print(f"❌ {pkg.capitalize()}: Not installed")

    print("==================================================")

if __name__ == "__main__":
    check_environment()
