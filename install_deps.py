import subprocess
import sys

def install_packages():
    packages = [
        'selenium',
        'sqlalchemy',
        'pandas',
        'webdriver-manager'
    ]
    
    for package in packages:
        print(f"Устанавливаю {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    
    print("\nВсе зависимости установлены!")

if __name__ == "__main__":
    install_packages()