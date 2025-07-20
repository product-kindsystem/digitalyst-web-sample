import os
import subprocess

def kill_process_using_port(port):
    try:
        # netstat でポートを使用しているプロセスを検索
        result = subprocess.run(
            f'netstat -ano | findstr :{port}',
            shell=True,
            capture_output=True,
            text=True
        )

        if result.stdout:
            lines = result.stdout.splitlines()
            for line in lines:
                parts = line.split()
                if len(parts) >= 5:
                    pid = parts[-1]  # PID は最後の要素
                    print(f"Port {port} is in use by process ID: {pid}")

                    # プロセスを強制終了
                    os.system(f'taskkill /PID {pid} /F')
                    print(f"Process {pid} terminated.")
        else:
            print(f"Port {port} is not in use.")

    except Exception as e:
        print(f"Error: {e}")

# ポート 50007 を使用しているプロセスを調べて、使用中なら終了
kill_process_using_port(50007)
