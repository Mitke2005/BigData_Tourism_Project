import subprocess

res = subprocess.check_output(
    ['powershell', '-Command', "Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python|java' } | Select-Object ProcessId, Name, CommandLine | Format-List"],
    text=True,
    errors='replace'
)
print(res)
