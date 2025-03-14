import shutil
import psutil
import pandas as pd
import time
import re
from uainepydat import datatransform

def gather_free_space_in_drive(drive: str) -> float:
    """
    Gather the free space in a specified drive.

    Parameters:
    drive (str): The drive to check the free space of. If the drive is a single letter, it is assumed to be a Windows drive.

    Returns:
    float: The free space in bytes.
    """
    print(drive)
    if len(drive) == 1:
        total, used, free = shutil.disk_usage(drive + ":\\")
    else:
        total, used, free = shutil.disk_usage(drive)
    return free

def free_gb_in_drive(drive: str) -> float:
    """
    Calculate the free space in a specified drive in gigabytes (GB).

    Parameters:
    drive (str): The drive to check the free space of.

    Returns:
    float: The free space in gigabytes (GB).
    """
    return gather_free_space_in_drive(drive) / (1024 * 1024 * 1024)

def list_drives() -> list[str]:
    """
    List all available drives on the system.

    Returns:
    list[str]: A list of device names for all available drives.
    """
    return [drive.device for drive in psutil.disk_partitions()]

def list_drive_spaces() -> pd.DataFrame:
    """
    List all available drives and their free space in gigabytes (GB).

    Returns:
    pd.DataFrame: A DataFrame with the drive names and their free space in gigabytes (GB).
    """
    df = pd.DataFrame(list_drives(), columns=["drive"])
    #df["drive"] = df["drive"].apply(datatransform.keep_only_letters)
    #print(df)
    df["space_free_gb"] = df["drive"].apply(free_gb_in_drive)
    return df

def get_largest_drive() -> dict[str, any]:
    """
    Identifies and returns information about the drive with the most free space.
    
    The function finds the drive with the maximum available free space and returns
    its information with only the letters kept in the drive name.
    
    Returns:
        dict[str, any]: Dictionary containing information about the drive with
        the most free space, with the drive name containing only letters.
    """
    # Get list of all drives with their space information
    df = list_drive_spaces()
    
    # Find the index of the drive with maximum free space
    index = df["space_free_gb"].idxmax()

def get_free_ram_in_gb() -> float:
    """
    Get the amount of free RAM on the system in gigabytes.

    This function uses the `psutil` library to retrieve the amount of free RAM
    and converts it from bytes to gigabytes.

    Returns:
        float: The amount of free RAM in gigabytes.
    """
    # Get the amount of free RAM in bytes
    free_ram_bytes = psutil.virtual_memory().available

    # Convert the amount of free RAM from bytes to gigabytes
    free_ram_gb = free_ram_bytes / (1024 ** 3)

    return free_ram_gb

def get_number_virtual_cores() -> int:
    """
    Get the number of virtual (logical) CPU cores including hyperthreads.

    Returns:
        int: The number of virtual CPU cores.
    """
    return psutil.cpu_count(logical=True)

def get_physical_cores() -> int:
    """
    Get the number of physical CPU cores.

    Returns:
        int: The number of physical CPU cores.
    """
    return psutil.cpu_count(logical=False)

def get_free_ram() -> int:
    """
    Get the amount of free RAM available in bytes.

    Returns:
        int: The amount of free RAM in bytes.
    """
    return psutil.virtual_memory().available

def get_installed_ram_gb() -> int:
    """
    Get the total amount of installed RAM in gigabytes (GB).

    Returns:
        int: The total amount of installed RAM in gigabytes (GB).
    """
    ram = psutil.virtual_memory().total
    ram = ram / (1024 * 1024 * 1024)  # convert to GB
    return round(ram)

def get_cpu_usage_percent() -> float:
    """
    Get the current CPU usage as a percentage.

    Returns:
        float: Current CPU usage percentage.
    """
    return psutil.cpu_percent(interval=1)

def get_per_cpu_usage_percent() -> list[float]:
    """
    Get CPU usage percentage for each individual CPU core.

    Returns:
        list[float]: List of CPU usage percentages for each core.
    """
    return psutil.cpu_percent(interval=1, percpu=True)

def get_system_uptime() -> float:
    """
    Get the system uptime in seconds.

    Returns:
        float: System uptime in seconds.
    """
    return psutil.boot_time()

def get_formatted_uptime() -> str:
    """
    Get the system uptime formatted as days, hours, minutes, seconds.

    Returns:
        str: Formatted uptime string.
    """
    uptime_seconds = time.time() - psutil.boot_time()
    days, remainder = divmod(uptime_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(days)}d {int(hours)}h {int(minutes)}m {int(seconds)}s"

def get_battery_info() -> dict:
    """
    Get information about the system battery.

    Returns:
        dict: Dictionary containing battery percentage, time left, and power plugged status.
              Returns None if no battery is present.
    """
    if not hasattr(psutil, "sensors_battery") or psutil.sensors_battery() is None:
        return None
    
    battery = psutil.sensors_battery()
    time_left = battery.secsleft
    
    # Convert seconds to hours and minutes if discharging
    if time_left != psutil.POWER_TIME_UNLIMITED and time_left != psutil.POWER_TIME_UNKNOWN:
        hours, remainder = divmod(time_left, 3600)
        minutes, _ = divmod(remainder, 60)
        time_str = f"{int(hours)}h {int(minutes)}m"
    else:
        time_str = "Unknown" if time_left == psutil.POWER_TIME_UNKNOWN else "Unlimited"
    
    return {
        "percent": battery.percent,
        "time_left": time_str,
        "power_plugged": battery.power_plugged
    }

def get_network_stats() -> pd.DataFrame:
    """
    Get statistics for all network interfaces.

    Returns:
        pd.DataFrame: DataFrame with network interface statistics.
    """
    net_io = psutil.net_io_counters(pernic=True)
    stats = []
    
    for interface, data in net_io.items():
        stats.append({
            "interface": interface,
            "bytes_sent": data.bytes_sent,
            "bytes_recv": data.bytes_recv,
            "packets_sent": data.packets_sent,
            "packets_recv": data.packets_recv,
            "mb_sent": round(data.bytes_sent / (1024 * 1024), 2),
            "mb_recv": round(data.bytes_recv / (1024 * 1024), 2)
        })
    
    return pd.DataFrame(stats)

def get_top_processes(n=5) -> pd.DataFrame:
    """
    Get the top n processes by memory usage.

    Parameters:
        n (int): Number of processes to return. Default is 5.

    Returns:
        pd.DataFrame: DataFrame with top processes information.
    """
    processes = []
    
    for proc in psutil.process_iter(['pid', 'name', 'memory_percent', 'cpu_percent']):
        try:
            processes.append({
                'pid': proc.info['pid'],
                'name': proc.info['name'],
                'memory_percent': proc.info['memory_percent'],
                'cpu_percent': proc.info['cpu_percent']
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    
    df = pd.DataFrame(processes)
    return df.nlargest(n, 'memory_percent')

def get_system_info() -> dict:
    """
    Get general system information.

    Returns:
        dict: Dictionary containing OS, hostname, and platform information.
    """
    import platform
    import socket
    
    return {
        "os": platform.system(),
        "os_version": platform.version(),
        "hostname": socket.gethostname(),
        "architecture": platform.architecture()[0],
        "processor": platform.processor(),
        "python_version": platform.python_version()
    }

# Example executions:
if __name__ == "__main__":
    # Drive space examples
    print(free_gb_in_drive("C"))
    print(list_drives())
    print("Drive with most free space:", get_largest_drive())
    
    # RAM examples
    print("Free RAM (GB):", get_free_ram_in_gb())
    print("Total installed RAM (GB):", get_installed_ram_gb())
    
    # CPU examples
    print("Physical CPU cores:", get_physical_cores())
    print("Virtual CPU cores:", get_number_virtual_cores())
    print("Current CPU usage (%):", get_cpu_usage_percent())
    print("Per-core CPU usage (%):", get_per_cpu_usage_percent())
    
    # System info examples
    print("System uptime:", get_formatted_uptime())
    print("System information:", get_system_info())
    
    # Battery info (if available)
    battery = get_battery_info()
    if battery:
        print("Battery percentage:", battery["percent"])
        print("Battery time left:", battery["time_left"])
        print("Power plugged in:", battery["power_plugged"])
    
    # Network stats
    print("Network statistics:")
    print(get_network_stats())
    
    # Process monitoring
    print("Top 3 processes by memory usage:")
    print(get_top_processes(3))
