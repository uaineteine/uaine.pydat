import shutil
import psutil
import pandas as pd

def gather_free_space_in_drive(drive: str) -> float:
    """
    Gather the free space in a specified drive.

    Parameters:
    drive (str): The drive to check the free space of. If the drive is a single letter, it is assumed to be a Windows drive.

    Returns:
    float: The free space in bytes.
    """
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
    df["space_free_gb"] = df["drive"].apply(free_gb_in_drive)
    return df

def get_largest_drive():
    df = list_drive_spaces()
    index = df["space_free_gb"].idxmax()
    return df.loc[index]

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

#example executions
#print(systatus.free_gb_in_drive("C"))
#print(systatus.list_drives())
