import shutil
import psutil
import pandas as pd

def gather_free_space_in_drive(drive):
    if (len(drive) == 1):
        total, used, free = shutil.disk_usage(drive + ":\\")
    else:
        total, used, free = shutil.disk_usage(drive)
    return free

def free_gb_in_drive(drive):
    return gather_free_space_in_drive(drive)/(1024*1024*1024)

def list_drives():
    return [drive.device for drive in psutil.disk_partitions()]

def list_drive_spaces():
    df = pd.DataFrame(list_drives(), columns=["drive"])
    df["space_free_gb"]=df["drive"].apply(free_gb_in_drive)
    return df

def get_largest_drive():
    df = list_drive_spaces()
    index = df["space_free_gb"].idxmax()
    return df.loc[index]

#example executions
#print(freespace.free_gb_in_drive("C"))
#print(freespace.list_drives())
