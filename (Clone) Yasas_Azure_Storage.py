# Databricks notebook source
storageAccountName = "internship"

storageAccountAccessKey = "ORKNtmjb6X3/xuSMXbei0r6k0LQhA2d4I40B8N+4MnWTj+wElPAFrPVsAXIB+M9q+nGUJ45xHlyi+AStghIXqg=="

blobContainerName = "yasas"

mountPoint = "/mnt/data/yasas"

if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):

  try:

    dbutils.fs.mount(

      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),

      mount_point = mountPoint,

      extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}

    )

    print("mount succeeded!")

  except Exception as e:

    print("mount exception", e)
