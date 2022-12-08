"""
    author: diasadhitama3@gmail.com
"""

from application import (Create, Upload, Read, Read2)
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--action", help="'create bucket' / 'read file' / 'upload file' ", type=str)

    args    = parser.parse_args()

    action  = args.action
    if not action:
        while True:
            action = input("'create bucket' or 'read file' or 'upload file'?\n")
            if not action:
                print("please enter argument")
            else:
                break
    if action not in["create bucket", "read file", "upload file"]:
        raise ValueError("Invalid action: should be 'create bucket' or 'read file' or 'upload file'")

    if action == 'create bucket':
        run_create = Create()
        run_create.create_bucket()
    elif action == 'read file':
        print("Read and Insert Data")
        run_read = Read()
        run_read.read_file() 
    elif action == 'upload file':
        print("Updload Data to s3 Bucket")
        run_upload = Upload()
        run_upload.upload_file()