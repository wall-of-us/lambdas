# this funtion streams the data from a mysql database to an s3 file, it is assumed that we have appropriate VPC
# permissions to read from the specified db and write to the indicated s3 bucket



import boto3
import subprocess
import os
import datetime

BUFFERSIZE = 8 * 1024 * 1024

# mysql_dump_program can be overwritten, because when running under lambda screws up executable bits 
def mysql_dump_to_s3(user, host, password, database, bucket, port=3306, mysql_dump_program="mysqldump"):
    now = datetime.datetime.now()
    key = "sqldumps/%04d/%02d/%02d/%02d:%02d:%02d.sql.gz" % (now.year, now.month, now.day, now.hour, now.minute, now.second)
    with os.fdopen(os.open('/tmp/.mysql', os.O_WRONLY | os.O_CREAT, 0o600), 'w') as handle:
        handle.write("[client]\npassword=%s\n" % password)
    print "About to save mysql db as zip file"
    try:
        s3 = boto3.client('s3')
        # Initiate the multipart upload and send the part(s)
        mpu = s3.create_multipart_upload(Bucket=bucket, Key=key)
        mysql_process = subprocess.Popen([mysql_dump_program, "--defaults-extra-file=/tmp/.mysql", "-u", user, "-h", host, "-e", "--opt",
                                          "--max_allowed_packet=512M", "-c", "-P", port, database],
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        gzip_process = subprocess.Popen(["gzip", "-c"], stdin=mysql_process.stdout, stdout=subprocess.PIPE)
        mysql_process.stdout.close() # Allow p1 to receive a SIGPIPE if p2 exits.
        parts = []
        index = 1
        while True:
            gzip_output = gzip_process.stdout.read(BUFFERSIZE)
            print "read %d packet from zip, doing an upload" % index
            if ('' == gzip_output):
                break
            part = s3.upload_part(Bucket=bucket, Key=key, PartNumber=index,
                                  UploadId=mpu['UploadId'], Body=gzip_output)
            
            parts.append({'PartNumber': index, 'ETag': part['ETag']})
            index += 1
        
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=mpu['UploadId'],
                                     MultipartUpload={ 'Parts': parts})
        error_string = mysql_process.stderr.read()
        print 'error_string: ' + error_string
    finally:
        os.unlink('/tmp/.mysql')
    print "Done save mysql db as zip file (key = '%s')"  % key
        
        
