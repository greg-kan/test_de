import boto3
import botocore
import json
import os
from pathlib import PurePath
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
import logging
import rosbag
import rospy
import cv2
from rosbags.image import message_to_cvimage
from rosbags.image import compressed_image_to_cvimage

from datetime import datetime
from datetime import timezone

CONNECT_JSON = '../gkanavenko_connect.json'
DESTINATION_JSON = '../gkanavenko_destination.json'
SOURCE_BUCKET = 'test-task-01'
SOURCE_PREFIX = 'bg/'
BAG_FILE_EXTENSION = '.bag'
DESTINATION_LOCAL_BAG = '/data1/s3'
LOG_FILE = '../routine.log'
DESTINATION_LOCAL_PICTURES = '/data1/s3/pictures'

DATA_TYPES = ['sensor_msgs/Image', 'sensor_msgs/CompressedImage']
TOPICS = ['/realsense_gripper/aligned_depth_to_color/image_raw', '/realsense_gripper/color/image_raw/compressed']

MIN_FILTER_TIME = '2023-08-22 18:33:48'
MAX_FILTER_TIME = '2023-08-22 18:33:52'

DEBUG_MODE = True

with open(CONNECT_JSON) as f_json:
    data_json = json.load(f_json)

s3_url = data_json['url']
s3_accessKey = data_json['accessKey']
s3_secretKey = data_json['secretKey']
s3_api = data_json['api']
s3_path = data_json['path']

with open(DESTINATION_JSON) as f_json:
    data_json = json.load(f_json)

destination_bucket = data_json['bucket']

logging.basicConfig(filename=LOG_FILE,
                    filemode='w',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)


def start_routine():
    logging.info("Routine started")
    for key, result in download_parallel_multithreading():
        logging.info(f"{key['Key']}, Size = {key['Size']}, result: {result}")
        if DEBUG_MODE:
            print(f"{key['Key']}, Size = {key['Size']}, result: {result}")

    process_bag_files(DESTINATION_LOCAL_BAG + '/' + SOURCE_PREFIX, topics=TOPICS,
                      start_time=MIN_FILTER_TIME,
                      end_time=MAX_FILTER_TIME)

    store_files_to_s3()

    logging.info("Routine finished")


def download_object(s3_client, file_name):
    download_path = PurePath(DESTINATION_LOCAL_BAG, file_name)

    logging.info(f"Downloading {file_name} to {download_path}")
    if DEBUG_MODE:
        print(f"Downloading {file_name} to {download_path}")
    s3_client.download_file(
        SOURCE_BUCKET,
        file_name,
        str(download_path)
    )
    return "Success"


def get_file_extension(file_name):
    _, f_extension = os.path.splitext(file_name)
    return f_extension.lower()


def get_file_name(file_name):
    f_name, _ = os.path.splitext(file_name)
    return f_name.lower()


def download_parallel_multithreading():
    b3_session = boto3.Session(aws_access_key_id=s3_accessKey,
                               aws_secret_access_key=s3_secretKey)  # ,region_name='us-east-1')

    b3_client = b3_session.client('s3',
                                  endpoint_url=s3_url,
                                  config=botocore.client.Config(signature_version=s3_api))

    obj_list = b3_client.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=SOURCE_PREFIX)

    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_key = {executor.submit(download_object,
                                         b3_client,
                                         obj['Key']): obj for obj in obj_list['Contents']
                         if get_file_extension(obj['Key']) == BAG_FILE_EXTENSION}

        for future in futures.as_completed(future_to_key):
            var_key = future_to_key[future]
            exception = future.exception()

            if not exception:
                yield var_key, future.result()
            else:
                yield var_key, exception


def filter_image_msgs(topic, datatype, md5sum, msg_def, header):
    if datatype in DATA_TYPES:
        return True

    return False


def process_bag_message(file_name, topic, message, datatype, time_stamp, folder_name_suffix):
    base_file_name = get_file_name(os.path.basename(file_name))

    nanosecs = str(time_stamp.nsecs)
    time_str = datetime.utcfromtimestamp(int(time_stamp.secs)).strftime('%Y-%m-%d_%H-%M-%S') + '-' + nanosecs

    out_file_name = time_str + '(' + base_file_name + ')'
    topic_str = topic[1:].replace('/', '_')

    out_dir_name = DESTINATION_LOCAL_PICTURES + '/' + topic_str + '/' + \
                   datatype + '/' + folder_name_suffix + '/'

    if datatype == 'Image':
        img = message_to_cvimage(message)
    elif datatype == 'CompressedImage':
        img = compressed_image_to_cvimage(message)

    if not os.path.exists(out_dir_name):
        os.makedirs(out_dir_name)

    cv2.imwrite(out_dir_name + out_file_name + '.png', img)

    logging.info(f"Written file: {out_file_name}")

    if DEBUG_MODE:
        print(out_file_name)
        print(out_dir_name)


def process_bag_file(file_name, topics, start_time, end_time):
    logging.info(f"Processing file: {file_name}")
    if DEBUG_MODE:
        print(f"Processing file: {file_name}")

    folder_name_suffix_start = datetime.utcfromtimestamp(int(start_time.secs)).strftime('%Y-%m-%d_%H-%M-%S')
    folder_name_suffix_end = datetime.utcfromtimestamp(int(end_time.secs)).strftime('%Y-%m-%d_%H-%M-%S')
    folder_name_suffix = folder_name_suffix_start + '_' + folder_name_suffix_end

    bag = rosbag.Bag(file_name, 'r')

    for topic, msg, t in bag.read_messages(topics=topics, start_time=start_time, end_time=end_time,
                                           connection_filter=filter_image_msgs):
        datatype = (str(type(msg)).split('__')[1])[:-2]
        process_bag_message(file_name, topic, msg, datatype, t, folder_name_suffix)

    bag.close()


def process_bag_files(path_to_files, topics=None, start_time=None, end_time=None):
    logging.info(f"Processing bag files started with: Local path to files={path_to_files}, "
                 f"Topics={topics}, Start time={start_time}, End time={end_time}")

    epoch_start = '1980-01-01'
    epoch_end = '2900-12-31'

    if start_time is None:
        start_time_t = datetime.strptime(epoch_start, '%Y-%m-%d')
    else:
        start_time_t = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')

    if end_time is None:
        end_time_t = datetime.strptime(epoch_end, '%Y-%m-%d')
    else:
        end_time_t = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

    start_timestamp = rospy.Time.from_sec(start_time_t.replace(tzinfo=timezone.utc).timestamp())
    end_timestamp = rospy.Time.from_sec(end_time_t.replace(tzinfo=timezone.utc).timestamp())

    if start_timestamp > end_timestamp:
        logging.error(f"Start filter time={start_time} is greater than End filter time={end_time}")

        if DEBUG_MODE:
            print('Error: End filter time is greater than Start filter time')
        return

    lst_files = os.listdir(path_to_files)
    lst_files = sorted([path_to_files + fl for fl in lst_files if get_file_extension(fl) == BAG_FILE_EXTENSION])

    if DEBUG_MODE:
        print(start_timestamp, end_timestamp)

    for fl in lst_files:
        process_bag_file(fl, topics, start_timestamp, end_timestamp)


def upload_file_to_s3(b3_client, file_name, bucket, object_name=None, args=None):
    if object_name is None:
        object_name = file_name

    try:
        response = b3_client.upload_file(file_name, bucket, object_name, ExtraArgs=args)
    except botocore.exceptions.ClientError as error:
        logging.error(error)
        return False
    return True


def store_files_to_s3():
    logging.info(f"Storing files to s3 store started")
    b3_session = boto3.Session(aws_access_key_id=s3_accessKey,
                               aws_secret_access_key=s3_secretKey)

    b3_client = b3_session.client('s3',
                                  endpoint_url=s3_url,
                                  config=botocore.client.Config(signature_version=s3_api))

    b3_resource = b3_session.resource('s3',
                                      endpoint_url=s3_url,
                                      config=botocore.client.Config(signature_version=s3_api))

    bucket = b3_resource.Bucket(destination_bucket)

    bucket.objects.all().delete()

    num_files = 0
    for root, subdirs, files in os.walk(DESTINATION_LOCAL_PICTURES):
        if files:
            for file in files:
                file_to_store = os.path.join(root, file)
                object_to_store = file_to_store.replace(DESTINATION_LOCAL_PICTURES + '/', '')
                if upload_file_to_s3(b3_client, file_to_store, destination_bucket, object_to_store):
                    num_files += 1
                    logging.info(f"File {file} stored")
                    if DEBUG_MODE:
                        print(f"File {file} stored")

    logging.info(f'Stored {num_files} files')
    if DEBUG_MODE:
        print(f'Stored {num_files} files')


if __name__ == "__main__":
    start_routine()

