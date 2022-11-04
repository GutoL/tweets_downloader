import pathlib
import mimetypes

from google.cloud import storage # pip install google-cloud-storage 
import glob

# References: 
# Interact with GCS: https://www.youtube.com/watch?v=1cDqRrw3t9o
# Create keys: https://www.skytowner.com/explore/guide_on_creating_a_service_account_and_private_keys_in_google_cloud_platform


def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""

class GCStorage:
  def __init__(self, storage_client) -> None:
     self.client = storage_client
  
  def create_bucket(self, bucket_name, storage_class, bucket_location='US'):
    bucket = self.client.bucket(bucket_name)
    bucket.storage_class = storage_class
    return self.client.create_bucket(bucket, bucket_location)
  
  def get_bucket(self, bucket_name):
    return self.client.get_bucket(bucket_name)

  def list_buckets(self):
    buckets = self.client.list_buckets()
    return [bucket.name for bucket in buckets]
  
  def upload_file(self, bucket, blob_destination, file_path):
    file_type = file_path.split('.')[-1]

    if file_type == '.csv':
      content_type = 'text/csv'
    else:
      content_type = mimetypes.guess_type(file_path)[0]
    
    blob = bucket.blob(blob_destination)
    blob.upload_from_filename(filename=file_path, content_type=content_type)
    
    return blob
  
  def list_blobs(self, bucket_name):
    return self.client.list_blobs(bucket_name)


class GoogleCloudStorageManager:

  def __init__(self, path_to_private_key):
    self.setup_connection(path_to_private_key)
    self.STORAGE_CLASSES = ('STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE')

    self.bucket_gcs = None

  def get_all_buckets_names(self):
    return [bucket for bucket in self.gcs.list_buckets()]

  # Step 2: construct the GCStorage instance
  def setup_connection(self, path_to_private_key):
    storage_client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
    self.gcs = GCStorage(storage_client)
  
  # Step 3: create gcp_api_demo Cloud Storage bucket
  def create_bucket(self, bucket_name, storage_class=0):
    if not bucket_name in self.gcs.list_buckets():
      self.bucket_gcs = self.gcs.create_bucket(bucket_name, self.STORAGE_CLASSES[storage_class])
    else:
      self.bucket_gcs = self.gcs.get_bucket(bucket_name)

  # Step 4: upload file to Google Cloud Storage
  def upload_file(self, files_folder, bucket_name):

    if self.bucket_gcs is None:
      self.create_bucket(bucket_name)

    for folders in glob.glob(files_folder+'*'):
        for file_path in pathlib.Path(folders).glob('*.*'):
            hashtag_group = find_between(str(file_path),'tweets/','/')
            self.gcs.upload_file(self.bucket_gcs, 'hashtag_group_'+hashtag_group+'/'+file_path.name, str(file_path))

    # files_folder = glob.glob(files_folder+'*') #pathlib.Path(files_folder)

    
    
    # for file_folder in files_folder:
    #     folder = file_folder+'/'
        
    #     for file_path in glob.glob(folder+'*'):
    #       file_name = file_path.split('/')[-1]
          
    #       # self.gcs.upload_file(self.bucket_gcs, str(folder), file_name)

  # Step 5 downloading data
  def download_data(self, bucket_name, path='', file_name=None):
    for blob in self.gcs.list_blobs(bucket_name):

      if file_name is not None:
        if blob.name == file_name:
          blob.download_to_filename(path+blob.name)
      else:
        blob.download_to_filename(path+blob.name)