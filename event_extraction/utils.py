from gcloud_storage import GoogleCloudStorage

PROJECT_ID = "frances-365422"
BUCKET_NAME = "eidf_gpu"

cloud_storage_service = None

def get_google_cloud_storage():
    global cloud_storage_service
    if cloud_storage_service is not None:
        return cloud_storage_service
    cloud_storage_service = GoogleCloudStorage(project_id=PROJECT_ID, bucket_name=BUCKET_NAME)
    return cloud_storage_service