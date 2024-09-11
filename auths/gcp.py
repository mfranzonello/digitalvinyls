''' Token management '''

from io import BytesIO

from google.oauth2.service_account import Credentials as SACredentials
from google.cloud.storage import Client

from . import gcp_auth
from ..common.secret import get_secret
from ..common.structure import GCP_TOKEN_URI, GCP_AUTH_URL, GCP_APIS_URL, GCP_S_PROJECT_ID, GCP_S_ACCOUNT_NAME, GCP_S_BUCKET_NAME


class GClouder:
    ''' store and retrieve objects '''
    def __init__(self):
        super().__init__()

        gcp_s_email = f'{GCP_S_ACCOUNT_NAME}-service@{GCP_S_PROJECT_ID}.iam.gserviceaccount.com'
        gcp_service_account = gcp_s_email.replace('@', '%40')

        info = {'type': 'service_account',
                'project_id': GCP_S_PROJECT_ID,
                'private_key_id': get_secret('GCP_S_PRIVATE_KEY_ID'),
                'private_key': get_secret('GCP_S_PRIVATE_KEY').replace('\\n', '\n'),
                'client_email': gcp_s_email,
                'client_id': get_secret('GCP_S_CLIENT_ID'),
                'auth_uri': f'{GCP_AUTH_URL}/o/oauth2/auth',
                'token_uri': GCP_TOKEN_URI,
                'auth_provider_x509_cert_url': f'{GCP_APIS_URL}/oauth2/v1/certs',
                'client_x509_cert_url': f'{GCP_APIS_URL}/robot/v1/metadata/x509/{gcp_service_account}'
                }

        self.credentials = SACredentials.from_service_account_info(info)
        self.storage = Client(credentials=self.credentials)

        self.bucket_name = GCP_S_BUCKET_NAME

    # pickling actions
    def save_item(self, key, item):
        self.store_blob(self.bucket_name, key, item)

    def load_item(self, key):
        return self.get_blob(self.bucket_name, key)

    def find_item(self, key):
        return self.find_blob(self.bucket_name, key)

    def get_item(self, key):
        ok = self.find_item(key)
        stored = self.load_item(key) if ok else None

        return stored, ok

    def clear_items(self, key):
        self.clear_blobs(self.bucket_name, key)

    # google cloud interactions
    def get_bucket(self, bucket_name):
        ''' get bucket container '''
        bucket = self.storage.get_bucket(bucket_name)

        return bucket

    def list_blobs(self, bucket_name):
        ''' get list of blobs in a bucket '''
        bucket = self.get_bucket(bucket_name)
        blobs = bucket.list_blobs()

        return blobs

    def find_blob(self, bucket_name, blob_name):
        ''' see if blob exists '''
        print(f'\t...looking for {bucket_name}/{blob_name}', end='')
        found = blob_name in [blob.name for blob in self.list_blobs(bucket_name)]
        print(f'...{"found" if found else "not found"}!')

        return found

    def store_blob(self, bucket_name, blob_name, contents):
        ''' store blob contents '''
        print(f'\t...storing {bucket_name}/{blob_name}', end='')
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        p = compress(pickle.dumps(contents))
        b = BytesIO(p)
        blob.upload_from_file(b)
        print(f'...complete!')

    def get_blob(self, bucket_name, blob_name):
        ''' retrive blob contents '''
        print(f'\t...retrieving {bucket_name}/{blob_name}', end='')
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        b = blob.download_as_bytes()
        contents = pickle.loads(decompress(b))
        print(f'...complete!')

        return contents

    def clear_blob(self, bucket_name, blob_name):
        ''' remove a specific blob '''
        bucket = self.get_bucket(bucket_name)
        bucket.delete_blob(blob_name)

    def clear_blobs(self, bucket_name, partial_blob_name):
        ''' remove all matching blobs '''
        bucket = self.get_bucket(bucket_name)
        blobs = self.list_blobs(bucket_name)
        remove_blobs = [blob for blob in blobs if f'/{partial_blob_name}' in blob.name]
        bucket.delete_blobs(remove_blobs)