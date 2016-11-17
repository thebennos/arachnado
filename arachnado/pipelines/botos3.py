import json
import boto3
from scrapy.utils.project import get_project_settings
from scrapy.utils.serialize import ScrapyJSONEncoder

from .items import CDRItem


class S3Pipeline(object):

    def __init__(self):

        self.SETTINGS = get_project_settings()
        self.encoder =  ScrapyJSONEncoder()
        if not 'AWS_ACCESS_KEY_ID' in self.SETTINGS:
            raise ValueError('Missing aws access key id')
        if not 'AWS_SECRET_ACCESS_KEY' in self.SETTINGS:
            raise ValueError('Missing aws secret key id')
        if not 'S3_BUCKET' in self.SETTINGS:
            raise ValueError('Missing s3 bucket name')
        if not 'FILES_STORE_S3_ACL' in self.SETTINGS:
            raise ValueError('Missing files store s3 acl')

        self.s3 = boto3.resource('s3',
                                 aws_access_key_id=self.SETTINGS['AWS_ACCESS_KEY_ID'],
                                 aws_secret_access_key=self.SETTINGS['AWS_SECRET_ACCESS_KEY'])


    def process_item(self, item, spider):

        if isinstance(item, CDRItem):

            try:
                self.put_item_in_bucket(item, self.SETTINGS['S3_BUCKET'])
            except botocore.exceptions.ClientError as e:
                raise DropItem(e)
            else:
                return item

        return item

    def put_item_in_bucket(self, item, bucket_name):
        bucket = self.s3.Bucket(bucket_name)

        json_data = self.encoder.encode(item)
        bucket.put_object(ACL=self.SETTINGS['FILES_STORE_S3_ACL'], Key=item['_id'], Body=json_data)
