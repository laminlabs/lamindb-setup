from upath import UPath
import time
import os


class s3Access:
    buckets_options: dict = dict()
    default_options: dict = dict(cache_regions=True)

    initialized: bool = False

    managed_credentials_start = None  # this is for managed credentials refresh

    # this implements anon fallback
    # does only one check request for a bucket
    @classmethod
    def process_path(cls, path: UPath):
        assert cls.initialized

        bucket = path._url.netloc
        if bucket in cls.buckets_options:
            bucket_options = cls.buckets_options[bucket]
            if not bucket_options.items() <= path._kwargs.items():
                path = UPath(path, **bucket_options)
        else:
            if not cls.default_options.items() <= path._kwargs.items():
                path = UPath(path, **cls.default_options)
            path = cls.try_access(path)
        return path

    @classmethod
    def try_access(cls, path: UPath):
        bucket = path._url.netloc
        try:
            # check with current credentials
            path.fs.call_s3("head_bucket", Bucket=bucket)
            cls.buckets_options[bucket] = dict(cls.default_options)
        except Exception:
            # set to anon if fails
            # will raise an error here if both ways fail
            path = UPath(path, anon=True)
            path.fs.call_s3("head_bucket", Bucket=bucket)
            cls.buckets_options[bucket] = dict(cache_regions=True, anon=True)
        return path

    # this should be called once on a root
    # checks managed credentials
    @classmethod
    def initialize(cls, path: UPath):
        cls.initialized = True

        if not cls.default_options.items() <= path._kwargs.items():
            path = UPath(path, **cls.default_options)
        bucket = path._url.netloc
        try:
            path.fs.call_s3("head_bucket", Bucket=bucket)
            # no need for furhter checks for this bucket
            cls.buckets_options[bucket] = dict(cls.default_options)
        except Exception:
            try:
                from ._hub_core import access_aws

                access_aws()  # sets credentials via env variables
                cls.managed_credentials_start = time.time()
                creds = dict(
                    key=os.environ["AWS_ACCESS_KEY_ID"],
                    secret=os.environ["AWS_SECRET_ACCESS_KEY"],
                    token=os.environ["AWS_SESSION_TOKEN"],
                )
                path = UPath(path, **creds)
                path.fs.call_s3("head_bucket", Bucket=bucket)
                cls.default_options = dict(cls.default_options, **creds)
                # no need for furhter checks for this bucket
                cls.buckets_options[bucket] = dict(cls.default_options)
            except Exception:
                os.environ.pop("AWS_ACCESS_KEY_ID", None)
                os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
                os.environ.pop("AWS_SESSION_TOKEN", None)

        path = cls.process_path(path)
        return path
