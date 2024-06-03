import unittest
from unittest.mock import patch, mock_open
import fsspec
import time
import random

class TestCloudLock(unittest.TestCase):

    @patch('fsspec.filesystem')
    def test_s3_lock(self, mock_filesystem):
        # Setup mock
        mock_fs = mock_filesystem.return_value
        mock_fs.exists.return_value = Falseimport unittest
from unittest.mock import patch, mock_open
import fsspec
import time
import random

class TestCloudLock(unittest.TestCase):

    @patch('fsspec.filesystem')
    def test_s3_lock(self, mock_filesystem):
        # Setup mock
        mock_fs = mock_filesystem.return_value
        mock_fs.exists.return_value = False

        # Create instance
        s3lock = cloudLock(dirlock="s3://mybucket/lock_hash")

        # Lock the file
        mock_fs.open = mock_open()
        mock_fs.open.return_value.__enter__.return_value.read.return_value = str(time.time_ns())
        result = s3lock.lock("s3://myotherbucket/myfile.csv")
        self.assertTrue(result)

        # Unlock the file
        result = s3lock.unlock("s3://myotherbucket/myfile.csv")
        self.assertTrue(result)

    @patch('fsspec.filesystem')
    def test_gcs_lock(self, mock_filesystem):
        # Setup mock
        mock_fs = mock_filesystem.return_value
        mock_fs.exists.return_value = False

        # Create instance
        gcslock = cloudLock(dirlock="gs://mybucket/lock_hash")

        # Lock the file
        mock_fs.open = mock_open()
        mock_fs.open.return_value.__enter__.return_value.read.return_value = str(time.time_ns())
        result = gcslock.lock("gs://myotherbucket/myfile.csv")
        self.assertTrue(result)

        # Unlock the file
        result = gcslock.unlock("gs://myotherbucket/myfile.csv")
        self.assertTrue(result)

    @patch('fsspec.filesystem')
    def test_local_lock(self, mock_filesystem):
        # Setup mock
        mock_fs = mock_filesystem.return_value
        mock_fs.exists.return_value = False

        # Create instance
        locallock = cloudLock(dirlock="/local/path/lock_hash")

        # Lock the file
        mock_fs.open = mock_open()
        mock_fs.open.return_value.__enter__.return_value.read.return_value = str(time.time_ns())
        result = locallock.lock("/local/path/myfile.csv")
        self.assertTrue(result)

        # Unlock the file
        result = locallock.unlock("/local/path/myfile.csv")
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()


        # Create instance
        s3lock = cloudLock(dirlock="s3://mybucket/lock_hash")

        # Lock the file
        mock_fs.open = mock_open()
        mock_fs.open.return_value.__enter__.return_value.read.return_value = str(time.time_ns())
        result = s3lock.lock("s3://myotherbucket/myfile.csv")
        self.assertTrue(result)

        # Unlock the file
        result = s3lock.unlock("s3://myotherbucket/myfile.csv")
        self.assertTrue(result)

    @patch('fsspec.filesystem')
    def test_gcs_lock(self, mock_filesystem):
        # Setup mock
        mock_fs = mock_filesystem.return_value
        mock_fs.exists.return_value = False

        # Create instance
        gcslock = cloudLock(dirlock="gs://mybucket/lock_hash")

        # Lock the file
        mock_fs.open = mock_open()
        mock_fs.open.return_value.__enter__.return_value.read.return_value = str(time.time_ns())
        result = gcslock.lock("gs://myotherbucket/myfile.csv")
        self.assertTrue(result)

        # Unlock the file
        result = gcslock.unlock("gs://myotherbucket/myfile.csv")
        self.assertTrue(result)

    @patch('fsspec.filesystem')
    def test_local_lock(self, mock_filesystem):
        # Setup mock
        mock_fs = mock_filesystem.return_value
        mock_fs.exists.return_value = False

        # Create instance
        locallock = cloudLock(dirlock="/local/path/lock_hash")

        # Lock the file
        mock_fs.open = mock_open()
        mock_fs.open.return_value.__enter__.return_value.read.return_value = str(time.time_ns())
        result = locallock.lock("/local/path/myfile.csv")
        self.assertTrue(result)

        # Unlock the file
        result = locallock.unlock("/local/path/myfile.csv")
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()
