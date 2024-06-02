# client.py ----------------------------------------------------------------------------------------
#
# Description:
#    This script contains the client class
#
# --------------------------------------------------------------------------------------------------


# ==================================================================================================
# Imports
# ==================================================================================================
# Build-in
import os
import re
import logging
# Installed
import requests
# Custom
# NOTE: Add here all the Custom modules


# ==================================================================================================
# Logging
# ==================================================================================================
logger = logging.getLogger(__name__)


# ==================================================================================================
# Classes
# ==================================================================================================
#
class FileResponse:
    """Represents a response object for handling file downloads from HTTP requests.

    Parameters
    ----------
    response (requests.Response): The response object obtained from a HTTP request.
    """

    def __init__(self, response):
        self.response = response

    def file(self):
        """Read the response content.

        Returns
        -------
        str: The content of the file.
        """
        return self.response.content.decode()

    def save(self, path: str = None):
        """Save the response content to a file.

        Parameters
        ----------
        path (optional[str]): The path where the file will be saved. If None, the current working directory is used.

        Returns
        -------
        str: The filepath where the file is saved.

        Raises
        ------
        Exception: If the specified path is not a valid directory.
        """
        if not path:
            path = os.getcwd()
        if not os.path.isdir(path):
            raise Exception(f"Directory {path} does not exist!")
        filename = self.filename()
        filepath = os.path.join(path, filename)
        with open(filepath, "wb") as f:
            for chunk in self.response.iter_content(1024):
                f.write(chunk)
        return filepath

    def is_downloadable(self):
        """Check if the response content is downloadable.

        Returns
        -------
        FileResponse: self

        Raises
        ------
        Exception: If the content-type indicates that the resource is not a file or HTML content.
        """
        content_type = self.response.headers.get('content-type')
        if 'text' in content_type.lower():
            raise Exception(f"Is not a file!")
        if 'html' in content_type.lower():
            raise Exception(f"Is not a file!")
        return self

    def filename(self):
        """Get filename from content-disposition header.

        Returns
        -------
        str: The filename extracted from the content-disposition header, or None if not found.
        """
        content_disposition = self.response.headers.get('content-disposition')
        if not content_disposition:
            return None
        fname = re.findall('filename=(.+)', content_disposition)
        if len(fname) == 0:
            return None
        return fname[0]


class BucketClient:
    """A simple client for interacting with an Bucket-like storage service.

    host (str): The host of the Bucket-like storage service.
    port (int): The port of the Bucket-like storage service.
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.url = f"http://{host}:{port}"

    def upload_file(self, filepath: str, **kwargs):
        """Upload multiple file to the storage service.

        Parameters
        ----------
        filepath (str): A path of the file object to upload.
        unique (bool): The Bucket stores the file using a genarated name.
        metadata (bool): The Bucket returns file's metadata instead of filename.
        """
        with open(filepath, "rb") as f:
            response = requests.post(f"{self.url}/bucket/v1/file",
                                     files={"file": f},
                                     params=kwargs)
            response.raise_for_status()  # Raise an exception for non-2xx status codes
            return response.json()

    def update_file(self, filepath: str, **kwargs):
        """Update a file in the storage service.

        Parameters
        ----------
        filepath (str): A path of the file object to updated.
        """
        filename = os.path.basename(filepath)
        with open(filepath, "rb") as f:
            response = requests.put(f"{self.url}/bucket/v1/file/{filename}",
                                    files={"file": f},
                                    params=kwargs)
            response.raise_for_status()  # Raise an exception for non-2xx status codes
            return response.json()

    def get_file(self, filename: str):
        """Retrieve a file from the storage service.

        Parameters
        ----------
        filename (str): The name of the file to retrieve.
        """
        response = requests.get(f"{self.url}/bucket/v1/file/{filename}",
                                stream=True)  # Stream download for large files
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        return FileResponse(response).is_downloadable()

    def delete_file(self, filename: str):
        """Delete a file from the storage service.

        Parameters
        ----------
        filename (str): The name of the file to delete.
        """
        response = requests.delete(f"{self.url}/bucket/v1/file/{filename}")
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        return response.json()
