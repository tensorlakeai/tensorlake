import httpx
import requests
from typing import Union, Dict, List
from pathlib import Path
from retry import retry
from httpx import ConnectTimeout


retry(tries=10, delay=2)
def upload_file_sync(
    file_path: Union[str, Path],
    api_key: str = "",
    base_url: str = "https://api.tensorlake.ai"
) -> str:
    """
    Synchronously upload a file to the Tensorlake
    
    Args:
        file_path: Path to the file to upload
        api_key: API authentication token
        base_url: Base URL for the API (optional)
        
    Returns:
        Dict containing the API response
        
    Raises:
        httpx.HTTPError: If the request fails
        FileNotFoundError: If the file doesn't exist
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
        
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    
    url = f"{base_url}/documents/v1/files"
    
    with open(file_path, "rb") as f:
        files = {"file": (file_path.name, f)}
        with httpx.Client() as client:
            response = client.post(
                url=url,
                headers=headers,
                files=files,
                timeout=None,
            )
            response.raise_for_status()
            resp = response.json()
            print(resp)
            return resp.get("id")
        
retry(tries=10, delay=2)
def upload_files_sync(
    file_paths: List[Union[str, Path]],
    api_key: str = "",
    base_url: str = "https://api.tensorlake.ai"
) -> str:
    """
    Synchronously upload a file to the Tensorlake
    
    Args:
        file_path: Path to the file to upload
        api_key: API authentication token
        base_url: Base URL for the API (optional)
        
    Returns:
        Dict containing the API response
        
    Raises:
        httpx.HTTPError: If the request fails
        FileNotFoundError: If the file doesn't exist
    """
    #file_path = Path(file_path)
    #if not file_path.exists():
    #    raise FileNotFoundError(f"File not found: {file_path}")
        
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    
    url = f"{base_url}/documents/v1/files"
    file_ids = []
    with httpx.Client(headers=headers, base_url="https://api.tensorlake.ai") as client:
        for file_path in file_paths:
            file_path = Path(file_path.path)
            with open(file_path, "rb") as f:
                files = {"file": (file_path.name, f)}
                response = client.post(
                    url="/documents/v1/files",
                    files=files,
                    timeout=None,
                )
                response.raise_for_status()
                resp = response.json()
                print(resp)
                file_id = resp.get("id")
                file_ids.append(file_id)
    return file_ids

    
async def upload_file_async(
    file_path: Union[str, Path],
    api_key: str = "",
    base_url: str = "https://api.tensorlake.ai"
) -> Dict:
    """
    Asynchronously upload a file to the Tensorlake
    
    Args:
        file_path: Path to the file to upload
        api_key: API authentication token
        base_url: Base URL for the API (optional)
        
    Returns:
        Dict containing the API response
        
    Raises:
        httpx.HTTPError: If the request fails
        FileNotFoundError: If the file doesn't exist
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
        
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    
    url = f"{base_url}/documents/v1/files"
    
    async with httpx.AsyncClient() as client:
        with open(file_path, "rb") as f:
            files = {"file": (file_path.name, f)}
            response = await client.post(
                url=url,
                headers=headers,
                files=files
            )
            response.raise_for_status()
            resp = response.json()
            return resp.get("id")
