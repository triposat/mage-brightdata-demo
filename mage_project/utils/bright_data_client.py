"""
Bright Data client utilities supporting multiple products:
1. Residential Proxies - for general web scraping
2. Web Unlocker API - for sites with anti-bot protection
3. Web Scraper APIs - for target-specific scraping (Amazon, eBay, etc.)
"""

import os
import requests
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import urllib3

# Disable SSL warnings for proxy connections
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@dataclass
class ResidentialProxyConfig:
    """Bright Data Residential Proxy configuration."""
    customer_id: str
    zone: str
    password: str
    host: str = "brd.superproxy.io"
    port: int = 33335
    country: Optional[str] = None

    @property
    def username(self) -> str:
        base = f"brd-customer-{self.customer_id}-zone-{self.zone}"
        if self.country:
            base += f"-country-{self.country}"
        return base

    @property
    def proxy_url(self) -> str:
        return f"http://{self.username}:{self.password}@{self.host}:{self.port}"

    @property
    def proxies(self) -> Dict[str, str]:
        return {"http": self.proxy_url, "https": self.proxy_url}


class BrightDataResidentialProxy:
    """
    Client for Bright Data Residential Proxy.

    Best for: General web scraping, geo-targeting, session management.
    """

    def __init__(
        self,
        customer_id: Optional[str] = None,
        zone: Optional[str] = None,
        password: Optional[str] = None,
        country: Optional[str] = None
    ):
        self.config = ResidentialProxyConfig(
            customer_id=customer_id or os.getenv('BRIGHT_DATA_CUSTOMER_ID'),
            zone=zone or os.getenv('BRIGHT_DATA_ZONE'),
            password=password or os.getenv('BRIGHT_DATA_PASSWORD'),
            country=country
        )

        if not all([self.config.customer_id, self.config.zone, self.config.password]):
            raise ValueError("Missing Bright Data residential proxy credentials")

    def fetch(self, url: str, timeout: int = 60, **kwargs) -> requests.Response:
        """Fetch URL through residential proxy."""
        return requests.get(
            url,
            proxies=self.config.proxies,
            timeout=timeout,
            verify=False,
            **kwargs
        )

    def fetch_many(self, urls: List[str], timeout: int = 60) -> List[Dict[str, Any]]:
        """Fetch multiple URLs and return results."""
        results = []
        for url in urls:
            try:
                response = self.fetch(url, timeout=timeout)
                results.append({
                    'url': url,
                    'status_code': response.status_code,
                    'content': response.text,
                    'success': response.status_code == 200
                })
            except Exception as e:
                results.append({
                    'url': url,
                    'status_code': None,
                    'content': None,
                    'success': False,
                    'error': str(e)
                })
        return results


class BrightDataWebUnlocker:
    """
    Client for Bright Data Web Unlocker API.

    Best for: Sites with anti-bot protection, CAPTCHAs, JavaScript rendering.
    """

    API_URL = "https://api.brightdata.com/request"

    def __init__(
        self,
        api_token: Optional[str] = None,
        zone: str = "web_unlocker"
    ):
        self.api_token = api_token or os.getenv('BRIGHT_DATA_API_TOKEN')
        self.zone = zone

        if not self.api_token:
            raise ValueError("Missing BRIGHT_DATA_API_TOKEN")

    def fetch(
        self,
        url: str,
        country: Optional[str] = None,
        format: str = "raw",
        timeout: int = 60
    ) -> requests.Response:
        """
        Fetch URL through Web Unlocker API.

        Args:
            url: Target URL
            country: Optional country code (us, uk, etc.)
            format: Response format ('raw' or 'json')
            timeout: Request timeout
        """
        payload = {
            "zone": self.zone,
            "url": url,
            "format": format
        }

        if country:
            payload["country"] = country

        return requests.post(
            self.API_URL,
            headers={
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=timeout
        )

    def fetch_many(self, urls: List[str], **kwargs) -> List[Dict[str, Any]]:
        """Fetch multiple URLs through Web Unlocker."""
        results = []
        for url in urls:
            try:
                response = self.fetch(url, **kwargs)
                results.append({
                    'url': url,
                    'status_code': response.status_code,
                    'content': response.text,
                    'success': response.status_code == 200
                })
            except Exception as e:
                results.append({
                    'url': url,
                    'status_code': None,
                    'content': None,
                    'success': False,
                    'error': str(e)
                })
        return results


class BrightDataWebScraperAPI:
    """
    Client for Bright Data target-specific Web Scraper APIs.

    Supported targets: Amazon, eBay, LinkedIn, Instagram, Google, and more.
    These APIs return structured JSON data directly.
    """

    BASE_URL = "https://api.brightdata.com/datasets/v3"

    # Dataset IDs for common targets
    DATASETS = {
        "amazon_products": "gd_l7q7dkf244hwjntr0",
        "amazon_reviews": "gd_le8e811kzy4ggddlq",
        "ebay_products": "gd_ltr9mjt81n0zzdk1fb",
        "linkedin_profiles": "gd_l1viktl72bvl7bjuj0",
        "linkedin_companies": "gd_l1viktkuf4gvs7esh2",
        "google_search": "gd_l1vijqt9jfj7olije",
        "google_maps": "gd_l1vijwop1198k7j4i",
        "instagram_profiles": "gd_lk5ns7kz21pck8jpis",
        "indeed_jobs": "gd_lpfll7v5hcqtkxl6l",
        "zillow_properties": "gd_lfqkr8wm13ixtbd8f5",
    }

    def __init__(self, api_token: Optional[str] = None):
        self.api_token = api_token or os.getenv('BRIGHT_DATA_API_TOKEN')

        if not self.api_token:
            raise ValueError("Missing BRIGHT_DATA_API_TOKEN")

    def trigger_collection(
        self,
        dataset: str,
        inputs: List[Dict[str, Any]],
        format: str = "json"
    ) -> Dict[str, Any]:
        """
        Trigger a data collection job.

        Args:
            dataset: Dataset name (e.g., 'amazon_products') or dataset ID
            inputs: List of input parameters (e.g., [{"url": "..."}])
            format: Output format ('json' or 'csv')

        Returns:
            Response with snapshot_id for tracking
        """
        dataset_id = self.DATASETS.get(dataset, dataset)

        response = requests.post(
            f"{self.BASE_URL}/trigger",
            headers={
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json"
            },
            params={"dataset_id": dataset_id, "format": format},
            json=inputs
        )

        return response.json()

    def get_snapshot(self, snapshot_id: str) -> requests.Response:
        """Get results of a completed collection job."""
        return requests.get(
            f"{self.BASE_URL}/snapshot/{snapshot_id}",
            headers={"Authorization": f"Bearer {self.api_token}"}
        )


# Convenience factory functions
def get_residential_proxy(country: Optional[str] = None) -> BrightDataResidentialProxy:
    """Get a residential proxy client."""
    return BrightDataResidentialProxy(country=country)


def get_web_unlocker() -> BrightDataWebUnlocker:
    """Get a Web Unlocker client."""
    return BrightDataWebUnlocker()


def get_scraper_api() -> BrightDataWebScraperAPI:
    """Get a Web Scraper API client."""
    return BrightDataWebScraperAPI()
