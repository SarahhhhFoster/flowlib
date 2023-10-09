import aiohttp
import asyncio
import logging
from jsonpath_ng import parse
from typing import Optional, Dict, List, Callable, Union, Any
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)

# Define a data class to represent a URL parameter
@dataclass
class URLParam:
    pass

# Define a data class to represent a data parameter
@dataclass
class DataParam:
    pass

# Define a data class to represent an API with its details
@dataclass
class API:
    base_url: str
    method: str = "GET"
    input_params: Optional[Dict[str, Union[URLParam, DataParam]]] = None
    output_params: Optional[Dict[str, str]] = None
    credentials: Optional[Dict[str, Dict[str, str]]] = None
    error_handlers: Optional[Dict[int, Callable]] = None
    data_template: Optional[Dict[str, Any]] = None  # Template for the request body

    # Set default values for the data class attributes if they are not provided
    def __post_init__(self):
        self.input_params = self.input_params or {}
        self.output_params = self.output_params or {}
        self.credentials = self.credentials or {}
        self.error_handlers = self.error_handlers or {}
        self.data_template = self.data_template or {}

    # Build and return the full URL by substituting the given parameters into the base URL
    def build_url(self, url_params: Dict[str, str]) -> str:
        return self.base_url.format(**url_params)

# Define a data class to represent a linkage between two APIs
@dataclass
class APILinkage:
    start_api: API
    end_api: API
    linkage_function: Callable  # Function to map the output of the start_api to the input of the end_api

# Define a class to represent an intermerdiate Python processing step
@dataclass
class PythonLinkage:
    function: Callable

# Define a data class to represent a flow of linked APIs
@dataclass
class APIFlow:
    linkages: List[Union[APILinkage, PythonLinkage]]

# Define a class to manage asynchronous API requests
class Getter:
    def __init__(self, max_retries: int = 5, workers: int = 5):
        self.cache = {}  # A cache to store API responses
        self.max_retries = max_retries  # Maximum number of retries for failed requests
        self.semaphore = asyncio.Semaphore(workers)  # Semaphore to limit the number of concurrent requests

    def populate_template(self, template: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively populates a template with values from params."""
        # If template is not provided, default to flat dictionary of params
        if not template:
            return params

        populated = {}
        for key, value in template.items():
            if isinstance(value, dict):
                populated[key] = self.populate_template(value, params)
            else:
                populated[key] = params.get(key, value)
        return populated

    async def fetch(self, session: aiohttp.ClientSession, api: API, params: Dict[str, str]) -> Dict[str, Any]:
        # Fetch the API response asynchronously
        cache_key = (api.base_url, frozenset(params.items()))
        if cache_key in self.cache:
            return self.cache[cache_key]

        # Separate parameters based on their type
        url_parameters = {k: v for k, v in params.items() if isinstance(api.input_params.get(k), URLParam)}
        data_parameters = {k: v for k, v in params.items() if isinstance(api.input_params.get(k), DataParam)}

        url = api.build_url(url_parameters)
        populated_data = self.populate_template(api.data_template, data_parameters)
        
        response = await session.request(
            method=api.method, 
            url=url, 
            headers=api.credentials.get('headers'), 
            cookies=api.credentials.get('cookies'),
            json=populated_data  # Use the populated data template as the JSON body
        )

        outputs = {}
        if response.status == 200:
            response_json = await response.json()
            for param_name, jsonpath_expr in api.output_params.items():
                # Use jsonpath to extract specific values from the JSON response
                expr = parse(jsonpath_expr)
                matches = [match.value for match in expr.find(response_json)]
                outputs[param_name] = matches or None
        else:
            # Handle response errors
            error_handler = api.error_handlers.get(response.status)
            if error_handler:
                outputs = error_handler(response)
            else:
                logging.info(f"Request to {url} failed with status code: {response.status}")

        await response.release()
        result = {'input': params, 'output': outputs}
        # cache results as we get them to avoid duplicate requests
        self.cache[cache_key] = result
        return result

    async def fetch_with_retry(self, session: aiohttp.ClientSession, api: API, params: Dict[str, str]) -> Dict[str, Any]:
        # Fetch the API response with retries in case of failures
        async with self.semaphore:
            for retry in range(self.max_retries):
                try:
                    return await self.fetch(session, api, params)
                except aiohttp.ClientResponseError as e:
                    logging.info(f"Request to {api.base_url} failed with status code: {e.status}. Retrying...")
                    await asyncio.sleep(2**retry)  # Exponential backoff
            logging.error(f"Failed to fetch data after {self.max_retries} retries for API {api.base_url}")
            return {}

    async def run_flow(self, flow: APIFlow, start_params: Dict[str, str], callback: Callable):
        # Execute the entire API flow
        async with aiohttp.ClientSession() as session:
            results = {}  # Moved outside the process_linkage to hold results across linkages

            async def process_linkage(linkage: Union[APILinkage, PythonLinkage], params: Dict[str, str]) -> Dict[str, Any]:
                # Process an individual linkage in the flow

                if isinstance(linkage, APILinkage):
                    start_response = await self.fetch_with_retry(session, linkage.start_api, params)
                    results[linkage.start_api.base_url] = start_response

                    outputs = start_response['output']
                    for key, values in outputs.items():
                        if not isinstance(values, list):
                            values = [values]

                        for value in values:
                            output_data = {key: value}
                            end_params_list = linkage.linkage_function(output_data)
                            end_responses = await asyncio.gather(
                                *[self.fetch_with_retry(session, linkage.end_api, end_params) for end_params in end_params_list]
                            )
                            for end_response in end_responses:
                                results[linkage.end_api.base_url] = end_response
                                callback(results)

                elif isinstance(linkage, PythonLinkage):
                    # Here we apply the function to transform/filter the results
                    transformed_results = linkage.function(results)
                    results.update(transformed_results)
                    callback(results)

                return results

            tasks = [process_linkage(linkage, start_params) for linkage in flow.linkages]
            await asyncio.gather(*tasks)

    def run(self, flow: APIFlow, start_params: Dict[str, str], callback: Callable):
        # Wrapper to run the API flow
        asyncio.run(self.run_flow(flow, start_params, callback))
