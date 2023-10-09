import aiohttp
import asyncio
import logging
import networkx as nx
from jsonpath_ng import parse
from typing import Optional, Dict, List, Callable, Union, Any, Type
from dataclasses import dataclass, field

logging.basicConfig(level=logging.INFO)

@dataclass
class URLParam:
    value: str

@dataclass
class DataParam:
    value: str

@dataclass
class APINode:
    id: str
    base_url: str
    method: str = "GET"
    input_params: Optional[Dict[str, Union[URLParam, DataParam]]] = None
    output_params: Optional[Dict[str, str]] = None
    credentials: Optional[Dict[str, Dict[str, str]]] = None
    error_handlers: Optional[Dict[int, Callable]] = None
    linkage_function: Callable = lambda input: input
    data_template: Optional[Dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self):
        self.input_params = self.input_params or {}
        self.output_params = self.output_params or {}
        self.credentials = self.credentials or {}
        self.error_handlers = self.error_handlers or {}

    def build_url(self, params: Dict[str, str]) -> str:
        return self.base_url.format(**params)

@dataclass
class PythonNode:
    id: str
    function: Callable

Node = Union[APINode, PythonNode]

@dataclass
class Edge:
    source: str  # id of the source node
    target: str  # id of the target node

class APIFlow:
    def __init__(self, nodes: List[Node], edges: List[Edge]):
        self.nodes = nodes
        self.edges = edges

    def get_node(self, node_id: str) -> Optional[Node]:
        """
        Retrieves a node by its ID.
        """
        for node in self.nodes:
            if node.id == node_id:
                return node
        return None

    def get_children(self, node_id: str) -> List[str]:
        """
        Retrieves the IDs of child nodes of a given node.
        """
        return [edge.target for edge in self.edges if edge.source == node_id]

    def get_starting_node(self) -> Optional[Node]:
        """
        Gets the starting node in the flow. This is determined by the node that has no incoming edges.
        Returns the starting node or None if no starting node could be determined.
        """
        G = nx.DiGraph()
        for edge in self.edges:
            G.add_edge(edge.source, edge.target)

        starting_nodes = [node for node, indegree in G.in_degree() if indegree == 0]

        if len(starting_nodes) == 1:
            return self.get_node(starting_nodes[0])
        elif len(starting_nodes) == 0:
            raise ValueError("No starting node found. The flow might have a cycle.")
        else:
            raise ValueError("Multiple potential starting nodes detected. Please ensure a single entry point for the flow.")


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

        populated_data = template.copy()
        for key, param in params.items():
            populated_data[key] = param
        return populated_data

    async def fetch(self, session: aiohttp.ClientSession, node: APINode, params: Dict[str, str]) -> Dict[str, Any]:
        cache_key = (node.base_url, frozenset(params.items()))
        if cache_key in self.cache:
            return self.cache[cache_key]

        # Separate parameters based on their type
        url_parameters = {k: v for k, v in params.items() if isinstance(node.input_params.get(k), URLParam)}
        data_parameters = {k: v for k, v in params.items() if isinstance(node.input_params.get(k), DataParam)}

        url = node.build_url(params)
        populated_data = self.populate_template(node.data_template, data_parameters)
        
        # Populate the data template
        if node.method in ["POST", "PUT", "PATCH"]:
            data_parameters = {k: v.value for k, v in node.input_params.items() if isinstance(v, DataParam)}
            populated_data = self.populate_template(node.data_template, data_parameters)
            response = await session.request(method=node.method, url=url, json=populated_data, headers=node.credentials.get('headers'), cookies=node.credentials.get('cookies'))
        else:
            response = await session.request(method=node.method, url=url, headers=node.credentials.get('headers'), cookies=node.credentials.get('cookies'))

        outputs = {}
        if response.status == 200:
            response_json = await response.json()
            for param_name, jsonpath_expr in node.output_params.items():
                # Use jsonpath to extract specific values from the JSON response
                expr = parse(jsonpath_expr)
                matches = [match.value for match in expr.find(response_json)]
                outputs[param_name] = matches or None
        else:
            # Handle response errors
            error_handler = node.error_handlers.get(response.status)
            if error_handler:
                outputs = error_handler(response)
            else:
                logging.info(f"Request to {url} failed with status code: {response.status}")

        await response.release()
        result = {'input': params, 'output': outputs}
        # cache results as we get them to avoid duplicate requests
        self.cache[cache_key] = result
        return result

    async def fetch_with_retry(self, session: aiohttp.ClientSession, node: APINode, params: Dict[str, str]) -> Dict[str, Any]:
        async with self.semaphore:
            for retry in range(self.max_retries):
                try:
                    return await self.fetch(session, node, params)
                except aiohttp.ClientResponseError as e:
                    logging.info(f"Request to {node.base_url} failed with status code: {e.status}. Retrying...")
                    await asyncio.sleep(2**retry)  # Exponential backoff
            logging.error(f"Failed to fetch data after {self.max_retries} retries for node {node.id}")
            return {}

    async def run_flow(self, flow: APIFlow, start_params: Dict[str, str], callback: Callable):
        async with aiohttp.ClientSession() as session:
            node_mapping = {node.id: node for node in flow.nodes}
            results = {}

            async def process_node(node: Node, params: Dict[str, str]) -> Dict[str, Any]:
                response = {}
                
                if isinstance(node, APINode):  # If this is an APINode
                    # Fetch the response for this node using the provided params
                    response = await self.fetch(session, node, params)
                    output_data = response['output']
                    # Use the node's linkage function to determine the parameters for subsequent calls
                    linkage_params = node.linkage_function(output_data)
                    if not linkage_params:
                        return response  # No linkage means we're done for this branch
                    
                elif isinstance(node, PythonNode):  # If this is a PythonNode
                    output_data = node.function(params)
                    linkage_params = [{"result": output_data}]  # Wrap in a list for uniform processing
                
                else:
                    raise ValueError(f"Unknown node type: {type(node)}")

                # The expected structure of linkage_params is a dictionary. However, for multiple subsequent calls,
                # it can be a list of dictionaries. Convert a single dictionary into a list for uniformity.
                if isinstance(linkage_params, dict):
                    linkage_params = [linkage_params]

                combined_responses = {node.id: response}

                # Process children nodes recursively
                for end_params in linkage_params:
                    for child_node_id in flow.get_children(node.id):
                        child_node = flow.get_node(child_node_id)
                        child_response = await process_node(child_node, end_params)
                        
                        # Combine the response of the child node
                        combined_responses.update(child_response)

                # Invoke the callback
                return combined_responses

            # Create the DAG and ensure it's acyclic
            G = nx.DiGraph()
            for edge in flow.edges:
                G.add_edge(edge.source, edge.target)

            if not nx.is_directed_acyclic_graph(G):
                raise ValueError("The API flow contains cycles and is not a valid DAG.")

            # Identify the starting node and initiate the flow
            starting_node = flow.get_starting_node()
            if not starting_node:
                raise ValueError("No starting node detected in the flow.")
            combined_responses = await process_node(node_mapping[starting_node.id], start_params)
            callback(combined_responses)

    def run(self, flow: APIFlow, start_params: Dict[str, str], callback: Callable):
        asyncio.run(self.run_flow(flow, start_params, callback))
