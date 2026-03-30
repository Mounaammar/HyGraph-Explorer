# Define all needed constraints
from datetime import datetime
# Placeholder for a date far in the future
FAR_FUTURE_DATE = datetime(2100, 12, 31, 23, 59, 59)
def is_valid_membership(element, subgraph_start_time, subgraph_end_time):
    """
    Validate if an element (node or edges) can be part of a subgraph based on the timeline.

    Parameters:
    - element: The node or edges to validate.
    - subgraph_start_time: The start time of the subgraph.
    - subgraph_end_time: The end time of the subgraph.

    Returns:
    - bool: True if the element can be part of the subgraph, False otherwise.
    """
    # Check if the element's start_time and end_time are within the subgraph's timeline
    element_start_time = parse_datetime(element.start_time)
    subgraph_start_time = parse_datetime(subgraph_start_time)
    if element_start_time  >  subgraph_start_time:
        print(f"Element {element.oid} starts after the subgraph {subgraph_start_time}.")
        return False
    if (element.end_time and element.end_time!=FAR_FUTURE_DATE) and (subgraph_end_time and subgraph_end_time!=FAR_FUTURE_DATE) :
        if element.end_time < subgraph_end_time:
            print(f"Element {element.oid} ends before the subgraph {subgraph_end_time}.")
            return False
    return True

    # Convert string dates to datetime objects if necessary

def parse_datetime(datetime_str):
    if isinstance(datetime_str, str):
        try:
            parsed_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            return parsed_datetime
        except (TypeError, ValueError):
            print(f"Failed to parse datetime: {datetime_str}")
            return None
    else:
        print(f"Expected a string, but got {type(datetime_str)}: {datetime_str}")
        return datetime_str  # If it's already a datetime object, just return it


def create_time_series_from_graph(self, query):
    """
    Create time series data for nodes, edges, or subgraphs based on a provided query.

    Parameters:
    - query (dict): A dictionary containing filters and configuration for time series creation.
        - element_type (str): 'node', 'edges', or 'subgraph'.
        - element_agg_type (str): Either 'edges' or 'node' to specify the element to aggregate.
        - property_name (str, optional): Property to aggregate (if it's not a count).
        - aggregate_name (str): Aggregation method ('sum', 'mean', 'count', etc.).
        - direction (str, optional): Direction for edges aggregation ('in', 'out', 'both').
    """
    element_type = query.get('element_type', 'node')
    ts_config = query.get('time_series_config')
    element_agg_type = query.get('element_agg_type')  # Either 'edges' or 'node'
    property_name = ts_config.get('property_name', None)  # Property to aggregate
    aggregate_name = ts_config.get('aggregate_name', 'count')  # Aggregation method (sum, mean, etc.)
    direction = ts_config.get('direction', 'both')  # Direction for edges aggregation (only for edges)

    if element_type == 'node':
        # Handle nodes and their edges
        selected_nodes = self.filter_nodes(query.get('node_filter'), query.get('edge_filter'))
        for node_data in selected_nodes:
            data = node_data['data']
            tsid = data.properties.get(ts_config['attribute'])

            if tsid and tsid in self.time_series:
                # Append to existing time series
                current_value, change_timestamp = self.aggregate_value(
                    data, element_agg_type, property_name, aggregate_name, direction
                )
                self.append_time_series(tsid, change_timestamp, current_value)
            else:
                # Create new time series
                self.process_element_for_time_series(node_data, ts_config, element_type='node')

    elif element_type == 'subgraph':
        # Handle subgraphs with subgraph_filter
        selected_subgraphs = self.filter_subgraphs(query.get('subgraph_filter'))

        for subgraph in selected_subgraphs:
            self.process_element_for_time_series(subgraph, ts_config, element_type='subgraph')


def filter_subgraphs(self, subgraph_filter):
    """
    Apply the subgraph filter to find the relevant subgraphs.
    """
    filtered_subgraphs = []
    for subgraph_id, subgraph_data in self.subgraphs.items():
        if subgraph_filter(subgraph_data['data']):
            filtered_subgraphs.append(subgraph_data['data'])
    return filtered_subgraphs
def aggregate_value(self, element, element_agg_type, property_name, aggregate_name, direction='both'):
    """
    Compute the aggregated value based on whether it's counting the element or aggregating a property.
    Handles 'in', 'out', or 'both' edges direction for edges aggregations.
    Returns the aggregated value and the timestamp of the change.
    """
    change_timestamp = self.get_last_change_timestamp(element.oid)

    if aggregate_name == 'count':
        # Count the number of elements (nodes or edges)
        if element_agg_type == 'edges':
            return self.count_edges(element, direction), change_timestamp
        elif element_agg_type == 'node':
            return len(list(self.graph.nodes[element.oid])), change_timestamp
    else:
        # Handle property aggregation
        if property_name:
            if self.is_dynamic_property(element, property_name):
                # Aggregate across time-series data (dynamic)
                time_series_data = self.get_time_series_for_property(element, property_name)
                return self.apply_aggregation(time_series_data, aggregate_name), change_timestamp
            else:
                # Aggregate a static property
                static_value = element.properties.get(property_name, 0)
                return static_value, change_timestamp
        else:
            raise ValueError("Property name must be provided for aggregations other than 'count'.")


def count_edges(self, element, direction):
    """
    Count edges based on the direction ('in', 'out', or 'both').
    """
    if direction == 'in':
        return len([edge for edge in self.graph.in_edges(element.oid)])
    elif direction == 'out':
        return len([edge for edge in self.graph.out_edges(element.oid)])
    elif direction == 'both':
        return len(list(self.graph.edges(element.oid)))
    else:
        raise ValueError(f"Unsupported direction: {direction}")


def get_last_change_timestamp(self, element_id):
    """
    Retrieve the timestamp of the last change (addition/removal) for the given element.
    """
    for change in reversed(self.element_changes):
        if change['element_id'] == element_id:
            return change['timestamp']
    return datetime.now()  # Default to current time if no prior change is found


def is_dynamic_property(self, element, property_name):
    """
    Determine whether a property is dynamic (time-series) or static by checking if its value exists as a key in time_series.
    """
    property_value = element.properties.get(property_name)
    return property_value in self.time_series  # If it's in time_series, it's a time-series property


def get_time_series_for_property(self, element, property_name):
    """
    Retrieve the time series data associated with the property.
    """
    tsid = element.properties.get(property_name)
    if tsid and tsid in self.time_series:
        return self.time_series[tsid].data
    else:
        raise ValueError(f"Time series with ID {tsid} not found for property {property_name}.")


def apply_aggregation(self, data, aggregate_name):
    """
    Apply the specified aggregation function to the data.
    """
    if aggregate_name == 'sum':
        return np.sum(data)
    elif aggregate_name == 'mean':
        return np.mean(data)
    elif aggregate_name == 'count':
        return len(data)
    elif aggregate_name == 'min':
        return np.min(data)
    elif aggregate_name == 'max':
        return np.max(data)
    elif aggregate_name == 'variance':
        return np.var(data)
    else:
        raise ValueError(f"Unsupported aggregation: {aggregate_name}")


def append_time_series(self, tsid, date, value):
    if tsid in self.time_series:
        self.time_series[tsid].append_data(date, value)
    else:
        raise ValueError(f"Time series with ID {tsid} does not exist.")


def process_element_for_time_series(self, element_data, ts_config, element_type='node'):
    """
    Process a node or subgraph to create a new time series and store it.
    """
    element = element_data['data']
    attribute = ts_config['attribute']
    property_name = ts_config.get('property_name')
    aggregate_name = ts_config['aggregate_name']
    direction = ts_config.get('direction', 'both')

    # Calculate the aggregated value for the current state of the element (node/edges/subgraph)
    current_value, change_timestamp = self.aggregate_value(element, element_type, property_name, aggregate_name,
                                                           direction)

    # Create a new time series with the calculated value
    tsid = self.id_generator.generate_timeseries_id()
    timestamps = [change_timestamp]  # Use the last change timestamp
    reshaped_data_values = np.array([[current_value]])

    metadata = TimeSeriesMetadata(element.oid, element_type, attribute)
    time_series = TimeSeries(tsid, timestamps, [attribute], reshaped_data_values, metadata)

    # Store the time series and link it to the element
    self.time_series[tsid] = time_series
    self.add_property(element_type, element.oid, attribute, tsid)
    print(f"Time series created for {element_type} {element.oid}: {time_series}")


def update_element_changes(self, element_changes):
    """
    Track and store the changes made after batch processing. These changes include added/removed nodes, edges, etc.
    This function clears the `element_changes` list after storing the new changes.
    """
    # Assuming element_changes stores changes like additions/removals of nodes/edges
    self.element_changes = []

    for element_id, change_type, timestamp in element_changes:
        # Log the type of change (addition/removal) for each element with its timestamp
        self.element_changes.append({
            'element_id': element_id,
            'change_type': change_type,
            'timestamp': timestamp
        })
    # After processing, clear the element_changes list
    print(f"Updated element changes: {self.element_changes}")
    self.element_changes.clear()


# Add changes in `add_node`, `delete_node`, `add_edge`, and `delete_edge`

def add_node(self, node_data, timestamp=None):
    # Add a node to the graph
    self.graph.add_node(node_data['id'], **node_data)
    change_timestamp = timestamp if timestamp else datetime.now()
    self.update_element_changes([{
        'element_id': node_data['id'],
        'change_type': 'addition',
        'timestamp': change_timestamp
    }])


def delete_node(self, node_id, timestamp=None):
    # Remove a node from the graph
    self.graph.remove_node(node_id)
    change_timestamp = timestamp if timestamp else datetime.now()
    self.update_element_changes([{
        'element_id': node_id,
        'change_type': 'removal',
        'timestamp': change_timestamp
    }])


def add_edge(self, edge_data):
    # Add an edges to the graph
    self.graph.add_edge(edge_data['source'], edge_data['target'], **edge_data)
    change_timestamp = timestamp if timestamp else datetime.now()
    self.update_element_changes([{
        'element_id': edge_data['source'] + '-' + edge_data['target'],
        'change_type': 'addition',
        'timestamp': edge.end_time
    }])


def delete_edge(self, edge_id ):
    # Remove an edges from the graph
    self.graph.remove_edge(edge_id)
    change_timestamp = timestamp if timestamp else datetime.now()
    self.update_element_changes([{
        'element_id': oid,
        'change_type': 'removal',
        'timestamp': edge.end_time
    }])


def get_closest_inferior_timestamp(timeseries, target_timestamp):
    """
    Returns the closest timestamp in the timeseries that is less than or equal to the target_timestamp.
    """
    all_timestamps = timeseries.data.coords['time'].values  # Accessing the 'time' coordinate
    closest_timestamp = None

    for ts in all_timestamps:
        if pd.to_datetime(ts) <= pd.to_datetime(target_timestamp):
            closest_timestamp = ts
        else:
            break  # Stop once we surpass the target timestamp

    return closest_timestamp


def get_elements_in_subgraph_at_timestamp(self, subgraph_id, timestamp):
    """
    Retrieves all nodes and edges that are part of the subgraph at a specific timestamp.
    If no exact match is found, the last inferior timestamp is used.
    """
    nodes_in_subgraph = []
    edges_in_subgraph = []

    # Check the membership timeseries of each node
    for node_id, node_data in self.graph.nodes(data=True):
        if 'membership' in node_data['data'] and node_data['data'].membership:
            # Get the membership timeseries ID
            membership_series_id = node_data['data'].membership
            membership_timeseries = self.time_series.get(membership_series_id)

            if membership_timeseries:
                # Get the closest timestamp (inferior or equal)
                closest_timestamp = self.get_closest_inferior_timestamp(membership_timeseries, timestamp)

                if closest_timestamp:
                    # Retrieve the 'membership' variable at the closest timestamp
                    memberships_at_time = membership_timeseries.data.sel(time=closest_timestamp,
                                                                         variable='membership').values

                    # Split and check if the subgraph_id is in the membership list
                    memberships_at_time_list = memberships_at_time[
                        0].split()  # Assuming the values are stored as strings
                    if subgraph_id in memberships_at_time_list:
                        nodes_in_subgraph.append(node_id)

    # Check the membership timeseries of each edges
    for edge in self.graph.edges(data=True):
        if 'membership' in edge['data'] and edge['data'].membership:
            # Get the membership timeseries ID
            membership_series_id = edge['data'].membership
            membership_timeseries = self.time_series.get(membership_series_id)

            if membership_timeseries:
                # Get the closest timestamp (inferior or equal)
                closest_timestamp = self.get_closest_inferior_timestamp(membership_timeseries, timestamp)

                if closest_timestamp:
                    # Retrieve the 'membership' variable at the closest timestamp
                    memberships_at_time = membership_timeseries.data.sel(time=closest_timestamp,
                                                                         variable='membership').values

                    # Split and check if the subgraph_id is in the membership list
                    memberships_at_time_list = memberships_at_time[
                        0].split()  # Assuming the values are stored as strings
                    if subgraph_id in memberships_at_time_list:
                        edges_in_subgraph.append(edge)

    return nodes_in_subgraph, edges_in_subgraph