import uuid


class IDGenerator:
    def generate_node_id(self):
        return str(uuid.uuid4())

    def generate_edge_id(self):
        return str(uuid.uuid4())

    def generate_subgraph_id(self):
        return str(uuid.uuid4())

    def generate_timeseries_id(self):
        return str(uuid.uuid4())


'''
def process_subgraph_batches(self):
    # Create a dictionary to store subgraph start times
    subgraph_start_times = {}
    for df in self.subgraph_batches:
        for _, row in df.iterrows():
            subgraph_id = row['id']
            start_time = parse_datetime(row['start_time'])
            subgraph_start_times[subgraph_id] = start_time

    # Create dictionaries to hold external to internal ID mappings
    external_to_internal_nodes = {}
    external_to_internal_edges = {}

    # Process the node membership CSV file
    node_membership_df = pd.read_csv('node_membership_data.csv')  # Adjust the path as necessary
    for _, row in node_membership_df.iterrows():
        external_id = str(row['id'])
        # Check if the node already exists and get the internal ID if it does
        existing_node = next((data['data'] for _, data in self.hygraph.graph.nodes(data=True) if data['data'].node_id == external_id), None)
        if existing_node:
            internal_id = existing_node.oid  # Use existing internal ID
        else:
            internal_id = self.hygraph.id_generator.generate_node_id()  # Generate a new internal ID
        external_to_internal_nodes[external_id] = internal_id

    # Process the edges membership CSV file
    edge_membership_df = pd.read_csv('edge_membership_data.csv')  # Adjust the path as necessary
    for _, row in edge_membership_df.iterrows():
        external_edge_id = str(row['id'])
        # Check if the edges already exists and get the internal ID if it does
        existing_edge = next((data['data'] for _, _, data in self.hygraph.graph.edges(data=True) if data['data'].edge_id == external_edge_id), None)
        if existing_edge:
            internal_edge_id = existing_edge.oid  # Use existing internal ID
        else:
            internal_edge_id = self.hygraph.id_generator.generate_edge_id()  # Generate a new internal ID
        external_to_internal_edges[external_edge_id] = internal_edge_id

    # Process the subgraphs
    for df in self.subgraph_batches:
        for _, row in df.iterrows():
            label = row['label']
            subgraph_id, start_time, end_time = str(row['id']), row['start_time'], row.get('end_time', FAR_FUTURE_DATE)
            end_time = self.end_time_config(end_time)

            node_file_ids = row['node_file_ids'].split(' ')
            edge_file_ids = row['edge_file_ids'].split(' ')

            # Convert external node IDs to internal IDs
            node_internal_ids = [external_to_internal_nodes[node_id] for node_id in node_file_ids if node_id in external_to_internal_nodes]

            # Convert external edges IDs to internal IDs
            edge_internal_ids = [external_to_internal_edges[edge_id] for edge_id in edge_file_ids if edge_id in external_to_internal_edges]

            def filter_func(element):
                if isinstance(element, PGNode):
                    return element.oid in node_internal_ids
                elif isinstance(element, Edge):
                    return element.oid in edge_internal_ids
                return False

            subgraph = Subgraph(subgraph_id=subgraph_id, label=label, start_time=start_time, end_time=end_time,
                                filter_func=filter_func)
            self.hygraph.add_subgraph(subgraph)

            # Collect timestamps and subgraph IDs
            timestamps = []
            subgraph_ids = []

            # Update membership for nodes
            for node_id in node_internal_ids:
                node = self.hygraph.get_element('node', node_id)
                memberships = [subgraph_id for subgraph_id in subgraph_start_times if
                               is_valid_membership(node, subgraph_start_times[subgraph_id], end_time)]
                for ts in memberships:
                    timestamps.append(subgraph_start_times[ts])
                    subgraph_ids.append(ts)

                self.hygraph.update_membership(node, timestamps, subgraph_ids)

            # Update membership for edges
            for edge_id in edge_internal_ids:
                edges = self.hygraph.get_element('edges', edge_id)
                memberships = [subgraph_id for subgraph_id in subgraph_start_times if
                               is_valid_membership(edges, subgraph_start_times[subgraph_id], end_time)]
                for ts in memberships:
                    timestamps.append(subgraph_start_times[ts])
                    subgraph_ids.append(ts)

                self.hygraph.update_membership(edges, timestamps, subgraph_ids) '''




