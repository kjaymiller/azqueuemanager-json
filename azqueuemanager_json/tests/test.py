import os
from azqueuemanager.queue import QueueClient
from azqueuemanager import QueueManager
from azqueuemanager_json import JSONTransform

json_parser = JSONTransform(json_in_file='test_json_in.json')

queue_client = QueueClient.from_connection_string(
    queue_name = 'test-queue',
)

queue_mgr = QueueManager(
    queue = queue_client,
    input_transformer= json_parser,
    output_transformer= json_parser,
)

if __name__ == '__main__':
    print(queue_mgr.next_messages())
