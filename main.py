import time
import traceback
import asyncio
import sys, json
# sys.path.insert(0, "..")
import logging
from asyncua import Client, Node, ua
logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('asyncua')

import awsiot.greengrasscoreipc
import awsiot.greengrasscoreipc.client as client
from awsiot.greengrasscoreipc.model import (
    IoTCoreMessage,
    QOS,
    SubscribeToIoTCoreRequest,
    PublishToIoTCoreRequest
)

from greengrasssdk.stream_manager import (
    StreamManagerClient,
    ReadMessagesOptions,
    StreamManagerException
)

TIMEOUT = 10

ipc_client = awsiot.greengrasscoreipc.connect()


async def m(message):
    async with Client(url="opc.tcp://192.168.21.141:4840/freeopcua/server/") as client:
        # Client has a few methods to get proxy to UA nodes that should always be in address space such as Root or Objects
        # Node objects have methods to read and write node attributes as well as browse or populate address space
        _logger.info('Children of root are: %r', await client.nodes.root.get_children())

        uri = 'http://examples.freeopcua.github.io'
        idx = await client.get_namespace_index(uri)

        try:
            node = client.get_node(ua.NodeId.from_string("ns=3;i=6027"))
            await node.write_value(message)
            
        except Exception:
            pass#print(Exception, id)


class StreamHandler(client.SubscribeToIoTCoreStreamHandler):
    def __init__(self):
        super().__init__()

    def on_stream_event(self, event: IoTCoreMessage) -> None:
        try:
            message = str(event.message.payload, "utf-8")
            topic_name = event.message.topic_name
            print(message)
            #ns=3;i=6027
            asyncio.run(m(str(message)))
            
            # Handle message.
            topic = "my/response"
            message = "Hello, World"
            qos = QOS.AT_LEAST_ONCE

            request = PublishToIoTCoreRequest()
            request.topic_name = topic
            request.payload = bytes(message, "utf-8")
            request.qos = qos
            operation = ipc_client.new_publish_to_iot_core()
            operation.activate(request)
            future = operation.get_response()
           
        except:
            traceback.print_exc()

    def on_stream_error(self, error: Exception) -> bool:
        # Handle error.
        return True  # Return True to close stream, False to keep stream open.

    def on_stream_closed(self) -> None:
        # Handle close.
        pass


topic = "my/topic"
qos = QOS.AT_MOST_ONCE

request = SubscribeToIoTCoreRequest()
request.topic_name = topic
request.qos = qos
handler = StreamHandler()
operation = ipc_client.new_subscribe_to_iot_core(handler)
future = operation.activate(request)
print("subscribed")




print("result wait")
# Keep the main thread alive, or the process will exit.
while True:
    time.sleep(10)
    
                  
# To stop subscribing, close the operation stream.
operation.close()