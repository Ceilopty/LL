"""This is the client running on the exposed computer, inquiring
realtime price from the third party API by server's command, receive
order from the server, transmit to API, and send the respond back to
server.
"""

__all__ = ["",
           ]

# keep getting server's order.
mainloop = None

# price inquiring
get_price = None

# send the price got to the server
send_price = None

# get orders from the server
get_order = None

# send orders to API
send_order = None

# get status of the order from API
get_status = None

# send status to the server
send_status = None
