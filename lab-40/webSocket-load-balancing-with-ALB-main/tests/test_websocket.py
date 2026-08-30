"""
Lab 40: WebSocket Connection Test
Tests basic WebSocket connectivity through the ALB
"""

import asyncio
import websockets
import json
import sys
import time


async def test_websocket_connection(url, test_name):
    """Test a single WebSocket connection."""
    print(f"\n{'='*60}")
    print(f"Test: {test_name}")
    print(f"URL: {url}")
    print(f"{'='*60}")

    try:
        async with websockets.connect(url) as websocket:
            print(f"✅ Connected successfully")

            # Receive welcome message
            welcome = await websocket.recv()
            welcome_data = json.loads(welcome)
            print(f"📨 Welcome message: {json.dumps(welcome_data, indent=2)}")

            # Send test message
            test_message = {
                "type": "test",
                "content": "Hello from test client",
                "timestamp": time.time()
            }
            await websocket.send(json.dumps(test_message))
            print(f"📤 Sent: {json.dumps(test_message)}")

            # Receive echo
            response = await websocket.recv()
            response_data = json.loads(response)
            print(f"📥 Received: {json.dumps(response_data, indent=2)}")

            # Verify response
            if response_data.get("type") == "echo":
                print(f"✅ Test passed!")
                return True
            else:
                print(f"❌ Unexpected response type: {response_data.get('type')}")
                return False

    except websockets.exceptions.WebSocketException as e:
        print(f"❌ WebSocket error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


async def test_multiple_messages(url, count=5):
    """Test sending multiple messages on the same connection."""
    print(f"\n{'='*60}")
    print(f"Test: Multiple Messages (count={count})")
    print(f"{'='*60}")

    try:
        async with websockets.connect(url) as websocket:
            await websocket.recv()  # Welcome message

            for i in range(count):
                msg = {"type": "msg", "number": i}
                await websocket.send(json.dumps(msg))

                response = await websocket.recv()
                resp_data = json.loads(response)
                print(f"  Message {i}: Sent {msg}, Got echo from {resp_data.get('instance_id')}")

            print(f"✅ All {count} messages sent and received successfully")
            return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


async def main():
    """Main test runner."""
    # Get URL from command line or use default
    if len(sys.argv) > 1:
        url = sys.argv[1]
    else:
        url = "ws://localhost:8000/ws"

    print(f"\n🧪 Lab 40: WebSocket Connection Tests")
    print(f"{'='*60}\n")

    # Test 1: Basic connection
    result1 = await test_websocket_connection(url, "Basic Connection")

    # Test 2: Multiple messages
    result2 = await test_multiple_messages(url)

    # Summary
    print(f"\n{'='*60}")
    print(f"Test Summary")
    print(f"{'='*60}")
    print(f"Basic Connection: {'✅ PASS' if result1 else '❌ FAIL'}")
    print(f"Multiple Messages: {'✅ PASS' if result2 else '❌ FAIL'}")

    if result1 and result2:
        print(f"\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print(f"\n❌ Some tests failed")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())