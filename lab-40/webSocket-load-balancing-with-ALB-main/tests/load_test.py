"""
Lab 40: Load Test
Simulates multiple concurrent WebSocket connections
"""

import asyncio
import websockets
import json
import sys
import time
from collections import defaultdict


async def client_session(client_id, url, duration, results):
    """Simulate a WebSocket client session."""
    try:
        start_time = time.time()
        async with websockets.connect(url) as websocket:
            welcome = await websocket.recv()
            welcome_data = json.loads(welcome)
            instance_id = welcome_data.get('instance_id', 'unknown')

            message_count = 0

            while time.time() - start_time < duration:
                msg = {
                    "type": "load_test",
                    "client_id": client_id,
                    "message_num": message_count
                }
                await websocket.send(json.dumps(msg))
                response = await websocket.recv()
                message_count += 1

                await asyncio.sleep(0.1)  # Small delay

            results[client_id] = {
                "success": True,
                "instance_id": instance_id,
                "messages_sent": message_count,
                "duration": time.time() - start_time
            }

    except Exception as e:
        results[client_id] = {
            "success": False,
            "error": str(e)
        }


async def load_test(url, num_clients=20, duration=30):
    """
    Run load test with multiple concurrent clients.

    Args:
        url: WebSocket URL
        num_clients: Number of concurrent clients
        duration: Test duration in seconds
    """
    print(f"\n{'='*60}")
    print(f"Load Test")
    print(f"URL: {url}")
    print(f"Concurrent clients: {num_clients}")
    print(f"Duration: {duration}s")
    print(f"{'='*60}\n")

    results = {}

    # Create client tasks
    tasks = [
        client_session(i, url, duration, results)
        for i in range(num_clients)
    ]

    # Run all clients concurrently
    print(f"🚀 Starting {num_clients} concurrent clients...\n")
    await asyncio.gather(*tasks)

    # Analyze results
    successful = sum(1 for r in results.values() if r.get('success'))
    failed = num_clients - successful

    instance_counts = defaultdict(int)
    total_messages = 0

    for client_id, result in results.items():
        if result.get('success'):
            instance_counts[result['instance_id']] += 1
            total_messages += result.get('messages_sent', 0)

    # Print results
    print(f"\n{'='*60}")
    print(f"Load Test Results")
    print(f"{'='*60}")
    print(f"Total clients: {num_clients}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total messages exchanged: {total_messages}")

    if total_messages > 0:
        print(f"Avg messages per client: {total_messages/successful:.1f}" if successful > 0 else "N/A")

    print(f"\nLoad distribution:")
    for instance_id, count in instance_counts.items():
        percentage = (count / successful * 100) if successful > 0 else 0
        print(f"  {instance_id}: {count} clients ({percentage:.1f}%)")

    # Calculate throughput
    if duration > 0 and total_messages > 0:
        throughput = total_messages / duration
        print(f"\nThroughput: {throughput:.2f} messages/second")

    if successful == num_clients:
        print(f"\n🎉 All clients connected successfully!")
        return True
    else:
        print(f"\n⚠️  {failed} clients failed")
        return False


async def main():
    """Main test runner."""
    if len(sys.argv) > 1:
        url = sys.argv[1]
    else:
        url = "ws://localhost:8000/ws"

    # Get parameters
    num_clients = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    duration = int(sys.argv[3]) if len(sys.argv) > 3 else 30

    success = await load_test(url, num_clients, duration)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())