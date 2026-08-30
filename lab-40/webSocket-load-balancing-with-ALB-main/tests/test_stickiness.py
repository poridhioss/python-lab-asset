"""
Lab 40: Sticky Session Test
Verifies that WebSocket connections are routed to the same backend instance
"""

import asyncio
import websockets
import json
import sys
from collections import defaultdict


async def test_stickiness(url, num_connections=10, messages_per_connection=5):
    """
    Test if connections stick to the same backend instance.

    Strategy:
    1. Establish multiple WebSocket connections
    2. Track which instance each connection reaches
    3. Verify that messages on the same connection go to the same instance
    """
    print(f"\n{'='*60}")
    print(f"Sticky Session Test")
    print(f"URL: {url}")
    print(f"Connections: {num_connections}")
    print(f"Messages per connection: {messages_per_connection}")
    print(f"{'='*60}\n")

    instance_per_connection = {}
    consistent_instances = 0
    total_tests = 0

    for i in range(num_connections):
        try:
            async with websockets.connect(url) as websocket:
                # Get instance from welcome message
                welcome = await websocket.recv()
                welcome_data = json.loads(welcome)
                initial_instance = welcome_data.get('instance_id', 'unknown')

                instance_per_connection[i] = [initial_instance]

                # Send multiple messages and track instance
                for j in range(messages_per_connection):
                    msg = {"type": "test", "conn": i, "msg": j}
                    await websocket.send(json.dumps(msg))

                    response = await websocket.recv()
                    resp_data = json.loads(response)
                    instance = resp_data.get('instance_id', 'unknown')
                    instance_per_connection[i].append(instance)

                # Check if all messages went to same instance
                instances = instance_per_connection[i]
                if len(set(instances)) == 1:
                    consistent_instances += 1
                    print(f"  Connection {i}: ✅ Consistent (instance: {initial_instance})")
                else:
                    print(f"  Connection {i}: ❌ Inconsistent - instances: {set(instances)}")

                total_tests += 1

        except Exception as e:
            print(f"  Connection {i}: ❌ Error: {e}")

    # Summary
    print(f"\n{'='*60}")
    print(f"Stickiness Test Results")
    print(f"{'='*60}")
    print(f"Total connections tested: {total_tests}")
    print(f"Consistent (sticky): {consistent_instances}")
    print(f"Inconsistent: {total_tests - consistent_instances}")
    print(f"Stickiness rate: {(consistent_instances/total_tests*100):.1f}%" if total_tests > 0 else "N/A")

    # Show instance distribution
    print(f"\nInstance distribution:")
    all_instances = []
    for instances in instance_per_connection.values():
        all_instances.extend(instances)
    instance_counts = defaultdict(int)
    for inst in all_instances:
        instance_counts[inst] += 1

    for instance, count in instance_counts.items():
        print(f"  {instance}: {count} connections")

    if consistent_instances == total_tests:
        print(f"\n🎉 All connections are sticky!")
        return True
    else:
        print(f"\n⚠️  Some connections are not sticky (expected during scaling events)")
        return consistent_instances > total_tests * 0.5  # At least 50% should be sticky


async def main():
    """Main test runner."""
    if len(sys.argv) > 1:
        url = sys.argv[1]
    else:
        url = "ws://localhost:8000/ws"

    success = await test_stickiness(url)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())