Reduce tail-end latencies by spinning up infinite generators!
- Spawns multiple generator threads that compete to give you the first response
- If the main generators are being slow, it switches to fallback generators
- Gives up entirely if everything's taking too long (timeout)
- Makes sure to clean up after itself (no zombie threads!)