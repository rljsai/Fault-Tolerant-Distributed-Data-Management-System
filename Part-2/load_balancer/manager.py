import asyncio
import aiohttp
from aiodocker import Docker
from hash_ring import HashRing
from colorama import Fore, Style


class Manager:
    def __init__(self, heartbeat_interval=5, max_fails=3, on_server_dead=None):
        self.ring = HashRing()
        self.replicas = set()
        self.heartbeat_fail_count = {}
        self.semaphore = asyncio.Semaphore(5)  # limit docker ops
        self.heartbeat_interval = heartbeat_interval
        self.max_fails = max_fails
        self.counter = 1  # for auto-spawn names
        self._task = None  # heartbeat task will be started later
        self.on_server_dead = on_server_dead  # callback in LB

    async def start(self):
        """Start background heartbeat checker (call inside Quart before_serving)."""
        if not self._task:
            self._task = asyncio.create_task(self._heartbeat_checker())

    async def stop(self):
        """Stop background task and cleanup servers."""
        if self._task:
            self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

        # cleanup all replicas
        for h in list(self.replicas):
           await self.remove_server(h)

    async def spawn_server(self, hostname: str):
        async with self.semaphore:
            async with Docker() as docker:
                container = await docker.containers.create_or_replace(
                    name=hostname,
                    config={
                        "Image": "myserver",  # image built from server Dockerfile
                        "Env": [f"SERVER_ID={hostname}", "POSTGRES_USER=postgres", "POSTGRES_PASSWORD=postgres", "POSTGRES_DB=studdb"],
                        "Hostname": hostname,
                        "Tty": True,
                    },
                )
                # connect network net1 if exists
                try:
                    net = await docker.networks.get("net1")
                    await net.connect({
                        "Container": container.id,
                        "EndpointConfig": {"Aliases": [hostname]}
                    })
                except Exception:
                    pass
                await container.start()
                print(f"{Fore.GREEN}[Spawned]{Style.RESET_ALL} {hostname}")

        self.replicas.add(hostname)
        self.ring.add_server(hostname)
        self.heartbeat_fail_count[hostname] = 0


    async def remove_server(self, hostname: str):
        async with self.semaphore:
            async with Docker() as docker:
                try:
                    container = await docker.containers.get(hostname)
                    await container.stop(timeout=3)
                    await container.delete(force=True)
                    print(f"{Fore.YELLOW}[Removed]{Style.RESET_ALL} {hostname}")
                except Exception:
                    pass

        if hostname in self.replicas:
            self.replicas.remove(hostname)
            self.ring.remove_server(hostname)
            self.heartbeat_fail_count.pop(hostname, None)

    def list_servers(self):
        return {
            "N": len(self.replicas),
            "replicas": list(self.replicas),
        }

    def get_server_for_request(self, rid: int):
        return self.ring.get_server(rid)

    # ---------- heartbeat ----------
    async def _heartbeat_checker(self):
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            dead = []

            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=2)
            ) as session:
                for server in list(self.replicas):
                    try:
                        async with session.get(f"http://{server}:5000/heartbeat") as resp:
                            if resp.status != 200:
                                raise Exception("bad heartbeat")
                            self.heartbeat_fail_count[server] = 0
                    except Exception:
                        self.heartbeat_fail_count[server] = (
                            self.heartbeat_fail_count.get(server, 0) + 1
                        )
                        if self.heartbeat_fail_count[server] >= self.max_fails:
                            dead.append(server)

            for d in dead:
                print(f"{Fore.RED}[Heartbeat] {d} failed! Respawning...{Style.RESET_ALL}")
                # mark dead and attempt to restore by removing and spawning a new server
                if self.on_server_dead:
                    await self.on_server_dead(d)   # delegate to LB

    async def _send_config(server_name, shard_list, retries=5, delay=2):
        async with aiohttp.ClientSession() as session:
            for attempt in range(retries):
               try:
                  async with session.post(f"http://{server_name}:5000/config",
                                        json={"shard_ids": shard_list}) as resp:
                     if resp.status == 200:
                        print(f"[Config] {server_name} configured OK")
                        return
               except Exception as e:
                   print(f"Warning: /config call to {server_name} failed (attempt {attempt+1}): {e}")
               await asyncio.sleep(delay)
        print(f"[Config] Failed to configure {server_name} after {retries} retries")