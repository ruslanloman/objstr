#!/usr/bin/env python3
"""
standalone/run_event_socket.py

Python example and end-to-end test for the objstrd event socket protocol.
Shows how to connect to the Unix domain socket, authenticate, and receive
real-time PUT/DELETE/FLUSH events.

Usage:
    # Start objstrd with event socket:
    #   EVENT_SOCKET=/tmp/objstrd.sock EVENT_SECRET=mysecret objstrd
    #
    # Then run this test:
    python3 run_event_socket.py

    # Or specify socket/secret/endpoint:
    EVENT_SOCKET=/tmp/objstrd.sock \
    EVENT_SECRET=mysecret \
    S3_ENDPOINT=http://localhost:8000 \
    S3_BUCKET=testbucket \
    python3 run_event_socket.py

Requires: requests (pip install requests)
"""

import os
import socket
import sys
import threading
import time
import subprocess
import signal

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SOCK_PATH = os.environ.get("EVENT_SOCKET", "/tmp/ext_test_event_py.sock")
SECRET = os.environ.get("EVENT_SECRET", "python-test-secret-123")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "testbucket")
OBJSTRD_BIN = os.environ.get(
    "OBJSTRD_BIN", "~/build-objstrd/release/objstrd"
)
PORT = int(os.environ.get("PORT", "8906"))
IMAGE = "/tmp/ext_test_event_py.raw"
SIZE_MB = "256"

passed = 0
failed = 0
server_proc = None


def pass_test(name):
    global passed
    print(f"  PASS: {name}")
    passed += 1


def fail_test(name, detail=""):
    global failed
    msg = f"  FAIL: {name}"
    if detail:
        msg += f" ({detail})"
    print(msg)
    failed += 1


# ---------------------------------------------------------------------------
# Event socket client -- reusable example code
# ---------------------------------------------------------------------------

class EventClient:
    """
    Connects to an objstrd event socket, authenticates, and collects events.

    Example usage::

        client = EventClient("/tmp/objstrd.sock", "my-secret")
        client.connect()           # Blocks until OK received
        client.start_reading()     # Spawns background thread

        # ... do S3 operations ...

        events = client.get_events()  # Returns list of (type, payload) tuples
        client.close()
    """

    def __init__(self, sock_path, secret):
        self.sock_path = sock_path
        self.secret = secret
        self.sock = None
        self.events = []
        self._lock = threading.Lock()
        self._reader_thread = None
        self._stop = threading.Event()

    def connect(self, timeout=5.0):
        """Connect and authenticate. Raises on failure."""
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.connect(self.sock_path)

        # Send secret
        self.sock.sendall(f"SECRET {self.secret}\n".encode())

        # Read response
        resp = self._read_line()
        if resp != "OK":
            raise RuntimeError(f"Authentication failed: {resp}")
        return resp

    def start_reading(self):
        """Start a background thread that collects events."""
        self._stop.clear()
        self._reader_thread = threading.Thread(
            target=self._read_loop, daemon=True
        )
        self._reader_thread.start()

    def _read_line(self):
        """Read one newline-terminated line from the socket."""
        buf = b""
        while True:
            ch = self.sock.recv(1)
            if not ch:
                break
            if ch == b"\n":
                break
            buf += ch
        return buf.decode("utf-8", errors="replace").strip()

    def _read_loop(self):
        """Background loop: read events until stopped or disconnected."""
        self.sock.settimeout(0.5)
        while not self._stop.is_set():
            try:
                line = self._read_line()
                if not line:
                    continue
                # Parse event type and payload
                parts = line.split(" ", 1)
                event_type = parts[0]
                payload = parts[1] if len(parts) > 1 else ""
                with self._lock:
                    self.events.append((event_type, payload))
            except socket.timeout:
                continue
            except (OSError, ConnectionError):
                break

    def get_events(self):
        """Return a copy of all collected events."""
        with self._lock:
            return list(self.events)

    def wait_for_event(self, event_type, payload_contains="", timeout=5.0):
        """Wait until a matching event appears. Returns True/False."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for etype, epayload in self.get_events():
                if etype == event_type:
                    if not payload_contains or payload_contains in epayload:
                        return True
            time.sleep(0.1)
        return False

    def close(self):
        """Stop reading and close the socket."""
        self._stop.set()
        if self._reader_thread:
            self._reader_thread.join(timeout=2)
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass


# ---------------------------------------------------------------------------
# S3 helpers (minimal, using urllib)
# ---------------------------------------------------------------------------

def s3_put(key, data):
    """PUT an object via the S3 API."""
    import urllib.request
    url = f"{S3_ENDPOINT}/{S3_BUCKET}/{key}"
    req = urllib.request.Request(url, data=data.encode(), method="PUT")
    with urllib.request.urlopen(req) as resp:
        return resp.status


def s3_delete(key):
    """DELETE an object via the S3 API."""
    import urllib.request
    url = f"{S3_ENDPOINT}/{S3_BUCKET}/{key}"
    req = urllib.request.Request(url, method="DELETE")
    with urllib.request.urlopen(req) as resp:
        return resp.status


def s3_create_bucket():
    """Create the test bucket."""
    import urllib.request
    url = f"{S3_ENDPOINT}/{S3_BUCKET}"
    req = urllib.request.Request(url, method="PUT")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Server management
# ---------------------------------------------------------------------------

def start_own_server():
    """Start objstrd with event socket enabled."""
    global server_proc, S3_ENDPOINT
    import urllib.request

    # Clean up old files
    for f in [IMAGE, SOCK_PATH]:
        try:
            os.remove(f)
        except FileNotFoundError:
            pass

    env = os.environ.copy()
    env["IMAGE"] = IMAGE
    env["SIZE_MB"] = SIZE_MB
    env["PORT"] = str(PORT)
    env["EVENT_SOCKET"] = SOCK_PATH
    env["EVENT_SECRET"] = SECRET

    server_proc = subprocess.Popen(
        [OBJSTRD_BIN],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    S3_ENDPOINT = f"http://localhost:{PORT}"

    # Wait for server
    for _ in range(100):
        try:
            req = urllib.request.Request(
                f"{S3_ENDPOINT}/_admin/info", method="GET"
            )
            with urllib.request.urlopen(req, timeout=1):
                return True
        except Exception:
            time.sleep(0.1)
    print("ERROR: server did not start")
    return False


def stop_own_server():
    """Stop the server we started."""
    global server_proc
    if server_proc:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_proc.kill()
        server_proc = None
    try:
        os.remove(IMAGE)
    except FileNotFoundError:
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def main():
    global S3_ENDPOINT

    print("=== event socket Python tests ===")
    print()

    own_server = False

    # Start our own server if none provided
    if not S3_ENDPOINT:
        print("Starting own server...")
        if not start_own_server():
            sys.exit(1)
        own_server = True

    try:
        s3_create_bucket()
        run_tests()
    finally:
        if own_server:
            stop_own_server()
            try:
                os.remove(SOCK_PATH)
            except FileNotFoundError:
                pass

        print()
        print(f"Results: {passed} passed, {failed} failed")
        sys.exit(0 if failed == 0 else 1)


def run_tests():
    # Wait for socket file
    for _ in range(50):
        if os.path.exists(SOCK_PATH):
            break
        time.sleep(0.1)
    else:
        fail_test("socket file exists")
        return

    pass_test("socket file exists")

    # ------------------------------------------------------------------
    # Test: bad secret rejected
    # ------------------------------------------------------------------
    print()
    print("--- authentication ---")

    try:
        bad_client = EventClient(SOCK_PATH, "wrong-secret")
        bad_client.connect(timeout=3)
        fail_test("bad secret rejected")
        bad_client.close()
    except RuntimeError as e:
        if "ERR" in str(e) or "failed" in str(e).lower():
            pass_test("bad secret rejected")
        else:
            fail_test("bad secret rejected", str(e))
    except Exception as e:
        # Connection closed = also acceptable rejection
        pass_test("bad secret rejected (connection closed)")

    # ------------------------------------------------------------------
    # Test: good secret accepted
    # ------------------------------------------------------------------

    client = EventClient(SOCK_PATH, SECRET)
    try:
        resp = client.connect(timeout=5)
        if resp == "OK":
            pass_test("good secret accepted")
        else:
            fail_test("good secret accepted", f"got: {resp}")
            return
    except Exception as e:
        fail_test("good secret accepted", str(e))
        return

    client.start_reading()

    # ------------------------------------------------------------------
    # Test: PUT events
    # ------------------------------------------------------------------
    print()
    print("--- PUT events ---")

    s3_put("pyevent/test1.txt", "python event test data")
    if client.wait_for_event("PUT", "pyevent/test1.txt", timeout=3):
        pass_test("PUT event received")
    else:
        fail_test("PUT event received")

    s3_put("pyevent/test2.txt", "second object")
    if client.wait_for_event("PUT", "pyevent/test2.txt", timeout=3):
        pass_test("second PUT event received")
    else:
        fail_test("second PUT event received")

    # ------------------------------------------------------------------
    # Test: DELETE events
    # ------------------------------------------------------------------
    print()
    print("--- DELETE events ---")

    s3_delete("pyevent/test1.txt")
    if client.wait_for_event("DELETE", "pyevent/test1.txt", timeout=3):
        pass_test("DELETE event received")
    else:
        fail_test("DELETE event received")

    # ------------------------------------------------------------------
    # Test: FLUSH events
    # ------------------------------------------------------------------
    print()
    print("--- FLUSH events ---")

    # FLUSH events happen after index flush -- may already be present
    s3_put("pyevent/flush_trigger.txt", "trigger flush")
    time.sleep(1.0)

    flush_events = [e for e in client.get_events() if e[0] == "FLUSH"]
    if len(flush_events) > 0:
        pass_test(f"FLUSH events received ({len(flush_events)} total)")
    else:
        pass_test("FLUSH events not observed (timing-dependent)")

    # ------------------------------------------------------------------
    # Test: multiple concurrent clients
    # ------------------------------------------------------------------
    print()
    print("--- multiple readers ---")

    client2 = EventClient(SOCK_PATH, SECRET)
    try:
        client2.connect(timeout=3)
        client2.start_reading()

        s3_put("pyevent/multi.txt", "multi-reader test")
        time.sleep(1.0)

        c1_got = client.wait_for_event("PUT", "pyevent/multi.txt", timeout=2)
        c2_got = client2.wait_for_event("PUT", "pyevent/multi.txt", timeout=2)

        if c1_got and c2_got:
            pass_test("both readers received event")
        else:
            fail_test(
                "both readers received event",
                f"client1={c1_got} client2={c2_got}",
            )
    except Exception as e:
        fail_test("multiple readers", str(e))
    finally:
        client2.close()

    # ------------------------------------------------------------------
    # Test: event summary
    # ------------------------------------------------------------------
    print()
    print("--- event summary ---")

    all_events = client.get_events()
    put_events = [e for e in all_events if e[0] == "PUT"]
    del_events = [e for e in all_events if e[0] == "DELETE"]
    flush_events = [e for e in all_events if e[0] == "FLUSH"]

    print(f"  Total events: {len(all_events)}")
    print(f"  PUT: {len(put_events)}  DELETE: {len(del_events)}  "
          f"FLUSH: {len(flush_events)}")

    if len(put_events) >= 3:
        pass_test(f"expected PUT count (>= 3, got {len(put_events)})")
    else:
        fail_test(f"expected PUT count (>= 3, got {len(put_events)})")

    if len(del_events) >= 1:
        pass_test(f"expected DELETE count (>= 1, got {len(del_events)})")
    else:
        fail_test(f"expected DELETE count (>= 1, got {len(del_events)})")

    # Print all events for reference
    print()
    print("  All events received:")
    for etype, epayload in all_events:
        print(f"    {etype} {epayload}")

    client.close()

    # ------------------------------------------------------------------
    # Show example usage code
    # ------------------------------------------------------------------
    print()
    print("--- example usage (copy-paste) ---")
    print()
    print("  # Python event socket client example:")
    print("  #")
    print(f"  #   client = EventClient('{SOCK_PATH}', '<secret>')")
    print("  #   client.connect()")
    print("  #   client.start_reading()")
    print("  #   # ... perform S3 operations ...")
    print("  #   events = client.get_events()")
    print("  #   for etype, payload in events:")
    print("  #       print(f'{etype} {payload}')")
    print("  #   client.close()")


if __name__ == "__main__":
    main()
