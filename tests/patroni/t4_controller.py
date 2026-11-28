

# ── T4 ───────────────────────────────────────────────────────────────────────
# Appended to Patroni's own features/environment.py by patroni/Dockerfile, so
# that `DCS=t4 behave` runs Patroni's acceptance suite against T4.
#
# The registry Patroni builds in PatroniPoolController.__init__ is
# {subclass.name(): subclass for subclass in AbstractDcsController
# .get_subclasses()}, and name() is the class name without its "Controller"
# suffix, lowercased. Appending the class is therefore enough to register it;
# nothing in Patroni needs patching.
#
# T4 serves the etcd v3 API, so everything the etcd3 controller does — build
# the client, read keys, wipe the service tree, set PATRONI_ETCD3_HOST — is
# inherited unchanged. Only starting the process differs.
class T4Controller(Etcd3Controller):

    def _start(self):
        # --advertise-client-urls is what makes MemberList answer, and
        # Etcd3Client's constructor calls it (via _load_machines_cache) before
        # the suite runs its first scenario. Without the flag MemberList
        # returns Unimplemented and _is_running never reports ready.
        #
        # No object store: a single node runs with local-only durability,
        # which is the same scope as the `etcd --data-dir` this replaces.
        return psutil.Popen(["t4", "run",
                             "--data-dir", self._work_directory,
                             "--listen", "0.0.0.0:2379",
                             "--advertise-client-urls", "http://localhost:2379"],
                            stdout=self._log, stderr=subprocess.STDOUT)
